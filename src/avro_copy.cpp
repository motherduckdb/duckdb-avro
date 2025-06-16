#include "avro_copy.hpp"

#include "duckdb/common/enums/file_compression_type.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/function/function.hpp"
#include "yyjson.hpp"
#include "duckdb/common/printer.hpp"
#include "field_ids.hpp"
#include "errno.h"

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {

static string ConvertTypeToAvro(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::VARCHAR:
		return "string";
	case LogicalTypeId::BLOB:
		return "bytes";
	case LogicalTypeId::INTEGER:
		return "int";
	case LogicalTypeId::BIGINT:
		return "long";
	case LogicalTypeId::FLOAT:
		return "float";
	case LogicalTypeId::DOUBLE:
		return "double";
	case LogicalTypeId::BOOLEAN:
		return "boolean";
	case LogicalTypeId::SQLNULL:
		return "null";
	case LogicalTypeId::STRUCT:
		return "record";
	case LogicalTypeId::ENUM:
		//! FIXME: this should be implemented at some point
		throw NotImplementedException("Can't convert logical type '%s' to Avro type", type.ToString());
	case LogicalTypeId::LIST:
		return "array";
	case LogicalTypeId::MAP:
		//! This uses a 'logicalType': map, and a struct as 'items'
		return "array";
	default:
		throw NotImplementedException("Can't convert logical type '%s' to Avro type", type.ToString());
	};

	//! FIXME: we don't have support for 'FIXED' currently (a fixed size blob)
}

static bool IsNamedSchema(const LogicalType &type) {
	switch (type.id()) {
	//! NOTE: 'fixed' is also part of this, but we don't have that type in DuckDB
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::ENUM:
		return true;
	default:
		return false;
	}
}

struct JSONSchemaGenerator {
public:
	JSONSchemaGenerator(const vector<string> &names, const vector<LogicalType> &types) : names(names), types(types) {
		doc = yyjson_mut_doc_new(nullptr);
		root_object = yyjson_mut_obj(doc);
		yyjson_mut_doc_set_root(doc, root_object);
		yyjson_mut_obj_add_str(doc, root_object, "type", "record");
		yyjson_mut_obj_add_strcpy(doc, root_object, "name", root_name.c_str());
	}
	~JSONSchemaGenerator() {
		if (doc) {
			yyjson_mut_doc_free(doc);
		}
	}

public:
	void ParseFieldIds(const case_insensitive_map_t<vector<Value>> &options, case_insensitive_set_t &recognized) {
		auto it = options.find("FIELD_IDS");
		if (it == options.end()) {
			return;
		}
		if (it->second.empty()) {
			throw InvalidInputException("FIELD_IDS can not be provided without a value");
		}
		field_ids = avro::FieldIDUtils::ParseFieldIds(it->second[0], names, types);
		recognized.insert(it->first);
	}
	void ParseRootName(const case_insensitive_map_t<vector<Value>> &options, case_insensitive_set_t &recognized) {
		auto it = options.find("ROOT_NAME");
		if (it == options.end()) {
			return;
		}
		if (it->second.empty()) {
			throw InvalidInputException("ROOT_NAME can not be provided without a value");
		}
		auto &value = it->second[0];
		if (value.type().id() != LogicalTypeId::VARCHAR) {
			throw InvalidInputException("'ROOT_NAME' is expected to be provided as VARCHAR, this is used for the name "
			                            "of the top level 'record'");
		}
		root_name = value.GetValue<string>();
		recognized.insert(it->first);
	}
	void ParseMetadata(const case_insensitive_map_t<vector<Value>> &options, case_insensitive_set_t &recognized) {
		auto it = options.find("METADATA");
		if (it == options.end()) {
			return;
		}

		throw NotImplementedException("The 'METADATA' option is not supported in this release of Avro, please try upgrading your version ('FORCE INSTALL avro;')");

		if (it->second.empty()) {
			throw InvalidInputException("METADATA can not be provided without a value");
		}
		auto &value = it->second[0];
		if (value.type().id() != LogicalTypeId::STRUCT) {
			throw InvalidInputException("'METADATA' is expected to be provided as a STRUCT of key-value metadata");
		}
		recognized.insert(it->first);
	}

public:
	void VerifyAvroName(const string &name, const LogicalType &type) {
		D_ASSERT(!name.empty());
		for (idx_t i = 0; i < name.size(); i++) {
			if (!(isalpha(name[i]) || name[i] == '_' || (i && isdigit(name[i])))) {
				throw InvalidInputException("'%s' is not a valid Avro identifier\nThe identifier has to match the "
				                            "regex: [A-Za-z_][A-Za-z0-9_]*",
				                            name);
			}
		}
		if (!IsNamedSchema(type)) {
			return;
		}
		auto res = named_schemas.insert(name);
		if (!res.second) {
			throw BinderException("Avro schema by the name of '%s' already exists, names of 'record', 'enum' and "
			                      "'fixed' types have to be distinct",
			                      name);
		}
	}

	string GenerateSchemaName(const string &base) {
		return StringUtil::Format("%s%d", base, generated_name_id++);
	}

	yyjson_mut_val *CreateJSONType(const string &name, const LogicalType &type, optional_ptr<avro::FieldID> field_id, bool struct_field = false) {
		yyjson_mut_val *object;
		if (!type.IsNested()) {
			object = yyjson_mut_obj(doc);
			yyjson_mut_obj_add_strcpy(doc, object, "type", ConvertTypeToAvro(type).c_str());
			// {
			//    "type": "bool"
			//    < additional fields >
			// }
			if (field_id) {
				yyjson_mut_obj_add_int(doc, object, "field-id", field_id->GetFieldId());
			}
			if (struct_field) {
				VerifyAvroName(name, type);
				yyjson_mut_obj_add_strcpy(doc, object, "name", name.c_str());
			}
		} else {
			object = CreateNestedType(name, type, field_id);
		}

		auto wrapper = yyjson_mut_obj(doc);
		auto union_type = yyjson_mut_obj_add_arr(doc, wrapper, "type");
		if (struct_field) {
			yyjson_mut_obj_add_strcpy(doc, wrapper, "name", name.c_str());
		}
		yyjson_mut_arr_add_strcpy(doc, union_type, "null");
		yyjson_mut_arr_add_val(union_type, object);
		if (field_id) {
			yyjson_mut_obj_add_int(doc, wrapper, "field-id", field_id->GetFieldId());
		}
		return wrapper;
	}

	yyjson_mut_val *CreateNestedType(const string &name, const LogicalType &type, optional_ptr<avro::FieldID> field_id) {
		D_ASSERT(type.IsNested());
		auto object = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_strcpy(doc, object, "type", ConvertTypeToAvro(type).c_str());
		VerifyAvroName(name, type);
		yyjson_mut_obj_add_strcpy(doc, object, "name", name.c_str());
		switch (type.id()) {
		case LogicalTypeId::STRUCT: {
			auto &struct_children = StructType::GetChildTypes(type);
			auto fields = yyjson_mut_obj_add_arr(doc, object, "fields");
			for (auto &it : struct_children) {
				auto &child_name = it.first;
				auto &child_type = it.second;
				optional_ptr<avro::FieldID> child_field_id;
				if (field_id) {
					auto it = field_id->children->find(child_name);
					if (it != field_id->children->end()) {
						child_field_id = it->second;
					}
				}
				yyjson_mut_arr_add_val(fields, CreateJSONType(child_name, child_type, child_field_id, true));
			}
			break;
		}
		case LogicalTypeId::MAP:
		case LogicalTypeId::LIST: {
			optional_ptr<avro::FieldID> element_field_id;
			if (field_id) {
				auto it = field_id->children->find("element");
				if (it != field_id->children->end()) {
					element_field_id = it->second;
				}
			}
			if (type.id() == LogicalTypeId::LIST) {
				if (element_field_id) {
					yyjson_mut_obj_add_int(doc, object, "element-id", element_field_id->GetFieldId());
				}
			} else {
				D_ASSERT(type.id() == LogicalTypeId::MAP);
				yyjson_mut_obj_add_strcpy(doc, object, "logicalType", "map");
			}

			auto &list_child = ListType::GetChildType(type);
			auto union_type = yyjson_mut_obj_add_arr(doc, object, "items");
			yyjson_mut_arr_add_strcpy(doc, union_type, "null");
			if (list_child.IsNested()) {
				yyjson_mut_arr_add_val(union_type, CreateNestedType(GenerateSchemaName("element"), list_child, element_field_id));
			} else {
				yyjson_mut_arr_add_strcpy(doc, union_type, ConvertTypeToAvro(list_child).c_str());
			}
			break;
		}
		default:
			throw NotImplementedException("Can't convert nested type '%s' to Avro", type.ToString());
		}
		return object;
	}

	string GenerateJSON() {
		auto array = yyjson_mut_obj_add_arr(doc, root_object, "fields");

		//! Add all the fields
		D_ASSERT(names.size() == types.size());
		for (idx_t i = 0; i < names.size(); i++) {
			auto &name = names[i];
			auto &type = types[i];
			optional_ptr<avro::FieldID> field_id;
			auto it = field_ids.find(name);
			if (it != field_ids.end()) {
				field_id = it->second;
			}
			yyjson_mut_arr_add_val(array, CreateJSONType(name, type, field_id, true));
		}

		//! Write the result to a string
		auto data = yyjson_mut_val_write_opts(root_object, YYJSON_WRITE_ALLOW_INF_AND_NAN, nullptr, nullptr, nullptr);
		if (!data) {
			yyjson_mut_doc_free(doc);
			throw InvalidInputException("Could not create a JSON representation of the table schema, yyjson failed");
		}
		auto res = string(data);
		free(data);
		return res;
	}

public:
	const vector<string> &names;
	const vector<LogicalType> &types;

	string root_name = "root";
	case_insensitive_map_t<avro::FieldID> field_ids;
	idx_t generated_name_id = 0;
	yyjson_mut_doc *doc = nullptr;
	yyjson_mut_val *root_object;
	unordered_set<string> named_schemas;
};

static string CreateJSONSchema(const case_insensitive_map_t<vector<Value>> &options, const vector<string> &names,
                               const vector<LogicalType> &types) {
	JSONSchemaGenerator state(names, types);

	case_insensitive_set_t recognized;
	state.ParseFieldIds(options, recognized);
	state.ParseRootName(options, recognized);
	state.ParseMetadata(options, recognized);
	vector<string> unrecognized_options;
	for (auto &option : options) {
		if (recognized.count(option.first)) {
			continue;
		}
		auto key = option.first;
		if (option.second.empty()) {
			unrecognized_options.push_back(StringUtil::Format("key: '%s'", key));
		} else {
			unrecognized_options.push_back(StringUtil::Format("key: '%s' with value: '%s'", key, option.second[0].ToString()));
		}
	}
	if (!unrecognized_options.empty()) {
		throw InvalidConfigurationException("The following option(s) are not recognized: %s", StringUtil::Join(unrecognized_options, ", "));
	}

	return state.GenerateJSON();
}

WriteAvroBindData::WriteAvroBindData(CopyFunctionBindInput &input, const vector<string> &names,
                                     const vector<LogicalType> &types)
    : names(names), types(types) {

	json_schema = CreateJSONSchema(input.info.options, names, types);
	if (avro_schema_from_json_length(json_schema.c_str(), json_schema.size(), &schema)) {
		throw InvalidInputException(avro_strerror());
	}
	interface = avro_generic_class_from_schema(schema);
}

WriteAvroBindData::~WriteAvroBindData() {
	avro_schema_decref(schema);
	avro_value_iface_decref(interface);
}

WriteAvroLocalState::WriteAvroLocalState(FunctionData &bind_data_p) {
	auto &bind_data = bind_data_p.Cast<WriteAvroBindData>();
	avro_generic_value_new(bind_data.interface, &value);
}

WriteAvroLocalState::~WriteAvroLocalState() {
	avro_value_decref(&value);
}

WriteAvroGlobalState::~WriteAvroGlobalState() {
	//! NOTE: the 'writer' and 'datum_writer' do not need to be closed, they are owned by the file_writer
	avro_file_writer_close(file_writer);
}

WriteAvroGlobalState::WriteAvroGlobalState(ClientContext &context, FunctionData &bind_data_p, FileSystem &fs,
                                           const string &file_path)
    : allocator(Allocator::Get(context)), memory_buffer(allocator), datum_buffer(allocator), fs(fs) {
	handle = fs.OpenFile(file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW |
	                                    FileLockType::WRITE_LOCK | FileCompressionType::AUTO_DETECT);
	auto &bind_data = bind_data_p.Cast<WriteAvroBindData>();

	//! Guess how big the "header" of the Avro file needs to be
	idx_t capacity = MaxValue<idx_t>(BUFFER_SIZE, NextPowerOfTwo(bind_data.json_schema.size() + SYNC_SIZE + MAX_ROW_COUNT_BYTES));
	memory_buffer.Resize(capacity);

	int ret;
	writer = avro_writer_memory(const_char_ptr_cast(memory_buffer.GetData()), memory_buffer.GetCapacity());
	datum_writer = avro_writer_memory(const_char_ptr_cast(datum_buffer.GetData()), datum_buffer.GetCapacity());
	while ((ret = avro_file_writer_create_from_writers(writer, datum_writer, bind_data.schema, &file_writer)) == ENOSPC) {
		auto current_capacity = memory_buffer.GetCapacity();
		memory_buffer.Resize(NextPowerOfTwo(current_capacity * 2));
	}
	if (ret) {
		throw InvalidInputException(avro_strerror());
	}

	auto written_bytes = avro_writer_tell(writer);
	WriteData(memory_buffer.GetData(), written_bytes);
	avro_writer_memory_set_dest(writer, (const char *)memory_buffer.GetData(), memory_buffer.GetCapacity());
}

static unique_ptr<FunctionData> WriteAvroBind(ClientContext &context, CopyFunctionBindInput &input,
                                              const vector<string> &names, const vector<LogicalType> &sql_types) {
	auto res = make_uniq<WriteAvroBindData>(input, names, sql_types);
	return res;
}

static unique_ptr<LocalFunctionData> WriteAvroInitializeLocal(ExecutionContext &context, FunctionData &bind_data_p) {
	auto res = make_uniq<WriteAvroLocalState>(bind_data_p);
	return res;
}

static unique_ptr<GlobalFunctionData> WriteAvroInitializeGlobal(ClientContext &context, FunctionData &bind_data_p,
                                                                const string &file_path) {
	auto res = make_uniq<WriteAvroGlobalState>(context, bind_data_p, FileSystem::GetFileSystem(context), file_path);
	return res;
}

static idx_t PopulateValue(avro_value_t *target, const Value &val);

static idx_t PopulateValue(avro_value_t *target, const Value &val) {
	auto &type = val.type();

	auto union_value = *target;
	if (val.IsNull()) {
		avro_value_set_branch(&union_value, 0, target);
		avro_value_set_null(target);
		return 1;
	}
	avro_value_set_branch(&union_value, 1, target);

	switch (type.id()) {
	case LogicalTypeId::BOOLEAN: {
		auto boolean = val.GetValueUnsafe<bool>();
		avro_value_set_boolean(target, boolean);
		return sizeof(bool);
	}
	case LogicalTypeId::BLOB: {
		auto str = val.GetValueUnsafe<string_t>();
		avro_value_set_bytes(target, (void *)str.GetData(), str.GetSize());
		return str.GetSize();
	}
	case LogicalTypeId::DOUBLE: {
		auto value = val.GetValueUnsafe<double>();
		avro_value_set_double(target, value);
		return sizeof(double);
	}
	case LogicalTypeId::FLOAT: {
		auto value = val.GetValueUnsafe<float>();
		avro_value_set_float(target, value);
		return sizeof(float);
	}
	case LogicalTypeId::INTEGER: {
		auto integer = val.GetValueUnsafe<int32_t>();
		avro_value_set_int(target, integer);
		return sizeof(int32_t);
	}
	case LogicalTypeId::BIGINT: {
		auto bigint = val.GetValueUnsafe<int64_t>();
		avro_value_set_long(target, bigint);
		return sizeof(int64_t);
	}
	case LogicalTypeId::VARCHAR: {
		auto str = val.GetValueUnsafe<string_t>();
		avro_value_set_string_len(target, str.GetData(), str.GetSize() + 1);
		return str.GetSize();
	}
	case LogicalTypeId::ENUM: {
		//! TODO: add support for ENUM
		throw NotImplementedException("Can't convert ENUM Value (%s) to Avro yet", val.ToString());
	}
	case LogicalTypeId::MAP:
	case LogicalTypeId::LIST: {
		auto &list_values = ListValue::GetChildren(val);
		idx_t list_value_size = 0;
		for (idx_t i = 0; i < list_values.size(); i++) {
			auto &list_value = list_values[i];

			avro_value_t item;
			size_t unused_new_index;
			if (avro_value_append(target, &item, &unused_new_index)) {
				throw InvalidInputException(avro_strerror());
			}

			list_value_size += PopulateValue(&item, list_value);
		}
		return list_value_size + 1;
	}
	case LogicalTypeId::STRUCT: {
		auto &struct_values = StructValue::GetChildren(val);
		idx_t struct_value_size = 0;
		for (idx_t i = 0; i < struct_values.size(); i++) {
			const char *unused_name;
			avro_value_t field;
			if (avro_value_get_by_index(target, i, &field, &unused_name)) {
				throw InvalidInputException(avro_strerror());
			}
			struct_value_size += PopulateValue(&field, struct_values[i]);
		}
		return struct_value_size + 1;
	}
	default:
		throw NotImplementedException("PopulateValue not implemented for type %s", type.ToString());
	}
}

static void WriteAvroSink(ExecutionContext &context, FunctionData &bind_data_p, GlobalFunctionData &gstate_p,
                          LocalFunctionData &lstate_p, DataChunk &input) {
	auto &global_state = gstate_p.Cast<WriteAvroGlobalState>();
	auto &local_state = lstate_p.Cast<WriteAvroLocalState>();

	auto &datum_buffer = global_state.datum_buffer;
	idx_t count = input.size();
	idx_t offset_in_datum_buffer = 0;
	for (idx_t i = 0; i < count; i++) {

		//! Populate our avro value, estimating the size of the value as we go
		idx_t value_size = 0;
		for (idx_t col_idx = 0; col_idx < input.ColumnCount(); col_idx++) {
			auto val = input.GetValue(col_idx, i);

			const char *unused_name;
			avro_value_t column;
			if (avro_value_get_by_index(&local_state.value, col_idx, &column, &unused_name)) {
				throw InvalidInputException(avro_strerror());
			}
			value_size += PopulateValue(&column, val);
		}

		//! Prepare the datum buffer for this row
		idx_t length = datum_buffer.GetCapacity() - offset_in_datum_buffer;
		if (value_size > length) {
			//! This value is too big to fit into the remaining portion of the buffer
			idx_t new_capacity = datum_buffer.GetCapacity();
			new_capacity += value_size;
			datum_buffer.ResizeAndCopy(NextPowerOfTwo(new_capacity));
		}
		avro_writer_memory_set_dest_with_offset(global_state.datum_writer, (const char *)datum_buffer.GetData(),
		                                        datum_buffer.GetCapacity(), offset_in_datum_buffer);

		int ret;
		while ((ret = avro_file_writer_append_value(global_state.file_writer, &local_state.value)) == ENOSPC) {
			auto current_capacity = datum_buffer.GetCapacity();
			datum_buffer.ResizeAndCopy(NextPowerOfTwo(current_capacity * 2));
			avro_writer_memory_set_dest_with_offset(global_state.datum_writer, (const char *)datum_buffer.GetData(),
			                                        datum_buffer.GetCapacity(), offset_in_datum_buffer);
		}
		if (ret) {
			throw InvalidInputException(avro_strerror());
		}

		offset_in_datum_buffer = avro_writer_tell(global_state.datum_writer);
		avro_value_reset(&local_state.value);
	}

	auto &buffer = global_state.memory_buffer;
	auto expected_size = avro_writer_tell(global_state.datum_writer);
	expected_size += WriteAvroGlobalState::SYNC_SIZE + WriteAvroGlobalState::MAX_ROW_COUNT_BYTES;
	if (expected_size > buffer.GetCapacity()) {
		//! Resize the buffer in advance, to prevent any need for resizing below
		buffer.Resize(NextPowerOfTwo(expected_size));
		avro_writer_memory_set_dest(global_state.writer, (const char *)buffer.GetData(), buffer.GetCapacity());
	}

	//! Flush the contents to the buffer, if it fails, resize the buffer and try again
	int ret;
	while ((ret = avro_file_writer_flush(global_state.file_writer)) == ENOSPC) {
		auto current_capacity = buffer.GetCapacity();
		buffer.Resize(NextPowerOfTwo(current_capacity * 2));
		avro_writer_memory_set_dest(global_state.writer, (const char *)buffer.GetData(), buffer.GetCapacity());
	}
	if (ret) {
		throw InvalidInputException(avro_strerror());
	}

	auto written_bytes = avro_writer_tell(global_state.writer);
	global_state.WriteData(buffer.GetData(), written_bytes);
	avro_writer_memory_set_dest(global_state.writer, (const char *)buffer.GetData(), buffer.GetCapacity());
}

static void WriteAvroCombine(ExecutionContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
                             LocalFunctionData &lstate) {
	return;
}

static void WriteAvroFinalize(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate) {
	return;
}

CopyFunctionExecutionMode WriteAvroExecutionMode(bool preserve_insertion_order, bool supports_batch_index) {
	//! For now we only support single-threaded writes to Avro
	return CopyFunctionExecutionMode::REGULAR_COPY_TO_FILE;
}

CopyFunction AvroCopyFunction::Create() {
	CopyFunction function("avro");
	function.extension = "avro";

	function.copy_to_bind = WriteAvroBind;
	function.copy_to_initialize_local = WriteAvroInitializeLocal;
	function.copy_to_initialize_global = WriteAvroInitializeGlobal;
	function.copy_to_sink = WriteAvroSink;
	function.copy_to_combine = WriteAvroCombine;
	function.copy_to_finalize = WriteAvroFinalize;
	function.execution_mode = WriteAvroExecutionMode;
	return function;
}

} // namespace duckdb
