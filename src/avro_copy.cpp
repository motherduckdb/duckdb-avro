#include "avro_copy.hpp"

#include "duckdb/common/enums/file_compression_type.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/function/function.hpp"
#include "yyjson.hpp"
#include "duckdb/common/printer.hpp"

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
		return "enum";
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

static void VerifyAvroName(const string &name) {
	D_ASSERT(!name.empty());
	for (idx_t i = 0; i < name.size(); i++) {
		if (!(isalpha(name[i]) || name[i] == '_' || (i && isdigit(name[i])))) {
			throw InvalidInputException(
			    "'%s' is not a valid Avro identifier\nThe identifier has to match the regex: [A-Za-z_][A-Za-z0-9_]*",
			    name);
		}
	}
}

struct JSONSchemaGenerator {
public:
	JSONSchemaGenerator() {
		doc = yyjson_mut_doc_new(nullptr);
		root_object = yyjson_mut_obj(doc);
		yyjson_mut_doc_set_root(doc, root_object);
		yyjson_mut_obj_add_str(doc, root_object, "type", "record");
		//! TODO: get this name from the bind input
		yyjson_mut_obj_add_str(doc, root_object, "name", "manifest_file");
	}
	~JSONSchemaGenerator() {
		if (doc) {
			yyjson_mut_doc_free(doc);
		}
	}

public:
	string GenerateSchemaName(const string &base) {
		return StringUtil::Format("%s%d", base, generated_name_id++);
	}

	yyjson_mut_val *CreateJSONType(const string &name, const LogicalType &type, bool struct_field = false) {
		//! TODO: actually get the field ids
		int32_t field_id = 42;
		yyjson_mut_val *object;
		if (!type.IsNested()) {
			object = yyjson_mut_obj(doc);
			yyjson_mut_obj_add_strcpy(doc, object, "type", ConvertTypeToAvro(type).c_str());
			// {
			//    "type": "bool"
			//    < additional fields >
			// }
			yyjson_mut_obj_add_int(doc, object, "field-id", field_id);
			if (struct_field) {
				VerifyAvroName(name);
				yyjson_mut_obj_add_strcpy(doc, object, "name", name.c_str());
			}
		} else {
			object = CreateNestedType(name, type);
		}

		auto wrapper = yyjson_mut_obj(doc);
		auto union_type = yyjson_mut_obj_add_arr(doc, wrapper, "type");
		if (struct_field) {
			VerifyAvroName(name);
			yyjson_mut_obj_add_strcpy(doc, wrapper, "name", name.c_str());
		}
		yyjson_mut_arr_add_strcpy(doc, union_type, "null");
		yyjson_mut_arr_add_val(union_type, object);
		return wrapper;
	}

	yyjson_mut_val *CreateNestedType(const string &name, const LogicalType &type) {
		//! TODO: actually get the field ids
		int32_t field_id = 42;

		D_ASSERT(type.IsNested());
		auto object = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_strcpy(doc, object, "type", ConvertTypeToAvro(type).c_str());
		VerifyAvroName(name);
		yyjson_mut_obj_add_strcpy(doc, object, "name", name.c_str());
		switch (type.id()) {
		case LogicalTypeId::STRUCT: {
			auto &struct_children = StructType::GetChildTypes(type);
			auto fields = yyjson_mut_obj_add_arr(doc, object, "fields");
			for (auto &it : struct_children) {
				auto &child_name = it.first;
				auto &child_type = it.second;
				yyjson_mut_arr_add_val(fields, CreateJSONType(child_name, child_type, true));
			}
			break;
		}
		case LogicalTypeId::MAP:
		case LogicalTypeId::LIST: {
			if (type.id() == LogicalTypeId::LIST) {
				yyjson_mut_obj_add_int(doc, object, "element-id", field_id);
			} else {
				D_ASSERT(type.id() == LogicalTypeId::MAP);
				yyjson_mut_obj_add_strcpy(doc, object, "logicalType", "map");
			}

			auto &list_child = ListType::GetChildType(type);
			auto union_type = yyjson_mut_obj_add_arr(doc, object, "items");
			yyjson_mut_arr_add_strcpy(doc, union_type, "null");
			if (list_child.IsNested()) {
				yyjson_mut_arr_add_val(union_type, CreateNestedType(GenerateSchemaName("element"), list_child));
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

	string GenerateJSON(const vector<string> &names, const vector<LogicalType> &types) {
		auto array = yyjson_mut_obj_add_arr(doc, root_object, "fields");

		//! Add all the fields
		D_ASSERT(names.size() == types.size());
		for (idx_t i = 0; i < names.size(); i++) {
			auto &name = names[i];
			auto &type = types[i];
			yyjson_mut_arr_add_val(array, CreateJSONType(name, type, true));
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
	idx_t generated_name_id = 0;
	yyjson_mut_doc *doc = nullptr;
	yyjson_mut_val *root_object;
};

static string CreateJSONSchema(const vector<string> &names, const vector<LogicalType> &types) {
	JSONSchemaGenerator state;

	return state.GenerateJSON(names, types);
}

WriteAvroBindData::WriteAvroBindData(const vector<string> &names, const vector<LogicalType> &types)
    : names(names), types(types) {
	json_schema = CreateJSONSchema(names, types);
	Printer::Print(json_schema);
	if (avro_schema_from_json_length(json_schema.c_str(), json_schema.size(), &schema)) {
		throw InvalidInputException(avro_strerror());
	}
}

WriteAvroBindData::~WriteAvroBindData() {
	avro_schema_decref(schema);
}

WriteAvroLocalState::WriteAvroLocalState(FunctionData &bind_data_p) {
	auto &bind_data = bind_data_p.Cast<WriteAvroBindData>();
	auto &global_state = bind_data.global_state->Cast<WriteAvroGlobalState>();
	avro_generic_value_new(global_state.interface, &value);
}

WriteAvroLocalState::~WriteAvroLocalState() {
	avro_value_decref(&value);
}

WriteAvroGlobalState::~WriteAvroGlobalState() {
	avro_value_iface_decref(interface);
}

WriteAvroGlobalState::WriteAvroGlobalState(ClientContext &context, FunctionData &bind_data_p, FileSystem &fs,
                                           const string &file_path)
    : allocator(Allocator::Get(context)), memory_buffer(allocator), datum_buffer(allocator), fs(fs) {
	handle = fs.OpenFile(file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW |
	                                    FileLockType::WRITE_LOCK | FileCompressionType::AUTO_DETECT);
	auto &bind_data = bind_data_p.Cast<WriteAvroBindData>();

	//! Guess how big the "header" of the Avro file needs to be
	idx_t capacity = MaxValue<idx_t>(BUFFER_SIZE, NextPowerOfTwo(bind_data.json_schema.size() + 16 + 4));
	memory_buffer.Resize(capacity);

	int ret;
	writer = avro_writer_memory(const_char_ptr_cast(memory_buffer.GetData()), memory_buffer.GetCapacity());
	datum_writer = avro_writer_memory(const_char_ptr_cast(datum_buffer.GetData()), datum_buffer.GetCapacity());
	do {
		//! Create a memory writer to store the header
		ret = avro_file_writer_create_from_writers(writer, datum_writer, bind_data.schema, &file_writer);
		if (ret) {
			auto current_capacity = memory_buffer.GetCapacity();
			memory_buffer.Resize(NextPowerOfTwo(current_capacity * 2));
		}
	} while (ret);

	auto written_bytes = avro_writer_tell(writer);
	WriteData(memory_buffer.GetData(), written_bytes);
	avro_writer_memory_set_dest(writer, (const char *)memory_buffer.GetData(), memory_buffer.GetCapacity());
	interface = avro_generic_class_from_schema(bind_data.schema);
}

static unique_ptr<FunctionData> WriteAvroBind(ClientContext &context, CopyFunctionBindInput &input,
                                              const vector<string> &names, const vector<LogicalType> &sql_types) {
	auto res = make_uniq<WriteAvroBindData>(names, sql_types);
	return res;
}

static unique_ptr<LocalFunctionData> WriteAvroInitializeLocal(ExecutionContext &context, FunctionData &bind_data_p) {
	auto res = make_uniq<WriteAvroLocalState>(bind_data_p);
	return res;
}

static unique_ptr<GlobalFunctionData> WriteAvroInitializeGlobal(ClientContext &context, FunctionData &bind_data_p,
                                                                const string &file_path) {
	auto res = make_uniq<WriteAvroGlobalState>(context, bind_data_p, FileSystem::GetFileSystem(context), file_path);
	auto &bind_data = bind_data_p.Cast<WriteAvroBindData>();

	bind_data.global_state = res.get();
	return res;
}

static idx_t PopulateValue(avro_value_t *target, const Value &val);

static idx_t PopulateValue(avro_value_t *target, const Value &val) {
	auto &type = val.type();

	//! FIXME: add tests for nulls
	auto avro_type = avro_value_get_type(target);
	D_ASSERT(avro_type == AVRO_UNION);
	auto union_value = *target;
	if (val.IsNull()) {
		avro_value_set_branch(&union_value, 0, target);
		avro_value_set_null(target);
		return 1;
	}
	avro_value_set_branch(&union_value, 1, target);
	avro_type = avro_value_get_type(target);

	switch (type.id()) {
	case LogicalTypeId::BOOLEAN: {
		D_ASSERT(avro_type == AVRO_BOOLEAN);
		auto boolean = val.GetValueUnsafe<bool>();
		avro_value_set_boolean(target, boolean);
		return sizeof(bool);
	}
	case LogicalTypeId::BLOB: {
		D_ASSERT(avro_type == AVRO_BYTES);
		auto str = val.GetValueUnsafe<string_t>();
		avro_value_set_bytes(target, (void *)str.GetData(), str.GetSize());
		return str.GetSize();
	}
	case LogicalTypeId::DOUBLE: {
		D_ASSERT(avro_type == AVRO_DOUBLE);
		auto value = val.GetValueUnsafe<double>();
		avro_value_set_double(target, value);
		return sizeof(double);
	}
	case LogicalTypeId::FLOAT: {
		D_ASSERT(avro_type == AVRO_FLOAT);
		auto value = val.GetValueUnsafe<float>();
		avro_value_set_float(target, value);
		return sizeof(float);
	}
	case LogicalTypeId::INTEGER: {
		D_ASSERT(avro_type == AVRO_INT32);
		auto integer = val.GetValueUnsafe<int32_t>();
		avro_value_set_int(target, integer);
		return sizeof(int32_t);
	}
	case LogicalTypeId::BIGINT: {
		D_ASSERT(avro_type == AVRO_INT64);
		auto bigint = val.GetValueUnsafe<int64_t>();
		avro_value_set_long(target, bigint);
		return sizeof(int64_t);
	}
	case LogicalTypeId::VARCHAR: {
		D_ASSERT(avro_type == AVRO_STRING);
		auto str = val.GetValueUnsafe<string_t>();
		avro_value_set_string_len(target, str.GetData(), str.GetSize() + 1);
		return str.GetSize();
	}
	case LogicalTypeId::ENUM: {
		D_ASSERT(avro_type == AVRO_ENUM);
		//! TODO: add support for ENUM
		throw NotImplementedException("Can't convert ENUM Value (%s) to Avro yet", val.ToString());
	}
	case LogicalTypeId::MAP:
	case LogicalTypeId::LIST: {
		D_ASSERT(avro_type == AVRO_ARRAY);
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
		D_ASSERT(avro_type == AVRO_RECORD);
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

		while (avro_file_writer_append_value(global_state.file_writer, &local_state.value)) {
			auto current_capacity = datum_buffer.GetCapacity();
			datum_buffer.ResizeAndCopy(NextPowerOfTwo(current_capacity * 2));
			avro_writer_memory_set_dest_with_offset(global_state.datum_writer, (const char *)datum_buffer.GetData(),
			                                        datum_buffer.GetCapacity(), offset_in_datum_buffer);
		}

		offset_in_datum_buffer = avro_writer_tell(global_state.datum_writer);
		avro_value_reset(&local_state.value);
	}

	auto &buffer = global_state.memory_buffer;
	auto expected_size = avro_writer_tell(global_state.datum_writer);
	expected_size += 16 + 4;
	if (expected_size > buffer.GetCapacity()) {
		//! Resize the buffer in advance, to prevent any need for resizing below
		buffer.Resize(NextPowerOfTwo(expected_size));
		avro_writer_memory_set_dest(global_state.writer, (const char *)buffer.GetData(), buffer.GetCapacity());
	}

	//! Flush the contents to the buffer, if it fails, resize the buffer and try again
	while (avro_file_writer_flush(global_state.file_writer)) {
		auto current_capacity = buffer.GetCapacity();
		buffer.Resize(NextPowerOfTwo(current_capacity * 2));
		avro_writer_memory_set_dest(global_state.writer, (const char *)buffer.GetData(), buffer.GetCapacity());
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
	auto &global_state = gstate.Cast<WriteAvroGlobalState>();

	//! Close the file, finishing the process
	avro_file_writer_close(global_state.file_writer);
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
