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

static yyjson_mut_val *CreateJSONType(yyjson_mut_doc *doc, const string &name, const LogicalType &type, bool struct_field = false);

static yyjson_mut_val *CreateNestedType(yyjson_mut_doc *doc, const string &name, const LogicalType &type) {
	//! TODO: actually get the field ids
	int32_t field_id = 42;

	D_ASSERT(type.IsNested());
	auto object = yyjson_mut_obj(doc);
	yyjson_mut_obj_add_strcpy(doc, object, "type", ConvertTypeToAvro(type).c_str());
	yyjson_mut_obj_add_strcpy(doc, object, "name", name.c_str());
	switch (type.id()) {
		case LogicalTypeId::STRUCT: {
			auto &struct_children = StructType::GetChildTypes(type);
			auto fields = yyjson_mut_obj_add_arr(doc, object, "fields");
			for (auto &it : struct_children) {
				auto &child_name = it.first;
				auto &child_type = it.second;
				yyjson_mut_arr_add_val(fields, CreateJSONType(doc, child_name, child_type, true));
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
			if (list_child.IsNested()) {
				auto union_type = yyjson_mut_obj_add_arr(doc, object, "items");
				yyjson_mut_arr_add_strcpy(doc, union_type, "null");
				yyjson_mut_arr_add_val(union_type, CreateNestedType(doc, "element", list_child));
			} else {
				yyjson_mut_obj_add_strcpy(doc, object, "items", ConvertTypeToAvro(list_child).c_str());
			}
			break;
		}
		default:
			throw NotImplementedException("Can't convert nested type '%s' to Avro", type.ToString());
	}
	return object;
}

static yyjson_mut_val *CreateJSONType(yyjson_mut_doc *doc, const string &name, const LogicalType &type, bool struct_field) {
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
			D_ASSERT(!name.empty());
			yyjson_mut_obj_add_strcpy(doc, object, "name", name.c_str());
		}
	} else {
		object = CreateNestedType(doc, name, type);
	}

	auto wrapper = yyjson_mut_obj(doc);
	auto union_type = yyjson_mut_obj_add_arr(doc, wrapper, "type");
	if (struct_field) {
		D_ASSERT(!name.empty());
		yyjson_mut_obj_add_strcpy(doc, wrapper, "name", name.c_str());
	}
	yyjson_mut_arr_add_strcpy(doc, union_type, "null");
	yyjson_mut_arr_add_val(union_type, object);
	return wrapper;
}

static string CreateJSONSchema(const vector<string> &names, const vector<LogicalType> &types) {
	auto doc = yyjson_mut_doc_new(nullptr);
	auto root_object = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root_object);
	yyjson_mut_obj_add_str(doc, root_object, "type", "record");
	//! TODO: get this name from the bind input
	yyjson_mut_obj_add_str(doc, root_object, "name", "manifest_file");
	auto array = yyjson_mut_obj_add_arr(doc, root_object, "fields");

	//! Add all the fields
	D_ASSERT(names.size() == types.size());
	for (idx_t i = 0; i < names.size(); i++) {
		auto &name = names[i];
		auto &type = types[i];
		yyjson_mut_arr_add_val(array, CreateJSONType(doc, name, type, true));
	}

	//! Write the result to a string
	auto data = yyjson_mut_val_write_opts(root_object, YYJSON_WRITE_ALLOW_INF_AND_NAN, nullptr, nullptr, nullptr);
	if (!data) {
		yyjson_mut_doc_free(doc);
		throw InvalidInputException("Could not create a JSON representation of the table schema, yyjson failed");
	}
	auto res = string(data);
	free(data);
	yyjson_mut_doc_free(doc);
	return res;
}

WriteAvroBindData::WriteAvroBindData(const vector<string> &names, const vector<LogicalType> &types) : names(names), types(types) {
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

WriteAvroGlobalState::WriteAvroGlobalState(ClientContext &context, FunctionData &bind_data_p, FileSystem &fs, const string &file_path)
	: allocator(Allocator::Get(context)), memory_buffer(allocator), fs(fs) {
	handle = fs.OpenFile(file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW | FileLockType::WRITE_LOCK | FileCompressionType::AUTO_DETECT);
	auto &bind_data = bind_data_p.Cast<WriteAvroBindData>();

	//! Guess how big the "header" of the Avro file needs to be
	idx_t capacity = MaxValue<idx_t>(BUFFER_SIZE, NextPowerOfTwo(bind_data.json_schema.size() + 16 + 4));
	memory_buffer.Resize(capacity);

	int ret;
	do {
		//! Create a memory writer to store the header
		writer = avro_writer_memory(const_char_ptr_cast(memory_buffer.GetData()), memory_buffer.GetCapacity());
		ret = avro_file_writer_create_from_writer(writer, bind_data.schema, &file_writer);
		if (ret) {
			avro_writer_free(writer);
			auto current_capacity = memory_buffer.GetCapacity();
			memory_buffer.Resize(NextPowerOfTwo(current_capacity * 2));
		}
	} while (ret);

	auto written_bytes = avro_writer_tell(writer);
	WriteData(memory_buffer.GetData(), written_bytes);
	avro_writer_memory_set_dest(writer, (const char *)memory_buffer.GetData(), memory_buffer.GetCapacity());
	interface = avro_generic_class_from_schema(bind_data.schema);
}

static unique_ptr<FunctionData> WriteAvroBind(ClientContext &context, CopyFunctionBindInput &input, const vector<string> &names, const vector<LogicalType> &sql_types) {
	auto res = make_uniq<WriteAvroBindData>(names, sql_types);
	return res;
}

static unique_ptr<LocalFunctionData> WriteAvroInitializeLocal(ExecutionContext &context, FunctionData &bind_data_p) {
	auto res = make_uniq<WriteAvroLocalState>(bind_data_p);
	return res;
}

static unique_ptr<GlobalFunctionData> WriteAvroInitializeGlobal(ClientContext &context, FunctionData &bind_data_p, const string &file_path) {
	auto res = make_uniq<WriteAvroGlobalState>(context, bind_data_p, FileSystem::GetFileSystem(context), file_path);
	auto &bind_data = bind_data_p.Cast<WriteAvroBindData>();

	bind_data.global_state = res.get();
	return res;
}

static void PopulateValue(avro_value_t *target, const Value &val);

static void PopulateValue(avro_value_t *target, const Value &val) {
	auto &type = val.type();

	//! FIXME: add tests for nulls
	D_ASSERT(avro_value_get_type(target) == AVRO_UNION);
	auto union_value = *target;
	if (val.IsNull()) {
		avro_value_set_branch(&union_value, 0, target);
		avro_value_set_null(target);
		return;
	}
	avro_value_set_branch(&union_value, 1, target);
	auto avro_type = avro_value_get_type(target);

	switch (type.id()) {
		case LogicalTypeId::BOOLEAN: {
			D_ASSERT(avro_type == AVRO_BOOLEAN);
			auto boolean = val.GetValueUnsafe<bool>();
			avro_value_set_boolean(target, boolean);
			break;
		}
		case LogicalTypeId::BLOB: {
			D_ASSERT(avro_type == AVRO_BYTES);
			auto str = val.GetValueUnsafe<string_t>();
			avro_value_set_bytes(target, (void *)str.GetData(), str.GetSize());
			break;
		}
		case LogicalTypeId::DOUBLE: {
			D_ASSERT(avro_type == AVRO_DOUBLE);
			auto value = val.GetValueUnsafe<double>();
			avro_value_set_double(target, value);
			break;
		}
		case LogicalTypeId::FLOAT: {
			D_ASSERT(avro_type == AVRO_FLOAT);
			auto value = val.GetValueUnsafe<float>();
			avro_value_set_float(target, value);
			break;
		}
		case LogicalTypeId::INTEGER: {
			D_ASSERT(avro_type == AVRO_INT32);
			auto integer = val.GetValueUnsafe<int32_t>();
			avro_value_set_int(target, integer);
			break;
		}
		case LogicalTypeId::BIGINT: {
			D_ASSERT(avro_type == AVRO_INT64);
			auto bigint = val.GetValueUnsafe<int64_t>();
			avro_value_set_long(target, bigint);
			break;
		}
		case LogicalTypeId::VARCHAR: {
			D_ASSERT(avro_type == AVRO_STRING);
			auto str = val.GetValueUnsafe<string_t>();
			avro_value_set_string_len(target, str.GetData(), str.GetSize() + 1);
			break;
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
			for (idx_t i = 0; i < list_values.size(); i++) {
				auto &list_value = list_values[i];

				avro_value_t item;
				size_t unused_new_index;
				if (avro_value_append(target, &item, &unused_new_index)) {
					throw InvalidInputException(avro_strerror());
				}

				PopulateValue(&item, list_value);
			}
			break;
		}
		case LogicalTypeId::STRUCT: {
			D_ASSERT(avro_type == AVRO_RECORD);
			auto &struct_values = StructValue::GetChildren(val);
			for (idx_t i = 0; i < struct_values.size(); i++) {
				const char *unused_name;
				avro_value_t field;
				if (avro_value_get_by_index(target, i, &field, &unused_name)) {
					throw InvalidInputException(avro_strerror());
				}
				PopulateValue(&field, struct_values[i]);
			}
			break;
		}
		default:
			throw NotImplementedException("PopulateValue not implemented for type %s", type.ToString());
	}
}

static void WriteAvroSink(ExecutionContext &context, FunctionData &bind_data_p, GlobalFunctionData &gstate_p, LocalFunctionData &lstate_p, DataChunk &input) {
	auto &global_state = gstate_p.Cast<WriteAvroGlobalState>();
	auto &local_state = lstate_p.Cast<WriteAvroLocalState>();

	idx_t count = input.size();
	for (idx_t i = 0; i < count; i++) {
		for (idx_t col_idx = 0; col_idx < input.ColumnCount(); col_idx++) {
			auto val = input.GetValue(col_idx, i);

			const char *unused_name;
			avro_value_t column;
			if (avro_value_get_by_index(&local_state.value, col_idx, &column, &unused_name)) {
				throw InvalidInputException(avro_strerror());
			}
			PopulateValue(&column, val);
		}
		avro_file_writer_append_value(global_state.file_writer, &local_state.value);
		avro_value_reset(&local_state.value);
	}

	auto &buffer = global_state.memory_buffer;
	auto expected_size = avro_file_writer_datum_writer_tell(global_state.file_writer);
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

static void WriteAvroCombine(ExecutionContext &context, FunctionData &bind_data, GlobalFunctionData &gstate, LocalFunctionData &lstate) {
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
