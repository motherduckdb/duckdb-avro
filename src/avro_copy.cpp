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

static yyjson_mut_val *CreateJSONType(yyjson_mut_doc *doc, const string &name, const LogicalType &type, optional_ptr<const LogicalType> parent_type) {
	auto object = yyjson_mut_obj(doc);

	if (!name.empty()) {
		yyjson_mut_obj_add_str(doc, object, "name", name.c_str());
	}

	//! TODO: actually get the field ids
	int32_t field_id = 42;

	switch (type.id()) {
		case LogicalTypeId::STRUCT: {
			auto &struct_children = StructType::GetChildTypes(type);
			auto fields = yyjson_mut_obj_add_arr(doc, object, "fields");
			for (auto &it : struct_children) {
				auto &child_name = it.first;
				auto &child_type = it.second;
				yyjson_mut_arr_add_val(fields, CreateJSONType(doc, child_name, child_type, &type));
			}
			break;
		}
		case LogicalTypeId::LIST: {
			//! NOTE: This is seemingly a work-around of a limitation in either the Avro spec, or the avro-c implementation
			//! 'array' can not be directly part of a struct field
			auto union_type = yyjson_mut_obj_add_arr(doc, object, "type");
			//! First item of the union is null, to indicate that the field is nullable
			yyjson_mut_arr_add_str(doc, union_type, "null");

			yyjson_mut_obj_add_int(doc, object, "element-id", field_id);

			auto list_object = yyjson_mut_obj(doc);

			yyjson_mut_obj_add_str(doc, list_object, "type", "array");
			auto &list_child = ListType::GetChildType(type);
			if (list_child.IsNested()) {
				yyjson_mut_obj_add_val(doc, list_object, "items", CreateJSONType(doc, "element", list_child, &type));
			} else {
				yyjson_mut_obj_add_strcpy(doc, list_object, "items", ConvertTypeToAvro(list_child).c_str());
			}

			yyjson_mut_arr_add_val(union_type, list_object);
			break;
		}
		case LogicalTypeId::MAP: {
			auto &child_type = ListType::GetChildType(type);
			yyjson_mut_obj_add_strcpy(doc, object, "logicalType", "map");
			yyjson_mut_obj_add_val(doc, object, "items", CreateJSONType(doc, "key_value", child_type, &type));
			break;
		}
		default:
			break;
	}

	if (type.id() != LogicalTypeId::LIST && type.id() != LogicalTypeId::MAP) {
		yyjson_mut_obj_add_strcpy(doc, object, "type", ConvertTypeToAvro(type).c_str());
		yyjson_mut_obj_add_int(doc, object, "field-id", field_id);
	}

	return object;
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
		yyjson_mut_arr_add_val(array, CreateJSONType(doc, name, type, nullptr));
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

WriteAvroBindData::WriteAvroBindData(const vector<string> &names, const vector<LogicalType> &types) {
	//! TODO: create the avro schema from the provided names and types
	//! Actually, looking through the code of 'avro_schema_from_json_literal', it might be easier to just generate JSON and use that method
	auto json_schema = CreateJSONSchema(names, types);
	Printer::Print(json_schema);
	if (avro_schema_from_json_length(json_schema.c_str(), json_schema.size(), &schema)) {
		throw InvalidInputException(avro_strerror());
	}

	auto t = 1 + 42;
	(void)t;
}

WriteAvroBindData::~WriteAvroBindData() {
	//! Make sure to free the schema
	avro_schema_decref(schema);
}

WriteAvroGlobalState::WriteAvroGlobalState(ClientContext &context, FunctionData &bind_data_p, FileSystem &fs, const string &file_path)
	: stream(Allocator::Get(context), BUFFER_SIZE), fs(fs) {
	handle = fs.OpenFile(file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW | FileLockType::WRITE_LOCK | FileCompressionType::AUTO_DETECT);
	writer = avro_writer_memory(const_char_ptr_cast(stream.GetData()), stream.GetCapacity());
	auto &bind_data = bind_data_p.Cast<WriteAvroBindData>();

	if (avro_file_writer_create_from_writer(writer, bind_data.schema, &file_writer)) {
		throw InvalidInputException(avro_strerror());
	}
}

static unique_ptr<FunctionData> WriteAvroBind(ClientContext &context, CopyFunctionBindInput &input, const vector<string> &names, const vector<LogicalType> &sql_types) {
	auto res = make_uniq<WriteAvroBindData>(names, sql_types);
	return res;
}

static unique_ptr<LocalFunctionData> WriteAvroInitializeLocal(ExecutionContext &context, FunctionData &bind_data) {
	//! Local state unused for now, since we only support single threaded Avro writes currently
	auto res = make_uniq<WriteAvroLocalState>();
	return res;
}

static unique_ptr<GlobalFunctionData> WriteAvroInitializeGlobal(ClientContext &context, FunctionData &bind_data_p, const string &file_path) {
	auto res = make_uniq<WriteAvroGlobalState>(context, bind_data_p, FileSystem::GetFileSystem(context), file_path);

	auto &bind_data = bind_data_p.Cast<WriteAvroBindData>();
	bind_data.global_state = res.get();
	//! Create the file to write, this already creates the Avro header
	// avro_file_writer_create_with_codec();

	return res;
}

static void WriteAvroSink(ExecutionContext &context, FunctionData &bind_data_p, GlobalFunctionData &gstate, LocalFunctionData &lstate, DataChunk &input) {

	(void)gstate;
	auto &bind_data = bind_data_p.Cast<WriteAvroBindData>();
	//! This finalizes a Data Block (writes the record count and the 'sync' footer for the block)
	// avro_file_writer_flush

	return;
}

static void WriteAvroCombine(ExecutionContext &context, FunctionData &bind_data, GlobalFunctionData &gstate, LocalFunctionData &lstate) {
	return;
}

static void WriteAvroFinalize(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate) {

	//! Close the file, finishing the process
	// avro_file_writer_close
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
