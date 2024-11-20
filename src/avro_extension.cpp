#define DUCKDB_EXTENSION_MAIN

#include "avro_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

#include "avro.h"

namespace duckdb {

struct AvroBindData : public FunctionData {

  string filename;

  bool Equals(const FunctionData &other_p) const {
    const AvroBindData &other = static_cast<const AvroBindData &>(other_p);
    return filename == other.filename;
  }

  unique_ptr<FunctionData> Copy() const {
    auto bind_data = make_uniq<AvroBindData>();
    bind_data->filename = filename;
    return bind_data;
  }
};

// map
// array
// ...

static LogicalType TransformSchema(avro_schema_t &avro_schema) {
  auto raw_type_name = avro_schema_type_name(avro_schema);
  if (!raw_type_name || strlen(raw_type_name) == 0) {
    throw InvalidInputException("Empty avro type name");
  }
  auto type_name = string(raw_type_name);
  if (type_name == "string") {
    return LogicalType::VARCHAR;
  } else if (type_name == "bytes") {
    return LogicalType::BLOB;
  } else if (type_name == "int") {
    return LogicalType::INTEGER;
  } else if (type_name == "long") {
    return LogicalType::BIGINT;
  } else if (type_name == "float") {
    return LogicalType::FLOAT;
  } else if (type_name == "double") {
    return LogicalType::DOUBLE;
  } else if (type_name == "boolean") {
    return LogicalType::BOOLEAN;
  } else if (type_name == "null") {
    return LogicalType::SQLNULL;
  } else if (type_name == "map") {
    throw NotImplementedException("Avro map type not implemented");
  } else if (type_name == "array") {
    throw NotImplementedException("Avro array type not implemented");
  } else if (type_name == "union") {
    auto num_children = avro_schema_union_size(avro_schema);
    child_list_t<LogicalType> union_children;
    for (idx_t child_idx = 0; child_idx < num_children; child_idx++) {
      auto child_schema = avro_schema_union_branch(avro_schema, child_idx);
      auto child_type = TransformSchema(child_schema);
      union_children.push_back(std::pair<std::string, LogicalType>(
          StringUtil::Format("u%llu", child_idx), std::move(child_type)));
    }
    return LogicalType::UNION(std::move(union_children));
  } else {
    // a Avro 'record', so a DuckDB STRUCT
    auto num_children = avro_schema_record_size(avro_schema);
    child_list_t<LogicalType> struct_children;
    for (idx_t child_idx = 0; child_idx < num_children; child_idx++) {
      auto child_schema =
          avro_schema_record_field_get_by_index(avro_schema, child_idx);
      auto child_type = TransformSchema(child_schema);
      auto child_name = avro_schema_record_field_name(avro_schema, child_idx);
      if (!child_name || strlen(child_name) == 0) {
        throw InvalidInputException("Empty avro field name");
      }
      struct_children.push_back(std::pair<std::string, LogicalType>(
          child_name, std::move(child_type)));
    }

    return LogicalType::STRUCT(std::move(struct_children));
  }
}

static unique_ptr<FunctionData>
AvroBindFunction(ClientContext &context, TableFunctionBindInput &input,
                 vector<LogicalType> &return_types, vector<string> &names) {
  auto bind_data = make_uniq<AvroBindData>();
  bind_data->filename = input.inputs[0].ToString();
  avro_file_reader_t reader;
  if (avro_file_reader(bind_data->filename.c_str(), &reader)) {
    throw InvalidInputException(avro_strerror());
  }

  auto avro_schema = avro_file_reader_get_writer_schema(reader);
  auto type = TransformSchema(avro_schema);
  avro_schema_decref(avro_schema);
  avro_file_reader_close(reader);

  if (type.id() == LogicalTypeId::STRUCT) {
    for (idx_t child_idx = 0; child_idx < StructType::GetChildCount(type);
         child_idx++) {
      names.push_back(StructType::GetChildName(type, child_idx));
      return_types.push_back(
          std::move(StructType::GetChildType(type, child_idx)));
    }
  } else {
    names.push_back("avro_root");
    return_types.push_back(std::move(type));
  }

  return bind_data;
}

static void AvroTableFunction(ClientContext &context, TableFunctionInput &data,
                              DataChunk &output) {

  //
  // auto num_columns = avro_schema_record_size(wschema);
  //
  // printf("%s %s\n", avro_schema_name(wschema),
  // avro_schema_type_name(wschema));
  //
  //
  // for (idx_t col_idx=0; col_idx < num_columns; col_idx++) {
  // 	auto col_schema =  avro_schema_record_field_get_by_index(wschema,
  // col_idx);
  //
  // 	printf("%s %s %s\n",  avro_schema_name(col_schema),
  // avro_schema_record_field_name(wschema, col_idx),
  // avro_schema_type_name(col_schema));
  // }
  //
  // auto iface = avro_generic_class_from_schema(wschema);
  // avro_generic_value_new(iface, &value);
  // int rval;
  //
  //
  // while ((rval = avro_file_reader_read_value(reader, &value)) == 0) {
  // 	char  *json;
  //
  // 	if (avro_value_to_json(&value, 1, &json)) {
  // 		fprintf(stderr, "Error converting value to JSON: %s\n",
  // 			avro_strerror());
  // 	} else {
  // 		//printf("%s\n", json);
  // 		free(json);
  // 	}
  //
  // 	avro_value_reset(&value);
  // }
  //
  // // If it was not an EOF that caused it to fail,
  // // print the error.
  // if (rval != EOF) {
  // 	fprintf(stderr, "Error: %s\n", avro_strerror());
  // }
  //
  //
  // avro_value_decref(&value);
  // avro_value_iface_decref(iface);
  // avro_schema_decref(wschema);
}

static void LoadInternal(DatabaseInstance &instance) {
  // Register a scalar function
  auto avro_read_function = TableFunction("read_avro", {LogicalType::VARCHAR},
                                          AvroTableFunction, AvroBindFunction);
  ExtensionUtil::RegisterFunction(instance, avro_read_function);
}

void AvroExtension::Load(DuckDB &db) { LoadInternal(*db.instance); }
std::string AvroExtension::Name() { return "avro"; }

std::string AvroExtension::Version() const {
#ifdef EXT_VERSION_AVRO
  return EXT_VERSION_AVRO;
#else
  return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void avro_init(duckdb::DatabaseInstance &db) {
  duckdb::DuckDB db_wrapper(db);
  db_wrapper.LoadExtension<duckdb::AvroExtension>();
}

DUCKDB_EXTENSION_API const char *avro_version() {
  return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
