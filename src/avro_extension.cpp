#define DUCKDB_EXTENSION_MAIN

#include "avro_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "utf8proc_wrapper.hpp"

#include "avro.h"

namespace duckdb {

struct AvroBindData : FunctionData {

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

  names.push_back(avro_schema_type_name(avro_schema));
  return_types.push_back(std::move(type));

  avro_schema_decref(avro_schema);
  avro_file_reader_close(reader);

  return bind_data;
}

static void TransformValue(avro_value *avro_val, Vector &target,
                           idx_t out_idx) {

  auto &type = target.GetType();
  switch (type.id()) {
  case LogicalType::SQLNULL: {
    break;
  }

  case LogicalType::BOOLEAN: {
    int bool_val;
    if (avro_value_get_boolean(avro_val, &bool_val)) {
      throw InvalidInputException(avro_strerror());
    }
    FlatVector::GetData<uint8_t>(target)[out_idx] = bool_val != 0;
    break;
  }
  case LogicalType::INTEGER: {
    if (avro_value_get_int(avro_val,
                           &FlatVector::GetData<int32_t>(target)[out_idx])) {
      throw InvalidInputException(avro_strerror());
    }
    break;
  }
  case LogicalType::BIGINT: {
    if (avro_value_get_long(avro_val,
                            &FlatVector::GetData<int64_t>(target)[out_idx])) {
      throw InvalidInputException(avro_strerror());
    }
    break;
  }
  case LogicalType::DOUBLE: {
    if (avro_value_get_double(avro_val,
                              &FlatVector::GetData<double>(target)[out_idx])) {
      throw InvalidInputException(avro_strerror());
    }
    break;
  }
  case LogicalType::BLOB:
  case LogicalType::VARCHAR: {
    avro_wrapped_buffer str_buf = AVRO_WRAPPED_BUFFER_EMPTY;
    if (avro_value_grab_string(avro_val, &str_buf)) {
      throw InvalidInputException(avro_strerror());
    }
    // avro strings are null-terminated
    D_ASSERT(const_char_ptr_cast(str_buf.buf)[str_buf.size - 1] == '\0');
    if (type.id() == LogicalType::VARCHAR &&
        Utf8Proc::Analyze(const_char_ptr_cast(str_buf.buf), str_buf.size - 1) ==
            UnicodeType::INVALID) {
      throw InvalidInputException("Avro file contains invalid unicode");
    }
    FlatVector::GetData<string_t>(target)[out_idx] =
        StringVector::AddStringOrBlob(target, const_char_ptr_cast(str_buf.buf),
                                      str_buf.size - 1);
    str_buf.free(&str_buf);
    break;
  }
  case LogicalTypeId::STRUCT: {
    child_list_t<Value> children;
    size_t child_count;
    if (avro_value_get_size(avro_val, &child_count)) {
      throw InvalidInputException(avro_strerror());
    }
    for (idx_t child_idx = 0; child_idx < child_count; child_idx++) {
      avro_value child_value;
      if (avro_value_get_by_index(avro_val, child_idx, &child_value, nullptr)) {
        throw InvalidInputException(avro_strerror());
      }
      TransformValue(&child_value, *StructVector::GetEntries(target)[child_idx],
                     out_idx);
    }
    break;
  }
  case LogicalTypeId::UNION: {
    int discriminant;
    avro_value union_value;
    if (avro_value_get_discriminant(avro_val, &discriminant) ||
        avro_value_get_current_branch(avro_val, &union_value)) {
      throw InvalidInputException(avro_strerror());
    }
    if (discriminant >= UnionType::GetMemberCount(type)) {
      throw InvalidInputException("Invalid union tag");
    }
    for (idx_t union_idx = 0; union_idx < UnionType::GetMemberCount(type);
         union_idx++) {
      FlatVector::Validity(UnionVector::GetMember(target, union_idx))
          .SetInvalid(out_idx);
    }

    auto &tags = UnionVector::GetTags(target);
    FlatVector::GetData<union_tag_t>(tags)[out_idx] = discriminant;
    auto &union_vector = UnionVector::GetMember(target, discriminant);
    TransformValue(&union_value, union_vector, out_idx);
    FlatVector::Validity(union_vector).SetValid(out_idx);

    break;
  }
  default:
    throw NotImplementedException(type.ToString());
  }
}

struct AvroGlobalState : GlobalTableFunctionState {
  ~AvroGlobalState() {
    // avro_value_decref(&value);
    //
    // avro_file_reader_close(reader);
  }
  AvroGlobalState(string filename) {
    if (avro_file_reader(filename.c_str(), &reader)) {
      throw InvalidInputException(avro_strerror());
    }
    auto schema = avro_file_reader_get_writer_schema(reader);
    auto interface = avro_generic_class_from_schema(schema);
    avro_schema_decref(schema);
    avro_generic_value_new(interface, &value);
    avro_value_iface_decref(interface);
  }
  avro_file_reader_t reader;
  avro_schema_t schema;
  avro_value_t value;
};

static void AvroTableFunction(ClientContext &context, TableFunctionInput &data,
                              DataChunk &output) {

  D_ASSERT(output.data.size() == 1);
  D_ASSERT(output.GetTypes().size() == 1);
  D_ASSERT(output.GetTypes()[0].id() == LogicalTypeId::STRUCT);
  auto global_state = data.global_state->Cast<AvroGlobalState>();
  idx_t out_idx = 0;
  while (avro_file_reader_read_value(global_state.reader,
                                     &global_state.value) == 0) {
    TransformValue(&global_state.value, output.data[0], out_idx++);
    if (out_idx == STANDARD_VECTOR_SIZE) {
      break;
    }
  }
  output.SetCardinality(out_idx);
}

static unique_ptr<GlobalTableFunctionState>
AvroGlobalInit(ClientContext &context, TableFunctionInitInput &input) {
  return make_uniq<AvroGlobalState>(
      input.bind_data->Cast<AvroBindData>().filename);
}

static void LoadInternal(DatabaseInstance &instance) {
  // Register a scalar function
  auto avro_read_function =
      TableFunction("read_avro", {LogicalType::VARCHAR}, AvroTableFunction,
                    AvroBindFunction, AvroGlobalInit);
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
