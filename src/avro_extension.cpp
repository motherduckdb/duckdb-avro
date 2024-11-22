#define DUCKDB_EXTENSION_MAIN

#include "avro_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include <sys/socket.h>

#include "utf8proc_wrapper.hpp"

#include "avro.h"

namespace duckdb {

struct AvroType {
  AvroType() : type(LogicalTypeId::INVALID) {}

  AvroType(LogicalTypeId type_p, child_list_t<AvroType> children_p = {})
      : type(type_p), children(children_p) {}
  LogicalTypeId type;
  child_list_t<AvroType> children;

  bool operator==(const AvroType &other) const {
    return type == other.type && children == other.children;
  }
};

struct AvroBindData : FunctionData {

  string filename;
  AvroType avro_type;
  LogicalType duckdb_type;

  bool Equals(const FunctionData &other_p) const override {
    const AvroBindData &other = static_cast<const AvroBindData &>(other_p);
    return filename == other.filename && avro_type == other.avro_type &&
           duckdb_type == other.duckdb_type;
  }

  unique_ptr<FunctionData> Copy() const override {
    auto bind_data = make_uniq<AvroBindData>();
    bind_data->filename = filename;
    bind_data->avro_type = avro_type;
    bind_data->duckdb_type = duckdb_type;

    return bind_data;
  }
};

// we use special transformation rules for unions with null:
// 1) the null does not become a union entry and
// 2) if there is only one entry the union disappears and is repaced by its
// child
static LogicalType TransformAvroType(const AvroType &avro_type) {
  child_list_t<LogicalType> children;

  switch (avro_type.type) {
  case LogicalTypeId::STRUCT: {
    for (auto &child : avro_type.children) {
      children.push_back(std::pair<std::string, LogicalType>(
          child.first, TransformAvroType(child.second)));
    }
    return LogicalType::STRUCT(std::move(children));
  }
  case LogicalTypeId::UNION: {
    for (auto &child : avro_type.children) {
      if (child.second.type == LogicalTypeId::SQLNULL) {
        continue;
      }
      children.push_back(std::pair<std::string, LogicalType>(
          child.first, TransformAvroType(child.second)));
    }
    if (children.size() == 1) {
      return children[0].second;
    }
    return LogicalType::UNION(std::move(children));
  }
  case LogicalTypeId::SQLNULL:
    throw InternalException("This should not happen");
  default:
    return LogicalType(avro_type.type);
  }
}

static AvroType TransformSchema(avro_schema_t &avro_schema) {
  auto raw_type_name = avro_schema_type_name(avro_schema);
  if (!raw_type_name || strlen(raw_type_name) == 0) {
    throw InvalidInputException("Empty avro type name");
  }
  auto type_name = string(raw_type_name);
  if (type_name == "string") {
    return AvroType(LogicalType::VARCHAR);
  } else if (type_name == "bytes") {
    return AvroType(LogicalType::BLOB);
  } else if (type_name == "int") {
    return AvroType(LogicalType::INTEGER);
  } else if (type_name == "long") {
    return AvroType(LogicalType::BIGINT);
  } else if (type_name == "float") {
    return AvroType(LogicalType::FLOAT);
  } else if (type_name == "double") {
    return AvroType(LogicalType::DOUBLE);
  } else if (type_name == "boolean") {
    return AvroType(LogicalType::BOOLEAN);
  } else if (type_name == "null") {
    return AvroType(LogicalType::SQLNULL);
  } else if (type_name == "map") {
    throw NotImplementedException("Avro map type not implemented");
  } else if (type_name == "array") {
    throw NotImplementedException("Avro array type not implemented");
  } else if (type_name == "union") {
    auto num_children = avro_schema_union_size(avro_schema);
    child_list_t<AvroType> union_children;
    for (idx_t child_idx = 0; child_idx < num_children; child_idx++) {
      auto child_schema = avro_schema_union_branch(avro_schema, child_idx);
      auto child_type = TransformSchema(child_schema);
      union_children.push_back(std::pair<std::string, AvroType>(
          StringUtil::Format("u%llu", child_idx), std::move(child_type)));
    }
    return AvroType(LogicalTypeId::UNION, std::move(union_children));
  } else {
    // a Avro 'record', so a DuckDB STRUCT
    auto num_children = avro_schema_record_size(avro_schema);
    child_list_t<AvroType> struct_children;
    for (idx_t child_idx = 0; child_idx < num_children; child_idx++) {
      auto child_schema =
          avro_schema_record_field_get_by_index(avro_schema, child_idx);
      auto child_type = TransformSchema(child_schema);
      auto child_name = avro_schema_record_field_name(avro_schema, child_idx);
      if (!child_name || strlen(child_name) == 0) {
        throw InvalidInputException("Empty avro field name");
      }
      struct_children.push_back(
          std::pair<std::string, AvroType>(child_name, std::move(child_type)));
    }

    return AvroType(LogicalTypeId::STRUCT, std::move(struct_children));
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
  bind_data->avro_type = TransformSchema(avro_schema);

  bind_data->duckdb_type = TransformAvroType(bind_data->avro_type);
  auto &duckdb_type = bind_data->duckdb_type;

  // special handling for root structs, we pull up the entries
  if (duckdb_type.id() == LogicalTypeId::STRUCT) {
    for (idx_t child_idx = 0;
         child_idx < StructType::GetChildCount(duckdb_type); child_idx++) {
      names.push_back(StructType::GetChildName(duckdb_type, child_idx));
      return_types.push_back(StructType::GetChildType(duckdb_type, child_idx));
    }
  } else {
    names.push_back(avro_schema_type_name(avro_schema));
    return_types.push_back(std::move(duckdb_type));
  }

  avro_schema_decref(avro_schema);
  avro_file_reader_close(reader);

  return bind_data;
}

static void TransformValue(avro_value *avro_val, AvroType &avro_type,
                           Vector &target, idx_t out_idx) {

  switch (avro_type.type) {
  case LogicalType::BOOLEAN: {
    int bool_val;
    if (avro_value_get_boolean(avro_val, &bool_val)) {
      throw InvalidInputException(avro_strerror());
    }
    FlatVector::GetData<uint8_t>(target)[out_idx] = bool_val != 0;
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
  case LogicalType::FLOAT: {
    if (avro_value_get_float(avro_val,
                             &FlatVector::GetData<float>(target)[out_idx])) {
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
    if (avro_type.type == LogicalType::VARCHAR &&
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
    D_ASSERT(child_count == StructType::GetChildCount(target.GetType()));
    D_ASSERT(child_count == avro_type.children.size());

    for (idx_t child_idx = 0; child_idx < child_count; child_idx++) {
      avro_value child_value;
      if (avro_value_get_by_index(avro_val, child_idx, &child_value, nullptr)) {
        throw InvalidInputException(avro_strerror());
      }
      TransformValue(&child_value, avro_type.children[child_idx].second,
                     *StructVector::GetEntries(target)[child_idx], out_idx);
    }
    break;
  }

  // TODO what about single-child unions in general?? have a test!
  case LogicalTypeId::UNION: {

    int discriminant;
    avro_value union_value;
    if (avro_value_get_discriminant(avro_val, &discriminant) ||
        avro_value_get_current_branch(avro_val, &union_value)) {
      throw InvalidInputException(avro_strerror());
    }
    if (discriminant >= avro_type.children.size()) {
      throw InvalidInputException("Invalid union tag");
    }

    // if the target is a union, we set everything to null to start with
    if (target.GetType().id() == LogicalTypeId::UNION) {
      for (idx_t union_idx = 0;
           union_idx < UnionType::GetMemberCount(target.GetType());
           union_idx++) {
        FlatVector::Validity(UnionVector::GetMember(target, union_idx))
            .SetInvalid(out_idx);
      }
    }
    FlatVector::Validity(target).SetInvalid(out_idx);

    if (avro_type.children[discriminant].second.type !=
        LogicalTypeId::SQLNULL) {
      if (target.GetType().id() == LogicalTypeId::UNION) {
        auto &tags = UnionVector::GetTags(target);
        // TODO this is excessive for every value
        int duckdb_struct_idx = 0;
        for (idx_t child_idx = 0; child_idx < discriminant; child_idx++) {
          if (avro_type.children[discriminant].second.type !=
              LogicalTypeId::SQLNULL) {
            duckdb_struct_idx++;
          }
        }
        FlatVector::GetData<union_tag_t>(tags)[out_idx] = duckdb_struct_idx;
        auto &union_vector = UnionVector::GetMember(target, duckdb_struct_idx);
        TransformValue(&union_value, avro_type.children[discriminant].second,
                       union_vector, out_idx);
      } else { // directly recurse, we have dissolved the union
        TransformValue(&union_value, avro_type.children[discriminant].second,
                       target, out_idx);
      }
      FlatVector::Validity(target).SetValid(out_idx);
    }

    break;
  }
  default:
    throw NotImplementedException(LogicalTypeIdToString(avro_type.type));
  }
}

struct AvroGlobalState : GlobalTableFunctionState {
  ~AvroGlobalState() {
    avro_value_decref(&value);
    avro_file_reader_close(reader);
  }
  AvroGlobalState(string filename, const LogicalType &duckdb_type) {
    if (avro_file_reader(filename.c_str(), &reader)) {
      throw InvalidInputException(avro_strerror());
    }
    auto schema = avro_file_reader_get_writer_schema(reader);
    auto interface = avro_generic_class_from_schema(schema);
    avro_schema_decref(schema);
    avro_generic_value_new(interface, &value);
    avro_value_iface_decref(interface);
    read_vec = make_uniq<Vector>(duckdb_type);
  }
  avro_file_reader_t reader;
  avro_schema_t schema;
  avro_value_t value;
  unique_ptr<Vector> read_vec;
};

static void AvroTableFunction(ClientContext &context, TableFunctionInput &data,
                              DataChunk &output) {
  auto bind_data = data.bind_data->Cast<AvroBindData>();

  auto &global_state = data.global_state->Cast<AvroGlobalState>();
  idx_t out_idx = 0;

  while (avro_file_reader_read_value(global_state.reader,
                                     &global_state.value) == 0) {
    TransformValue(&global_state.value, bind_data.avro_type,
                   *global_state.read_vec, out_idx++);
    if (out_idx == STANDARD_VECTOR_SIZE) {
      break;
    }
  }

  // pull up root struct into output chunk
  if (bind_data.duckdb_type.id() == LogicalTypeId::STRUCT) {
    for (idx_t col_idx = 0; col_idx < output.ColumnCount(); col_idx++) {
      output.data[col_idx].Reference(
          *StructVector::GetEntries(*global_state.read_vec)[col_idx]);
    }
  }

  output.SetCardinality(out_idx);
}

static unique_ptr<GlobalTableFunctionState>
AvroGlobalInit(ClientContext &context, TableFunctionInitInput &input) {
  auto &bind_data = input.bind_data->Cast<AvroBindData>();
  return make_uniq<AvroGlobalState>(bind_data.filename, bind_data.duckdb_type);
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
