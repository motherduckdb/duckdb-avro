#define DUCKDB_EXTENSION_MAIN

#include "avro_extension.hpp"

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"

#include "duckdb/main/extension_util.hpp"
#include "include/avro_reader.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "avro_multi_file_info.hpp"
#include "duckdb/common/multi_file/multi_file_function.hpp"

#include <avro.h>

namespace duckdb {

static void LoadInternal(DatabaseInstance &instance) {
	// Register a scalar function
	auto table_function = MultiFileFunction<AvroMultiFileInfo>("read_avro");
	table_function.projection_pushdown = true;
	ExtensionUtil::RegisterFunction(instance, MultiFileReader::CreateFunctionSet(table_function));
}

void AvroExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string AvroExtension::Name() {
	return "avro";
}

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
