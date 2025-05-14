#pragma once

#include "duckdb/common/helper.hpp"
#include "avro_type.hpp"
#include "duckdb/common/multi_file/base_file_reader.hpp"

namespace duckdb {

class AvroReader : public BaseFileReader {
public:
	AvroReader(ClientContext &context, const string filename_p);

	~AvroReader() {
		avro_value_decref(&value);
		avro_file_reader_close(reader);
	}

public:
	void Read(DataChunk &output);

	string GetReaderType() const override {
		return "Avro";
	}

	bool TryInitializeScan(ClientContext &context, GlobalTableFunctionState &gstate,
	                       LocalTableFunctionState &lstate) override;
	void Scan(ClientContext &context, GlobalTableFunctionState &global_state, LocalTableFunctionState &local_state,
	          DataChunk &chunk) override;

public:
	avro_file_reader_t reader;
	avro_value_t value;
	unique_ptr<Vector> read_vec;

	AllocatedData allocated_data;
	AvroType avro_type;
	LogicalType duckdb_type;
};

} // namespace duckdb
