#pragma once

#include "duckdb/common/helper.hpp"
#include "avro_type.hpp"
#include "duckdb/common/multi_file/base_file_reader.hpp"

namespace duckdb {

struct AvroReader;

// this is just a dummy to make the multi file reader compile
struct AvroUnionData {
public:
	AvroUnionData() {
		throw InternalException("union_by_name not supported");
	}

public:
	const string &GetFileName() {
		return file_name;
	}

public:
	string file_name;
	vector<string> names;
	vector<LogicalType> types;
	// AvroOptions options;
	unique_ptr<AvroReader> reader;
};

struct AvroReader : public BaseFileReader {
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

public:
	avro_file_reader_t reader;
	avro_value_t value;
	unique_ptr<Vector> read_vec;

	AllocatedData allocated_data;
	AvroType avro_type;
	LogicalType duckdb_type;
};

} // namespace duckdb
