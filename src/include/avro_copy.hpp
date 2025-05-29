#pragma once

#include "duckdb/function/copy_function.hpp"
#include <avro.h>
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/file_system.hpp"

namespace duckdb {

struct AvroCopyFunction {
	static CopyFunction Create();
};

struct WriteAvroBindData : public FunctionData {
public:
	WriteAvroBindData(const vector<string> &names, const vector<LogicalType> &types);
	WriteAvroBindData() {}
	virtual ~WriteAvroBindData();
public:
	unique_ptr<FunctionData> Copy() const override {
		//! FIXME: actually implement
		return make_uniq<WriteAvroBindData>();
	}

	bool Equals(const FunctionData &other) const override {
		//! FIXME: actually implement
		return true;
	}

public:
	vector<string> names;
	vector<LogicalType> types;

	optional_ptr<GlobalFunctionData> global_state;
	//! The schema of the file to write
	avro_schema_t schema = nullptr;
};

struct AvroValueMapping {
public:
	AvroValueMapping() {}
public:
	avro_value_t target;
	vector<AvroValueMapping> child_mappings;
};

struct WriteAvroLocalState : public LocalFunctionData {
public:
	WriteAvroLocalState(FunctionData &bind_data_p);
	virtual ~WriteAvroLocalState();
public:
	//! Avro value representing a row of the schema
	avro_value_t value;
	//! Mapping from Vector index to avro_value
	vector<AvroValueMapping> mappings;
};

struct WriteAvroGlobalState : public GlobalFunctionData {
public:
	static constexpr idx_t BUFFER_SIZE = STANDARD_VECTOR_SIZE;
public:
	WriteAvroGlobalState(ClientContext &context, FunctionData &bind_data_p, FileSystem &fs, const string &file_path);
	virtual ~WriteAvroGlobalState();
public:
	void WriteData(const_data_ptr_t data, idx_t size) {
		lock_guard<mutex> flock(lock);
		handle->Write((void *)data, size);
	}

	idx_t FileSize() {
		lock_guard<mutex> flock(lock);
		return handle->GetFileSize();
	}
public:
	//! The in-memory stream to write to
	MemoryStream stream;
	FileSystem &fs;
	//! The mutex for writing to the physical file
	mutex lock;
	//! The file handle to write to
	unique_ptr<FileHandle> handle;

	//! The writer for the file
	avro_writer_t writer;
	avro_file_writer_t file_writer;
	//! The interface through which new avro values are created
	avro_value_iface_t *interface = nullptr;
};

} // namespace duckdb
