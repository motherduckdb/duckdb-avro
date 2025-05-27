#include "avro_copy.hpp"

#include "duckdb/common/enums/file_compression_type.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/function/function.hpp"

namespace duckdb {

struct WriteAvroBindData : public FunctionData {
public:
	WriteAvroBindData() {}
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
	//! TODO: store any data immutable data created during bind
	optional_ptr<GlobalFunctionData> global_state;
};

struct WriteAvroLocalState : public LocalFunctionData {
public:
	WriteAvroLocalState() {}
public:
	//! TODO: store any data required by a single thread to write to the Avro file
};

struct WriteAvroGlobalState : public GlobalFunctionData {
public:
	WriteAvroGlobalState(FileSystem &fs, const string &file_path)
	    : fs(fs) {
		handle = fs.OpenFile(file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW | FileLockType::WRITE_LOCK | FileCompressionType::AUTO_DETECT);
	}

	//! Write a finished data block to the file
	void WriteDataBlock(const_data_ptr_t data, idx_t size) {
		lock_guard<mutex> flock(lock);
		handle->Write((void *)data, size);
	}

	idx_t FileSize() {
		lock_guard<mutex> flock(lock);
		return handle->GetFileSize();
	}

	FileSystem &fs;
	//! The mutex for writing to the physical file
	mutex lock;
	//! The file handle to write to
	unique_ptr<FileHandle> handle;

	//! The writer for the file
	avro_file_writer_t writer;
	//! The avro schema created from the input
	avro_schema_t avro_schema;
};

static unique_ptr<FunctionData> WriteAvroBind(ClientContext &context, CopyFunctionBindInput &input, const vector<string> &names, const vector<LogicalType> &sql_types) {
	auto res = make_uniq<WriteAvroBindData>();

	return res;
}

static unique_ptr<LocalFunctionData> WriteAvroInitializeLocal(ExecutionContext &context, FunctionData &bind_data) {
	auto res = make_uniq<WriteAvroLocalState>();

	return res;
}

static unique_ptr<GlobalFunctionData> WriteAvroInitializeGlobal(ClientContext &context, FunctionData &bind_data_p, const string &file_path) {
	auto res = make_uniq<WriteAvroGlobalState>(FileSystem::GetFileSystem(context), file_path);

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

CopyFunction AvroCopyFunction::Create() {
	CopyFunction function("avro");
	function.extension = "avro";

	function.copy_to_bind = WriteAvroBind;
	function.copy_to_initialize_local = WriteAvroInitializeLocal;
	function.copy_to_initialize_global = WriteAvroInitializeGlobal;
	function.copy_to_sink = WriteAvroSink;
	function.copy_to_combine = WriteAvroCombine;
	function.copy_to_finalize = WriteAvroFinalize;
	return function;
}

} // namespace duckdb
