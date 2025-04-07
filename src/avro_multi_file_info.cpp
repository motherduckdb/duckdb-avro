#include "avro_multi_file_info.hpp"
#include "avro_reader.hpp"

namespace duckdb {

		//if (!AvroNextFile(context, bind_data, global_state, nullptr)) {
		//	return;
		//}

unique_ptr<BaseFileReaderOptions> AvroMultiFileInfo::InitializeOptions(ClientContext &context,
                                                                       optional_ptr<TableFunctionInfo> info) {
	return make_uniq<AvroFileReaderOptions>();
}

bool AvroMultiFileInfo::ParseCopyOption(ClientContext &context, const string &key, const vector<Value> &values,
                                        BaseFileReaderOptions &options_p, vector<string> &expected_names,
                                        vector<LogicalType> &expected_types) {
	// We currently do not have any options for the scanner, so we always return false
	return false;
}

bool AvroMultiFileInfo::ParseOption(ClientContext &context, const string &key, const Value &val,
                                    MultiFileOptions &file_options, BaseFileReaderOptions &options) {
	// We currently do not have any options for the scanner, so we always return false
	return false;
}

void AvroMultiFileInfo::FinalizeCopyBind(ClientContext &context, BaseFileReaderOptions &options_p,
                                         const vector<string> &expected_names,
                                         const vector<LogicalType> &expected_types) {
}

struct AvroMultiFileData final : public TableFunctionData {
public:
	AvroMultiFileData() = default;
};

unique_ptr<TableFunctionData> AvroMultiFileInfo::InitializeBindData(MultiFileBindData &multi_file_data,
                                                                    unique_ptr<BaseFileReaderOptions> options_p) {
	return make_uniq<AvroMultiFileData>();
}

void AvroMultiFileInfo::BindReader(ClientContext &context, vector<LogicalType> &return_types, vector<string> &names,
                                   MultiFileBindData &bind_data) {
	AvroFileReaderOptions options;
	if (bind_data.file_options.union_by_name) {
		throw NotImplementedException("'union_by_name' not implemented for Avro reader yet");
	}
	bind_data.reader_bind = bind_data.multi_file_reader->BindReader<AvroMultiFileInfo>( context, return_types, names, *bind_data.file_list, bind_data, options, bind_data.file_options);
	D_ASSERT(names.size() == return_types.size());
}

void AvroMultiFileInfo::FinalizeBindData(MultiFileBindData &multi_file_data) {
	if (multi_file_data.reader_bind.file_row_number_idx != DConstants::INVALID_INDEX) {
		throw NotImplementedException("'file_row_number' not supported by Avro reader yet");
	}
}

void AvroMultiFileInfo::GetBindInfo(const TableFunctionData &bind_data, BindInfo &info) {
}

optional_idx AvroMultiFileInfo::MaxThreads(const MultiFileBindData &bind_data_p,
                                           const MultiFileGlobalState &global_state, FileExpandResult expand_result) {
	if (expand_result == FileExpandResult::MULTIPLE_FILES) {
		// always launch max threads if we are reading multiple files
		return {};
	}
	// Otherwise, only one thread
	return 1;
}

struct AvroFileGlobalState : public GlobalTableFunctionState {
public:
	AvroFileGlobalState() = default;
	~AvroFileGlobalState() override = default;
public:
	//! TODO: this should contain the state of the current file being scanned
	//! so we can parallelize over a single file
	set<idx_t> files;
};

unique_ptr<GlobalTableFunctionState> AvroMultiFileInfo::InitializeGlobalState(ClientContext &context,
                                                                              MultiFileBindData &bind_data,
                                                                              MultiFileGlobalState &global_state) {
	return make_uniq<AvroFileGlobalState>();
}

//! The Avro Local File State, basically refers to the Scan of one Avro File
//! This is done by calling the Avro Scan directly on one file.
struct AvroFileLocalState : public LocalTableFunctionState {
public:
	explicit AvroFileLocalState(ExecutionContext &execution_context) : execution_context(execution_context) {};
public:
	shared_ptr<AvroReader> file_scan;

	ExecutionContext &execution_context;

	////! Each local state refers to an Avro Scan on a local file
	//unique_ptr<AvroScanFunctionData> local_avro_function_data;
	//unique_ptr<TableFunctionInitInput> init_input;
	//unique_ptr<GlobalTableFunctionState> local_avro_global_state;
	//unique_ptr<LocalTableFunctionState> local_avro_local_state;
	//unique_ptr<TableFunctionInput> table_function_input;
};

unique_ptr<LocalTableFunctionState> AvroMultiFileInfo::InitializeLocalState(ExecutionContext &context,
                                                                            GlobalTableFunctionState &function_state) {
	return make_uniq<AvroFileLocalState>(context);
}

shared_ptr<BaseFileReader> AvroMultiFileInfo::CreateReader(ClientContext &context, GlobalTableFunctionState &gstate_p,
                                                           BaseUnionData &union_data,
                                                           const MultiFileBindData &bind_data) {
	throw NotImplementedException("'union_by_name' is not implemented for the Avro reader yet");
}

shared_ptr<BaseFileReader> AvroMultiFileInfo::CreateReader(ClientContext &context, GlobalTableFunctionState &gstate_p,
                                                           const string &filename, idx_t file_idx,
                                                           const MultiFileBindData &bind_data) {
	return make_shared_ptr<AvroReader>(context, filename);
}

shared_ptr<BaseFileReader> AvroMultiFileInfo::CreateReader(ClientContext &context, const string &filename,
                                                           AvroFileReaderOptions &options,
                                                           const MultiFileOptions &file_options) {
	return make_shared_ptr<AvroReader>(context, filename);
}

shared_ptr<BaseUnionData> AvroMultiFileInfo::GetUnionData(shared_ptr<BaseFileReader> scan_p, idx_t file_idx) {
	throw NotImplementedException("'union_by_name' is not implemented");
}

void AvroMultiFileInfo::FinalizeReader(ClientContext &context, BaseFileReader &reader, GlobalTableFunctionState &) {
}

bool AvroMultiFileInfo::TryInitializeScan(ClientContext &context, const shared_ptr<BaseFileReader> &reader_p,
                                          GlobalTableFunctionState &gstate_p, LocalTableFunctionState &lstate_p) {
	auto &gstate = gstate_p.Cast<AvroFileGlobalState>();
	auto &lstate = lstate_p.Cast<AvroFileLocalState>();
	auto &reader = reader_p->Cast<AvroReader>();
	if (gstate.files.count(reader.file_list_idx.GetIndex())) {
		// Return false because we don't currently support more than one thread
		// scanning a file.
		return false;
	}
	gstate.files.insert(reader.file_list_idx.GetIndex());
	lstate.file_scan = shared_ptr_cast<BaseFileReader, AvroReader>(reader_p);
	return true;
}

void AvroMultiFileInfo::Scan(ClientContext &context, BaseFileReader &reader_p, GlobalTableFunctionState &global_state,
                             LocalTableFunctionState &local_state_p, DataChunk &chunk) {
	auto &reader = reader_p.Cast<AvroReader>();
	reader.Read(chunk);
}

void AvroMultiFileInfo::FinishFile(ClientContext &context, GlobalTableFunctionState &global_state,
                                   BaseFileReader &reader) {
	//! FIXME: this is where the state of the current file being read would be reset
	//! once we support parallelizing over a single file.
}

void AvroMultiFileInfo::FinishReading(ClientContext &context, GlobalTableFunctionState &global_state,
                                      LocalTableFunctionState &local_state) {
}

unique_ptr<NodeStatistics> AvroMultiFileInfo::GetCardinality(const MultiFileBindData &bind_data, idx_t file_count) {
	//! FIXME: Here is where we might set statistics, for optimizations if we have them
	return make_uniq<NodeStatistics>();
}

unique_ptr<BaseStatistics> AvroMultiFileInfo::GetStatistics(ClientContext &context, BaseFileReader &reader,
                                                            const string &name) {
	return nullptr;
}

double AvroMultiFileInfo::GetProgressInFile(ClientContext &context, const BaseFileReader &reader) {
	//auto &file_scan = reader.Cast<AvroReader>();
	//D_ASSERT(is_memory_io(file_scan.reader.reader));
	//struct _avro_reader_memory_t *mem_reader = avro_reader_to_memory(&file_scan.reader.reader);
	//return 100.0 * (mem_reader->read / mem_reader->len);
	return 0;
}

void AvroMultiFileInfo::GetVirtualColumns(ClientContext &, MultiFileBindData &, virtual_column_map_t &result) {
}

} // namespace duckdb
