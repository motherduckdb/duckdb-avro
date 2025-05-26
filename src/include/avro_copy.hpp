#pragma once

#include "duckdb/function/copy_function.hpp"
#include <avro.h>

namespace duckdb {

struct AvroCopyFunction {
	static CopyFunction Create();
};

} // namespace duckdb
