#pragma once

#include "duckdb/common/types.hpp"
#include <avro.h>
#include "duckdb/common/optional_idx.hpp"

namespace duckdb {

struct AvroType {
public:
	AvroType() : duckdb_type(LogicalType::INVALID) {
	}
	AvroType(avro_type_t avro_type_p, LogicalType duckdb_type_p, child_list_t<AvroType> children_p = {},
	         unordered_map<idx_t, optional_idx> union_child_map_p = {})
	    : duckdb_type(duckdb_type_p), avro_type(avro_type_p), children(children_p), union_child_map(union_child_map_p) {
	}

public:
	bool operator==(const AvroType &other) const {
		return duckdb_type == other.duckdb_type && avro_type == other.avro_type && children == other.children &&
		       union_child_map == other.union_child_map;
	}

public:
	// we use special transformation rules for unions with null:
	// 1) the null does not become a union entry and
	// 2) if there is only one entry the union disappears and is repaced by its
	// child
	static LogicalType TransformAvroType(const AvroType &avro_type) {
		child_list_t<LogicalType> children;

		switch (avro_type.duckdb_type.id()) {
		case LogicalTypeId::STRUCT: {
			for (auto &child : avro_type.children) {
				children.push_back(std::pair<std::string, LogicalType>(child.first, TransformAvroType(child.second)));
			}
			D_ASSERT(!children.empty());
			return LogicalType::STRUCT(std::move(children));
		}
		case LogicalTypeId::LIST:
			return LogicalType::LIST(TransformAvroType(avro_type.children[0].second));
		case LogicalTypeId::MAP: {
			child_list_t<LogicalType> children;
			children.push_back(std::pair<std::string, LogicalType>("key", LogicalType::VARCHAR));
			children.push_back(
			    std::pair<std::string, LogicalType>("value", TransformAvroType(avro_type.children[0].second)));
			return LogicalType::MAP(LogicalType::STRUCT(std::move(children)));
		}
		case LogicalTypeId::UNION: {
			for (auto &child : avro_type.children) {
				if (child.second.duckdb_type == LogicalTypeId::SQLNULL) {
					continue;
				}
				children.push_back(std::pair<std::string, LogicalType>(child.first, TransformAvroType(child.second)));
			}
			if (children.size() == 1) {
				return children[0].second;
			}
			if (children.empty()) {
				throw InvalidInputException("Empty union type");
			}
			return LogicalType::UNION(std::move(children));
		}
		default:
			return LogicalType(avro_type.duckdb_type);
		}
	}

public:
	LogicalType duckdb_type;
	avro_type_t avro_type;
	child_list_t<AvroType> children;
	unordered_map<idx_t, optional_idx> union_child_map;
};

} // namespace duckdb
