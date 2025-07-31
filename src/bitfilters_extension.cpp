#include "bitfilters_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include <boost/bloom/filter.hpp>
#include "bloom_filter.hpp"

namespace duckdb {

static void LoadInternal(DatabaseInstance &instance) {
	// Register Bloom filter functions
	LoadBloomFilter(instance);
}

void BitfiltersExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string BitfiltersExtension::Name() {
	return "bitfilters";
}

std::string BitfiltersExtension::Version() const {
	return "0.0.1";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void bitfilters_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::BitfiltersExtension>();
}

DUCKDB_EXTENSION_API const char *bitfilters_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}
