#include "bitfilters_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
// #include "bitfilters_bloom_filter.hpp"
#include "bitfilters_quotient_filter.hpp"
#include "bitfilters_xor_filter.hpp"
#include "bitfilters_binary_fuse_filter.hpp"
#include "query_farm_telemetry.hpp"
namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	// Register Bloom filter functions
	//	LoadBloomFilter(instance);
	LoadQuotientFilter(loader);
	LoadXorFilter(loader);
	LoadBinaryFuseFilter(loader);

	QueryFarmSendTelemetry(loader, "bitfilters", BitfiltersExtension().Version());
}

void BitfiltersExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string BitfiltersExtension::Name() {
	return "bitfilters";
}

std::string BitfiltersExtension::Version() const {
	return "2025091501";
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(bitfilters, loader) {
	duckdb::LoadInternal(loader);
}
}
