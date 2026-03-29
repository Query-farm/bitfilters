#include "bitfilters_duckdb_bloom_filter.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/execution/expression_executor.hpp"

#include <cmath>
#include <cstring>
#include <limits>

namespace duckdb {

namespace {

// ============================================================================
// Version registry
// ============================================================================

enum class DuckDbHashVersion {
	// v1.4.x and v1.5.x all use the same MurmurHash64 with constant 0xd6e8feb86659fd93.
	// The only difference is v1.5.x added explicit little-endian loads (LoadLE/BSwapIfBE)
	// which only affects big-endian platforms. On x86/ARM64 (little-endian), all versions
	// produce identical hash values.
	V1_4_0, // v1.4.0
	V1_4_1, // v1.4.1
	V1_4_2, // v1.4.2
	V1_4_3, // v1.4.3
	V1_4_4, // v1.4.4
	V1_5_0, // v1.5.0
	V1_5_1, // v1.5.1
	V1_6_0, // v1.6.0
};

static DuckDbHashVersion ParseVersion(const string &version_str) {
	if (version_str == "v1.4.0" || version_str == "1.4.0") {
		return DuckDbHashVersion::V1_4_0;
	}
	if (version_str == "v1.4.1" || version_str == "1.4.1") {
		return DuckDbHashVersion::V1_4_1;
	}
	if (version_str == "v1.4.2" || version_str == "1.4.2") {
		return DuckDbHashVersion::V1_4_2;
	}
	if (version_str == "v1.4.3" || version_str == "1.4.3") {
		return DuckDbHashVersion::V1_4_3;
	}
	if (version_str == "v1.4.4" || version_str == "1.4.4") {
		return DuckDbHashVersion::V1_4_4;
	}
	if (version_str == "v1.5.0" || version_str == "1.5.0") {
		return DuckDbHashVersion::V1_5_0;
	}
	if (version_str == "v1.5.1" || version_str == "1.5.1") {
		return DuckDbHashVersion::V1_5_1;
	}
	if (version_str == "v1.6.0" || version_str == "1.6.0") {
		return DuckDbHashVersion::V1_6_0;
	}
	throw BinderException("Unsupported DuckDB version '%s' for bitfilters DuckDB hash/bloom filter. "
	                       "Supported versions: v1.4.0-v1.4.4, v1.5.0, v1.5.1, v1.6.0",
	                       version_str);
}

// ============================================================================
// DuckDB v1.5.x hash algorithm reimplementation
// ============================================================================
// These are standalone reimplementations of DuckDB's Hash() functions.
// They do NOT call DuckDB's internal Hash() — they replicate the algorithm
// so that the bitfilters extension can produce matching hashes for a specific
// DuckDB version even if the extension is loaded in a different DuckDB version.

static constexpr uint64_t MURMURHASH_CONSTANT = 0xd6e8feb86659fd93ULL;

static inline uint64_t MurmurHash64_v1_5(uint64_t x) {
	x ^= x >> 32;
	x *= MURMURHASH_CONSTANT;
	x ^= x >> 32;
	x *= MURMURHASH_CONSTANT;
	x ^= x >> 32;
	return x;
}

static inline uint64_t HashInt_v1_5(int8_t val) {
	// Matches DuckDB's Hash<T> template: static_cast<uint32_t>(value)
	// For negative values, this sign-extends through int→uint32: -1 → 0xFFFFFFFF
	return MurmurHash64_v1_5(static_cast<uint32_t>(val));
}
static inline uint64_t HashInt_v1_5(int16_t val) {
	return MurmurHash64_v1_5(static_cast<uint32_t>(val));
}
static inline uint64_t HashInt_v1_5(int32_t val) {
	return MurmurHash64_v1_5(static_cast<uint32_t>(val));
}
static inline uint64_t HashInt_v1_5(int64_t val) {
	return MurmurHash64_v1_5(static_cast<uint64_t>(val));
}
static inline uint64_t HashInt_v1_5(uint8_t val) {
	return MurmurHash64_v1_5(static_cast<uint32_t>(val));
}
static inline uint64_t HashInt_v1_5(uint16_t val) {
	return MurmurHash64_v1_5(static_cast<uint32_t>(val));
}
static inline uint64_t HashInt_v1_5(uint32_t val) {
	return MurmurHash64_v1_5(static_cast<uint64_t>(val));
}
static inline uint64_t HashInt_v1_5(uint64_t val) {
	return MurmurHash64_v1_5(val);
}

static inline uint64_t HashFloat_v1_5(float val) {
	if (val == 0.0f) {
		val = 0.0f; // normalize -0 to +0
	} else if (std::isnan(val)) {
		val = std::numeric_limits<float>::quiet_NaN();
	}
	uint32_t uval;
	memcpy(&uval, &val, sizeof(uval));
	return MurmurHash64_v1_5(uval);
}

static inline uint64_t HashDouble_v1_5(double val) {
	if (val == 0.0) {
		val = 0.0; // normalize -0 to +0
	} else if (std::isnan(val)) {
		val = std::numeric_limits<double>::quiet_NaN();
	}
	uint64_t uval;
	memcpy(&uval, &val, sizeof(uval));
	return MurmurHash64_v1_5(uval);
}

static inline uint64_t LoadLE64(const uint8_t *ptr) {
	uint64_t val;
	memcpy(&val, ptr, sizeof(val));
	// Assume little-endian (x86/ARM64). On big-endian, would need byte swap.
	return val;
}

static uint64_t HashBytes_v1_5(const char *data, idx_t len) {
	uint64_t h = 0xe17a1465ULL ^ (static_cast<uint64_t>(len) * 0xc6a4a7935bd1e995ULL);

	auto ptr = reinterpret_cast<const uint8_t *>(data);
	auto remainder = len & 7U;
	auto end = ptr + len - remainder;

	while (ptr < end) {
		h ^= LoadLE64(ptr);
		h *= MURMURHASH_CONSTANT;
		ptr += 8;
	}

	if (remainder != 0) {
		uint64_t hr = 0;
		memcpy(&hr, ptr, remainder);
		// little-endian: no byte swap needed
		h ^= hr;
		h *= MURMURHASH_CONSTANT;
	}

	return MurmurHash64_v1_5(h);
}

static inline uint64_t HashString_v1_5(string_t val) {
	return HashBytes_v1_5(val.GetData(), val.GetSize());
}

static inline uint64_t CombineHash_v1_5(uint64_t a, uint64_t b) {
	a ^= a >> 32;
	a *= MURMURHASH_CONSTANT;
	return a ^ b;
}

// Hash a unified vector into a result array using a typed hash function.
// Handles all vector types (flat, constant, dictionary) via UnifiedVectorFormat.
template <class T, uint64_t (*HASH_FN)(T)>
static void HashVectorTyped(UnifiedVectorFormat &vdata, uint64_t *result, idx_t count) {
	auto data = UnifiedVectorFormat::GetData<T>(vdata);
	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		result[i] = vdata.validity.RowIsValid(idx) ? HASH_FN(data[idx]) : 0;
	}
}

// Hash an entire vector into a flat array of hash values.
// Uses UnifiedVectorFormat to handle flat, constant, and dictionary vectors.
static void HashVector_v1_5(Vector &vec, uint64_t *result, idx_t count) {
	UnifiedVectorFormat vdata;
	vec.ToUnifiedFormat(count, vdata);

	switch (vec.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		HashVectorTyped<int8_t, HashInt_v1_5>(vdata, result, count);
		break;
	case PhysicalType::INT16:
		HashVectorTyped<int16_t, HashInt_v1_5>(vdata, result, count);
		break;
	case PhysicalType::INT32:
		HashVectorTyped<int32_t, HashInt_v1_5>(vdata, result, count);
		break;
	case PhysicalType::INT64:
		HashVectorTyped<int64_t, HashInt_v1_5>(vdata, result, count);
		break;
	case PhysicalType::UINT8:
		HashVectorTyped<uint8_t, HashInt_v1_5>(vdata, result, count);
		break;
	case PhysicalType::UINT16:
		HashVectorTyped<uint16_t, HashInt_v1_5>(vdata, result, count);
		break;
	case PhysicalType::UINT32:
		HashVectorTyped<uint32_t, HashInt_v1_5>(vdata, result, count);
		break;
	case PhysicalType::UINT64:
		HashVectorTyped<uint64_t, HashInt_v1_5>(vdata, result, count);
		break;
	case PhysicalType::FLOAT:
		HashVectorTyped<float, HashFloat_v1_5>(vdata, result, count);
		break;
	case PhysicalType::DOUBLE:
		HashVectorTyped<double, HashDouble_v1_5>(vdata, result, count);
		break;
	case PhysicalType::VARCHAR:
		HashVectorTyped<string_t, HashString_v1_5>(vdata, result, count);
		break;
	default:
		throw InvalidInputException("bitfilters_duckdb_hash: unsupported type %s", vec.GetType().ToString());
	}
}

// Combine hashes from a second vector into existing hash values.
static void CombineHashVector_v1_5(Vector &vec, uint64_t *hashes, idx_t count) {
	// Hash this vector into a temp buffer, then combine
	auto temp = unique_ptr<uint64_t[]>(new uint64_t[count]);
	HashVector_v1_5(vec, temp.get(), count);

	for (idx_t i = 0; i < count; i++) {
		hashes[i] = CombineHash_v1_5(hashes[i], temp[i]);
	}
}

// ============================================================================
// Bloom filter probe internals (v1.5.x)
// ============================================================================

static inline uint64_t GetMask_v1_5(uint64_t hash) {
	uint64_t mask = 0;
	// Extract bytes 4-7 of the hash, each mod 64 gives a bit position
	for (int byte_idx = 4; byte_idx < 8; byte_idx++) {
		uint8_t bit_pos = (hash >> (byte_idx * 8)) & 0x3F;
		mask |= (1ULL << bit_pos);
	}
	return mask;
}

// ============================================================================
// bitfilters_duckdb_hash — scalar function
// ============================================================================

struct DuckDbHashBindData : public FunctionData {
	DuckDbHashVersion version;

	explicit DuckDbHashBindData(DuckDbHashVersion version_p) : version(version_p) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<DuckDbHashBindData>(version);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<DuckDbHashBindData>();
		return version == other.version;
	}
};

static unique_ptr<FunctionData> DuckDbHashBind(ClientContext &context, ScalarFunction &function,
                                                vector<unique_ptr<Expression>> &arguments) {
	if (arguments.empty()) {
		throw BinderException("bitfilters_duckdb_hash requires at least 2 arguments: version and value(s)");
	}
	if (!arguments[0]->IsFoldable()) {
		throw BinderException("bitfilters_duckdb_hash: version must be a constant string");
	}
	Value version_val = ExpressionExecutor::EvaluateScalar(context, *arguments[0]);
	if (version_val.IsNull()) {
		throw BinderException("bitfilters_duckdb_hash: version cannot be NULL");
	}
	auto version = ParseVersion(version_val.ToString());
	Function::EraseArgument(function, arguments, 0);

	if (arguments.empty()) {
		throw BinderException("bitfilters_duckdb_hash requires at least one value argument after version");
	}

	return make_uniq<DuckDbHashBindData>(version);
}

static void DuckDbHashFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &bind_data = func_expr.bind_info->Cast<DuckDbHashBindData>();
	(void)bind_data; // version checked at bind time; all v1.5.x use same impl

	auto count = args.size();
	auto result_data = FlatVector::GetData<uint64_t>(result);

	// Hash first column directly into result
	HashVector_v1_5(args.data[0], result_data, count);

	// Combine with remaining columns
	for (idx_t col_idx = 1; col_idx < args.ColumnCount(); col_idx++) {
		CombineHashVector_v1_5(args.data[col_idx], result_data, count);
	}

	result.SetVectorType(VectorType::FLAT_VECTOR);
}

// ============================================================================
// bitfilters_duckdb_bloom_filter_probe — scalar function
// ============================================================================

struct DuckDbBloomFilterProbeBindData : public FunctionData {
	DuckDbHashVersion version;
	bool filter_is_constant;
	// Constant filter fields (populated when filter is constant-folded)
	uint64_t num_sectors = 0;
	uint64_t bitmask = 0;
	string blob_copy; // keeps the data alive
	const uint64_t *sectors = nullptr;

	DuckDbBloomFilterProbeBindData(DuckDbHashVersion version_p, string blob_p, bool is_constant)
	    : version(version_p), filter_is_constant(is_constant), blob_copy(std::move(blob_p)) {
		if (filter_is_constant) {
			ParseBlob();
		}
	}

	void ParseBlob() {
		if (blob_copy.size() < sizeof(uint64_t)) {
			throw InvalidInputException("bitfilters_duckdb_bloom_filter_probe: filter blob too short");
		}
		memcpy(&num_sectors, blob_copy.data(), sizeof(uint64_t));
		if (num_sectors == 0 || (num_sectors & (num_sectors - 1)) != 0) {
			throw InvalidInputException(
			    "bitfilters_duckdb_bloom_filter_probe: num_sectors must be a power of 2, got %llu", num_sectors);
		}
		// Guard against overflow in size calculation: DuckDB's MAX_NUM_SECTORS is 1<<26.
		// Allow up to 1<<40 (~8TB filter) to be generous, but prevent overflow.
		if (num_sectors > (1ULL << 40)) {
			throw InvalidInputException(
			    "bitfilters_duckdb_bloom_filter_probe: num_sectors too large (%llu), max is %llu",
			    num_sectors, (1ULL << 40));
		}
		auto expected_size = (num_sectors + 1) * sizeof(uint64_t);
		if (blob_copy.size() < expected_size) {
			throw InvalidInputException(
			    "bitfilters_duckdb_bloom_filter_probe: filter blob too short for %llu sectors", num_sectors);
		}
		bitmask = num_sectors - 1;
		sectors = reinterpret_cast<const uint64_t *>(blob_copy.data() + sizeof(uint64_t));
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<DuckDbBloomFilterProbeBindData>(version, blob_copy, filter_is_constant);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<DuckDbBloomFilterProbeBindData>();
		return version == other.version && blob_copy == other.blob_copy &&
		       filter_is_constant == other.filter_is_constant;
	}
};

static unique_ptr<FunctionData> DuckDbBloomFilterProbeBind(ClientContext &context, ScalarFunction &function,
                                                            vector<unique_ptr<Expression>> &arguments) {
	if (arguments.size() < 3) {
		throw BinderException("bitfilters_duckdb_bloom_filter_probe requires at least 3 arguments: "
		                       "version, filter_blob, value(s)");
	}
	// Validate and extract version (constant)
	if (!arguments[0]->IsFoldable()) {
		throw BinderException("bitfilters_duckdb_bloom_filter_probe: version must be a constant string");
	}
	Value version_val = ExpressionExecutor::EvaluateScalar(context, *arguments[0]);
	if (version_val.IsNull()) {
		throw BinderException("bitfilters_duckdb_bloom_filter_probe: version cannot be NULL");
	}
	auto version = ParseVersion(version_val.ToString());

	// Try to constant-fold the filter blob at bind time (optional optimization)
	string blob_str;
	bool filter_is_constant = false;
	if (arguments[1]->IsFoldable()) {
		Value filter_val = ExpressionExecutor::EvaluateScalar(context, *arguments[1]);
		if (filter_val.IsNull()) {
			throw BinderException("bitfilters_duckdb_bloom_filter_probe: filter blob cannot be NULL");
		}
		blob_str = StringValue::Get(filter_val);
		filter_is_constant = true;
	}

	// Erase version argument (always constant)
	Function::EraseArgument(function, arguments, 0); // version
	// Erase filter argument only if it was constant-folded
	if (filter_is_constant) {
		Function::EraseArgument(function, arguments, 0); // filter
	}

	if (arguments.empty()) {
		throw BinderException("bitfilters_duckdb_bloom_filter_probe requires at least one value argument");
	}

	return make_uniq<DuckDbBloomFilterProbeBindData>(version, blob_str, filter_is_constant);
}

static void DuckDbBloomFilterProbeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &bind_data = func_expr.bind_info->Cast<DuckDbBloomFilterProbeBindData>();

	auto count = args.size();
	auto result_data = FlatVector::GetData<bool>(result);

	// Determine which column index the value arguments start at
	idx_t value_start = bind_data.filter_is_constant ? 0 : 1;

	// For non-constant filter, parse from first column
	const uint64_t *sectors;
	uint64_t bitmask;
	// Holds parsed blob data for the non-constant case (must outlive the probe loop)
	unique_ptr<DuckDbBloomFilterProbeBindData> runtime_bd;
	if (!bind_data.filter_is_constant) {
		// Get filter from first column (must be constant vector or we parse per-row)
		auto &filter_vec = args.data[0];
		if (filter_vec.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			auto filter_str = ConstantVector::GetData<string_t>(filter_vec)[0];
			runtime_bd = make_uniq<DuckDbBloomFilterProbeBindData>(bind_data.version, filter_str.GetString(), true);
			sectors = runtime_bd->sectors;
			bitmask = runtime_bd->bitmask;
		} else {
			throw InvalidInputException("bitfilters_duckdb_bloom_filter_probe: filter blob must be constant across rows");
		}
	} else {
		sectors = bind_data.sectors;
		bitmask = bind_data.bitmask;
	}

	// Hash all value columns into a temp buffer
	auto hashes = unique_ptr<uint64_t[]>(new uint64_t[count]);
	HashVector_v1_5(args.data[value_start], hashes.get(), count);
	for (idx_t col_idx = value_start + 1; col_idx < args.ColumnCount(); col_idx++) {
		CombineHashVector_v1_5(args.data[col_idx], hashes.get(), count);
	}

	// Probe bloom filter for each hash
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		auto h = hashes[row_idx];
		auto sector_idx = h & bitmask;
		auto mask = GetMask_v1_5(h);
		result_data[row_idx] = (sectors[sector_idx] & mask) == mask;
	}

	result.SetVectorType(VectorType::FLAT_VECTOR);
}

// ============================================================================
// bitfilters_duckdb_bloom_filter_create — aggregate function
// ============================================================================

struct DuckDbBloomFilterCreateBindData : public FunctionData {
	DuckDbHashVersion version;
	uint64_t num_sectors;

	DuckDbBloomFilterCreateBindData(DuckDbHashVersion version_p, uint64_t num_sectors_p)
	    : version(version_p), num_sectors(num_sectors_p) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<DuckDbBloomFilterCreateBindData>(version, num_sectors);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<DuckDbBloomFilterCreateBindData>();
		return version == other.version && num_sectors == other.num_sectors;
	}
};

struct DuckDbBloomFilterCreateState {
	unique_ptr<uint64_t[]> sectors;
	uint64_t num_sectors = 0;

	void Initialize(uint64_t n) {
		num_sectors = n;
		sectors = unique_ptr<uint64_t[]>(new uint64_t[n]());
	}

	void Insert(uint64_t hash) {
		auto sector_idx = hash & (num_sectors - 1);
		auto mask = GetMask_v1_5(hash);
		sectors[sector_idx] |= mask;
	}

	string Serialize() const {
		string result;
		auto total_bytes = (num_sectors + 1) * sizeof(uint64_t);
		result.resize(total_bytes);
		memcpy(result.data(), &num_sectors, sizeof(uint64_t));
		memcpy(result.data() + sizeof(uint64_t), sectors.get(), num_sectors * sizeof(uint64_t));
		return result;
	}

	void Merge(const DuckDbBloomFilterCreateState &other) {
		if (!sectors) {
			num_sectors = other.num_sectors;
			sectors = unique_ptr<uint64_t[]>(new uint64_t[num_sectors]);
			memcpy(sectors.get(), other.sectors.get(), num_sectors * sizeof(uint64_t));
		} else {
			D_ASSERT(num_sectors == other.num_sectors);
			for (uint64_t i = 0; i < num_sectors; i++) {
				sectors[i] |= other.sectors[i];
			}
		}
	}
};

struct DuckDbBloomFilterCreateOperation {
	template <class STATE>
	static void Initialize(STATE &state) {
		state.sectors = nullptr;
		state.num_sectors = 0;
	}

	template <class STATE>
	static void Destroy(STATE &state, AggregateInputData &) {
		state.sectors = nullptr;
	}

	static bool IgnoreNull() {
		return true;
	}

	template <class A_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const A_TYPE &input, AggregateUnaryInput &agg_input) {
		if (!state.sectors) {
			auto &bind_data = agg_input.input.bind_data->Cast<DuckDbBloomFilterCreateBindData>();
			state.Initialize(bind_data.num_sectors);
		}
		// Input is a pre-hashed UBIGINT value from bitfilters_duckdb_hash()
		state.Insert(static_cast<uint64_t>(input));
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &agg_input, idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			Operation<INPUT_TYPE, STATE, OP>(state, input, agg_input);
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		if (!source.sectors) {
			return;
		}
		target.Merge(source);
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (!state.sectors) {
			finalize_data.ReturnNull();
		} else {
			target = StringVector::AddStringOrBlob(finalize_data.result, state.Serialize());
		}
	}
};

static unique_ptr<FunctionData> DuckDbBloomFilterCreateBind(ClientContext &context, AggregateFunction &function,
                                                             vector<unique_ptr<Expression>> &arguments) {
	if (arguments.size() < 3) {
		throw BinderException("bitfilters_duckdb_bloom_filter_create requires at least 3 arguments: "
		                       "version, num_sectors, value(s)");
	}

	// Version (constant string)
	if (!arguments[0]->IsFoldable()) {
		throw BinderException("bitfilters_duckdb_bloom_filter_create: version must be a constant string");
	}
	Value version_val = ExpressionExecutor::EvaluateScalar(context, *arguments[0]);
	if (version_val.IsNull()) {
		throw BinderException("bitfilters_duckdb_bloom_filter_create: version cannot be NULL");
	}
	auto version = ParseVersion(version_val.ToString());

	// num_sectors (constant integer, power of 2)
	if (!arguments[1]->IsFoldable()) {
		throw BinderException("bitfilters_duckdb_bloom_filter_create: num_sectors must be a constant integer");
	}
	Value sectors_val = ExpressionExecutor::EvaluateScalar(context, *arguments[1]);
	if (sectors_val.IsNull()) {
		throw BinderException("bitfilters_duckdb_bloom_filter_create: num_sectors cannot be NULL");
	}
	auto num_sectors = sectors_val.GetValue<uint64_t>();
	if (num_sectors == 0 || (num_sectors & (num_sectors - 1)) != 0) {
		throw BinderException("bitfilters_duckdb_bloom_filter_create: num_sectors must be a power of 2, got %llu",
		                       num_sectors);
	}

	Function::EraseArgument(function, arguments, 0); // version
	Function::EraseArgument(function, arguments, 0); // num_sectors

	return make_uniq<DuckDbBloomFilterCreateBindData>(version, num_sectors);
}

template <typename T>
static AggregateFunction CreateBloomFilterAggregate(const LogicalType &type) {
	return AggregateFunction::UnaryAggregateDestructor<DuckDbBloomFilterCreateState, T, string_t,
	                                                   DuckDbBloomFilterCreateOperation,
	                                                   AggregateDestructorType::LEGACY>(type, LogicalType::BLOB);
}

} // namespace

// ============================================================================
// Registration
// ============================================================================

void LoadDuckDbBloomFilter(ExtensionLoader &loader) {
	// Register bitfilters_duckdb_hash scalar function
	{
		ScalarFunctionSet fs("bitfilters_duckdb_hash");
		// Single argument with version (version is erased at bind time, so we register with VARCHAR + varargs)
		auto func = ScalarFunction({LogicalType::VARCHAR}, LogicalType::UBIGINT, DuckDbHashFunction, DuckDbHashBind);
		func.varargs = LogicalType::ANY;
		fs.AddFunction(func);

		CreateScalarFunctionInfo info(std::move(fs));
		FunctionDescription desc;
		desc.description = "Computes the DuckDB-internal hash value for the given DuckDB version. "
		                   "The hash matches DuckDB's built-in hash() function for that version. "
		                   "Multiple values are combined using the version's CombineHash algorithm.";
		desc.examples.push_back("SELECT bitfilters_duckdb_hash('v1.5.1', 42)");
		desc.examples.push_back("SELECT bitfilters_duckdb_hash('v1.5.1', col1, col2) FROM tbl");
		info.descriptions.push_back(desc);
		loader.RegisterFunction(info);
	}

	// Register bitfilters_duckdb_bloom_filter_probe scalar function
	{
		ScalarFunctionSet fs("bitfilters_duckdb_bloom_filter_probe");
		auto func = ScalarFunction({LogicalType::VARCHAR, LogicalType::BLOB}, LogicalType::BOOLEAN,
		                            DuckDbBloomFilterProbeFunction, DuckDbBloomFilterProbeBind);
		func.varargs = LogicalType::ANY;
		fs.AddFunction(func);

		CreateScalarFunctionInfo info(std::move(fs));
		FunctionDescription desc;
		desc.description = "Probes a DuckDB-internal bloom filter (serialized as BLOB) for a value. "
		                   "The version parameter specifies which DuckDB hash algorithm to use. "
		                   "The filter blob and version are constant-folded at bind time. "
		                   "Multiple value arguments are combined for multi-key bloom filter probes.";
		desc.examples.push_back(
		    "SELECT bitfilters_duckdb_bloom_filter_probe('v1.5.1', filter_blob, key_col) FROM tbl");
		desc.examples.push_back(
		    "SELECT bitfilters_duckdb_bloom_filter_probe('v1.5.1', filter_blob, col1, col2) FROM tbl");
		info.descriptions.push_back(desc);
		loader.RegisterFunction(info);
	}

	// Register bitfilters_duckdb_bloom_filter_create aggregate function
	// Accepts pre-hashed UBIGINT values from bitfilters_duckdb_hash()
	{
		AggregateFunctionSet agg_func("bitfilters_duckdb_bloom_filter_create");

		auto func = CreateBloomFilterAggregate<uint64_t>(LogicalType::UBIGINT);
		func.bind = DuckDbBloomFilterCreateBind;
		func.arguments.insert(func.arguments.begin(), LogicalType::INTEGER);    // num_sectors
		func.arguments.insert(func.arguments.begin(), LogicalType::VARCHAR);    // version
		agg_func.AddFunction(func);

		CreateAggregateFunctionInfo info(agg_func);
		FunctionDescription desc;
		desc.description = "Creates a DuckDB-compatible bloom filter by aggregating pre-hashed UBIGINT values. "
		                   "Use bitfilters_duckdb_hash() to hash values first. "
		                   "The version and num_sectors (power of 2) are constants. "
		                   "Returns a BLOB that can be probed with bitfilters_duckdb_bloom_filter_probe.";
		desc.examples.push_back(
		    "SELECT bitfilters_duckdb_bloom_filter_create('v1.5.1', 16384, bitfilters_duckdb_hash('v1.5.1', key)) FROM tbl");
		info.descriptions.push_back(desc);
		loader.RegisterFunction(info);
	}
}

} // namespace duckdb
