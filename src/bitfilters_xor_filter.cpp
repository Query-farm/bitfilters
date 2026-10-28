#include "bitfilters_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "bitfilters_aggregate_util.hpp"
#include "duckdb/common/vector/constant_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "vendor/fastfilter/xorfilter.h"

namespace duckdb {

namespace {

// Serialized layout written by xor{8,16}_serialize (native endianness):
//
//   uint64 seed | uint64 blockLength | fingerprint[3 * blockLength]
constexpr idx_t XOR_HEADER_BYTES = 2 * sizeof(uint64_t);

// xor{8,16}_contain truncates blockLength to 32 bits when computing indices, so
// a filter whose block length does not fit in 32 bits could never be probed
// correctly (and cannot be produced by xor{8,16}_allocate).
constexpr uint64_t XOR_MAX_BLOCK_LENGTH = NumericLimits<uint32_t>::Maximum();

// The xor construction spreads each key over three blocks.
constexpr uint64_t XOR_BLOCKS = 3;

// A deserialized filter points its fingerprint array straight into a BLOB,
// which carries no alignment guarantee, so 16-bit fingerprints have to be read
// byte-wise. Dereferencing them as uint16_t is undefined behaviour (UBSan flags
// it) and faults outright on strict-alignment platforms.
inline uint16_t LoadUnaligned16(const uint16_t *base, uint32_t index) {
	uint16_t value;
	memcpy(&value, reinterpret_cast<const char *>(base) + index * sizeof(uint16_t), sizeof(uint16_t));
	return value;
}

template <typename FilterType>
struct XorTraits;

template <>
struct XorTraits<xor8_t> {
	using FingerprintType = uint8_t;

	static const char *Name() {
		return "xor8";
	}
	static bool Allocate(uint32_t size, xor8_t *filter) {
		return xor8_allocate(size, filter);
	}
	static bool Populate(uint64_t *keys, uint32_t size, xor8_t *filter) {
		return xor8_buffered_populate(keys, size, filter);
	}
	static size_t SerializationBytes(xor8_t *filter) {
		return xor8_serialization_bytes(filter);
	}
	static void Serialize(const xor8_t *filter, char *buffer) {
		xor8_serialize(filter, buffer);
	}
	static void Free(xor8_t *filter) {
		xor8_free(filter);
	}
	static bool Contains(uint64_t item, const xor8_t *filter) {
		return xor8_contain(item, filter);
	}
	static void SetFingerprints(xor8_t *filter, const char *data) {
		filter->fingerprints = reinterpret_cast<uint8_t *>(const_cast<char *>(data));
	}
};

template <>
struct XorTraits<xor16_t> {
	using FingerprintType = uint16_t;

	static const char *Name() {
		return "xor16";
	}
	static bool Allocate(uint32_t size, xor16_t *filter) {
		return xor16_allocate(size, filter);
	}
	static bool Populate(uint64_t *keys, uint32_t size, xor16_t *filter) {
		return xor16_buffered_populate(keys, size, filter);
	}
	static size_t SerializationBytes(xor16_t *filter) {
		return xor16_serialization_bytes(filter);
	}
	static void Serialize(const xor16_t *filter, char *buffer) {
		xor16_serialize(filter, buffer);
	}
	static void Free(xor16_t *filter) {
		xor16_free(filter);
	}
	// xor16_contain, with unaligned-safe fingerprint loads.
	static bool Contains(uint64_t item, const xor16_t *filter) {
		uint64_t hash = xor_mix_split(item, filter->seed);
		uint16_t f = (uint16_t)xor_fingerprint(hash);
		uint32_t r0 = (uint32_t)hash;
		uint32_t r1 = (uint32_t)xor_rotl64(hash, 21);
		uint32_t r2 = (uint32_t)xor_rotl64(hash, 42);
		uint32_t block_length = (uint32_t)filter->blockLength;
		uint32_t h0 = xor_reduce(r0, block_length);
		uint32_t h1 = xor_reduce(r1, block_length) + block_length;
		uint32_t h2 = xor_reduce(r2, block_length) + 2 * block_length;
		return f == (uint16_t)(LoadUnaligned16(filter->fingerprints, h0) ^ LoadUnaligned16(filter->fingerprints, h1) ^
		                       LoadUnaligned16(filter->fingerprints, h2));
	}
	static void SetFingerprints(xor16_t *filter, const char *data) {
		filter->fingerprints = reinterpret_cast<uint16_t *>(const_cast<char *>(data));
	}
};

template <typename FilterType>
uint64_t XorExactSerializedSize(uint64_t block_length) {
	return XOR_HEADER_BYTES + XOR_BLOCKS * block_length * sizeof(typename XorTraits<FilterType>::FingerprintType);
}

template <typename FilterType>
InvalidInputException XorInvalidBlob(const string &detail) {
	return InvalidInputException("%s_filter_contains: the filter argument is not a valid %s filter BLOB (%s). Filter "
	                             "BLOBs must be produced by %s_filter().",
	                             XorTraits<FilterType>::Name(), XorTraits<FilterType>::Name(), detail,
	                             XorTraits<FilterType>::Name());
}

// Reads a serialized filter out of `blob` without copying: on success `filter`
// points its fingerprints array directly at the BLOB's bytes, so `filter` must
// not outlive `blob` and must never be passed to xor*_free.
//
// xor*_contain indexes the fingerprint array at h0 < blockLength,
// h1 < 2 * blockLength and h2 < 3 * blockLength, so requiring the BLOB to hold
// exactly 3 * blockLength fingerprints bounds every read. Without the check any
// BLOB (a truncated filter, a filter of the other width, a binary fuse filter,
// arbitrary user bytes) turns into wild reads far outside the BLOB.
template <typename FilterType>
void DeserializeFilter(FilterType &filter, const string_t &blob) {
	using Traits = XorTraits<FilterType>;

	const auto size = static_cast<uint64_t>(blob.GetSize());
	const auto *data = blob.GetData();

	if (size < XOR_HEADER_BYTES) {
		throw XorInvalidBlob<FilterType>(
		    StringUtil::Format("it is %llu bytes, which is smaller than the %llu byte header", size,
		                       static_cast<uint64_t>(XOR_HEADER_BYTES)));
	}

	uint64_t seed, block_length;
	memcpy(&seed, data, sizeof(seed));
	memcpy(&block_length, data + sizeof(seed), sizeof(block_length));

	if (block_length == 0 || block_length > XOR_MAX_BLOCK_LENGTH) {
		throw XorInvalidBlob<FilterType>(
		    StringUtil::Format("its block length (%llu) is not in [1, %llu]", block_length, XOR_MAX_BLOCK_LENGTH));
	}

	// Require an exact length match. Besides catching truncation, this is what
	// distinguishes an 8-bit filter from a 16-bit one: the headers are
	// identical, only the fingerprint width differs.
	const uint64_t exact_size = XorExactSerializedSize<FilterType>(block_length);
	if (size != exact_size) {
		throw XorInvalidBlob<FilterType>(StringUtil::Format(
		    "its header describes a filter of %llu bytes (%llu byte header plus %llu %u-bit fingerprints) but the BLOB "
		    "is %llu bytes",
		    exact_size, static_cast<uint64_t>(XOR_HEADER_BYTES), XOR_BLOCKS * block_length,
		    static_cast<uint32_t>(sizeof(typename Traits::FingerprintType) * 8), size));
	}

	filter.seed = seed;
	filter.blockLength = block_length;
	Traits::SetFingerprints(&filter, data + XOR_HEADER_BYTES);
}

// Frees the vendored filter's fingerprint array when Finalize leaves early via
// an exception.
template <typename FilterType>
struct XorFilterGuard {
	explicit XorFilterGuard(FilterType &filter_p) : filter(filter_p) {
	}
	~XorFilterGuard() {
		XorTraits<FilterType>::Free(&filter);
	}
	FilterType &filter;
};

template <typename FilterType>
struct XorFilterCreateOperation : BitfiltersKeyCollectOperation {
	using Traits = XorTraits<FilterType>;

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		auto &keys = state.keys;
		if (keys.total_capacity == 0) {
			finalize_data.ReturnNull();
			return;
		}
		if (keys.total_capacity > NumericLimits<uint32_t>::Maximum()) {
			throw OutOfRangeException("%s_filter: cannot build a filter over %llu values, the maximum is %u",
			                          Traits::Name(), static_cast<uint64_t>(keys.total_capacity),
			                          NumericLimits<uint32_t>::Maximum());
		}
		const auto count = static_cast<uint32_t>(keys.total_capacity);

		// The construction's scratch space and the filter itself are charged
		// to memory_limit like the collected keys.
		BitfiltersFastFilterAllocationScope allocation_scope(BitfiltersStateAllocator(finalize_data.input));
		// The construction may reorder and de-duplicate the keys in place, so
		// it works on a copy and the state stays intact.
		auto key_array = GatherKeys(keys, allocation_scope.GetAllocator());

		FilterType filter;
		if (!Traits::Allocate(count, &filter)) {
			allocation_scope.ThrowIfFailed();
			throw OutOfMemoryException("%s_filter: failed to allocate a filter for %u values", Traits::Name(), count);
		}
		XorFilterGuard<FilterType> guard(filter);

		if (!Traits::Populate(reinterpret_cast<uint64_t *>(key_array.get()), count, &filter)) {
			allocation_scope.ThrowIfFailed();
			throw InvalidInputException(
			    "%s_filter: failed to construct a filter over %u values. This is usually caused by running out of "
			    "memory, or by an input distribution the construction algorithm could not resolve.",
			    Traits::Name(), count);
		}

		const auto exact_size = XorExactSerializedSize<FilterType>(filter.blockLength);
		if (exact_size > NumericLimits<uint32_t>::Maximum()) {
			throw OutOfRangeException("%s_filter: the resulting filter would be %llu bytes, which exceeds the maximum "
			                          "BLOB size of %u bytes",
			                          Traits::Name(), exact_size, NumericLimits<uint32_t>::Maximum());
		}
		// Serialize straight into the result rather than through a scratch
		// copy of the whole filter.
		D_ASSERT(Traits::SerializationBytes(&filter) == exact_size);
		target = StringVector::EmptyString(finalize_data.result, exact_size);
		Traits::Serialize(&filter, target.GetDataWriteable());
		target.Finalize();
	}
};

template <typename T, typename FilterType>
auto static XorFilterCreateAggregate(const LogicalType &type, const LogicalType &result_type) -> AggregateFunction {
	auto aggregate = BitfiltersKeyAggregate<XorFilterCreateOperation<FilterType>>(type, result_type);
	aggregate.SetFallible();
	return aggregate;
}

template <class T, typename FilterType>
static inline void XorFilterContains(DataChunk &args, ExpressionState &state, Vector &result) {
	using Traits = XorTraits<FilterType>;

	D_ASSERT(args.ColumnCount() == 2);

	auto &filter_vector = args.data[0];
	auto &item_vector = args.data[1];

	if (filter_vector.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		if (ConstantVector::IsNull(filter_vector)) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(result, true);
			return;
		}
		FilterType deserialized_filter;
		DeserializeFilter(deserialized_filter, ConstantVector::GetData<string_t>(filter_vector)[0]);

		UnaryExecutor::Execute<T, bool>(item_vector, result,
		                                [&](T item_data) { return Traits::Contains(item_data, &deserialized_filter); });
		return;
	}

	// Per-row filters. The same filter is usually repeated across many rows (a
	// join against a table of filters), so remember the last blob we validated.
	// Only blobs that passed validation are cached, and those are always longer
	// than the 12 byte inline threshold, so their data pointers are stable heap
	// addresses rather than pointers into a by-value string_t on the stack.
	FilterType deserialized_filter;
	const char *cached_data = nullptr;
	uint32_t cached_size = 0;
	BinaryExecutor::Execute<string_t, T, bool>(
	    filter_vector, item_vector, result, [&](string_t filter_data, T item_data) {
		    if (filter_data.GetData() != cached_data || filter_data.GetSize() != cached_size) {
			    DeserializeFilter(deserialized_filter, filter_data);
			    cached_data = filter_data.GetData();
			    cached_size = filter_data.GetSize();
		    }
		    return Traits::Contains(item_data, &deserialized_filter);
	    });
}

// Helper function to register Xor filter functions for a specific type
template <typename T, typename FilterType>
static void RegisterXorFilterFunctionsForType(AggregateFunctionSet &Xorfilter, const LogicalType &type) {
	// Register create aggregate function
	Xorfilter.AddFunction(XorFilterCreateAggregate<T, FilterType>(type, LogicalType::BLOB));
}

// Helper function to register scalar contains functions for a specific type
template <typename T, typename FilterType>
static void RegisterXorFilterContainsForType(ScalarFunctionSet &fs, const LogicalType &type) {
	fs.AddFunction(ScalarFunction({LogicalType::BLOB, type}, LogicalType::BOOLEAN, XorFilterContains<T, FilterType>));
}
} // namespace

void LoadXorFilter(ExtensionLoader &loader) {
	// Register aggregate functions
	{
		AggregateFunctionSet Xorfilter("xor16_filter");
		RegisterXorFilterFunctionsForType<uint64_t, xor16_t>(Xorfilter, LogicalType::UBIGINT);
		CreateAggregateFunctionInfo Xorfilter_create_info(Xorfilter);
		{
			FunctionDescription desc;
			desc.description = "Creates an Xor16 filter (16-bit fingerprints, ~0.0015% false positive rate) "
			                   "by aggregating UBIGINT values. Returns a BLOB to probe with xor16_filter_contains.";
			desc.parameter_names = {"value"};
			desc.parameter_types = {LogicalType::UBIGINT};
			desc.examples.push_back("SELECT xor16_filter(hash(column)) FROM table");
			desc.categories = {"bitfilters", "probabilistic"};
			Xorfilter_create_info.descriptions.push_back(desc);
		}
		BitfiltersApplyParameterNames(Xorfilter_create_info.functions, Xorfilter_create_info.descriptions);
		loader.RegisterFunction(Xorfilter_create_info);
	}

	{
		AggregateFunctionSet Xorfilter("xor8_filter");
		RegisterXorFilterFunctionsForType<uint64_t, xor8_t>(Xorfilter, LogicalType::UBIGINT);
		CreateAggregateFunctionInfo Xorfilter_create_info(Xorfilter);
		{
			FunctionDescription desc;
			desc.description = "Creates an Xor8 filter (8-bit fingerprints, ~0.4% false positive rate) "
			                   "by aggregating UBIGINT values. Returns a BLOB to probe with xor8_filter_contains.";
			desc.parameter_names = {"value"};
			desc.parameter_types = {LogicalType::UBIGINT};
			desc.examples.push_back("SELECT xor8_filter(hash(column)) FROM table");
			desc.categories = {"bitfilters", "probabilistic"};
			Xorfilter_create_info.descriptions.push_back(desc);
		}
		BitfiltersApplyParameterNames(Xorfilter_create_info.functions, Xorfilter_create_info.descriptions);
		loader.RegisterFunction(Xorfilter_create_info);
	}

	// Register scalar functions
	{
		ScalarFunctionSet fs("xor16_filter_contains");
		RegisterXorFilterContainsForType<uint64_t, xor16_t>(fs, LogicalType::UBIGINT);
		fs.SetFallible();
		CreateScalarFunctionInfo info(std::move(fs));
		{
			FunctionDescription desc;
			desc.description = "Tests if an Xor16 filter may contain a value. Returns true if the value "
			                   "might be in the set (with possible false positives), or false if the value "
			                   "is definitely not in the set (no false negatives).";
			desc.parameter_names = {"filter", "value"};
			desc.parameter_types = {LogicalType::BLOB, LogicalType::UBIGINT};
			desc.examples.push_back("SELECT xor16_filter_contains(filter, 42) FROM table");
			desc.categories = {"bitfilters", "probabilistic"};
			info.descriptions.push_back(desc);
		}
		BitfiltersApplyParameterNames(info.functions, info.descriptions);
		loader.RegisterFunction(info);
	}

	{
		ScalarFunctionSet fs("xor8_filter_contains");
		RegisterXorFilterContainsForType<uint64_t, xor8_t>(fs, LogicalType::UBIGINT);
		fs.SetFallible();
		CreateScalarFunctionInfo info(std::move(fs));
		{
			FunctionDescription desc;
			desc.description = "Tests if an Xor8 filter may contain a value. Returns true if the value "
			                   "might be in the set (with possible false positives), or false if the value "
			                   "is definitely not in the set (no false negatives).";
			desc.parameter_names = {"filter", "value"};
			desc.parameter_types = {LogicalType::BLOB, LogicalType::UBIGINT};
			desc.examples.push_back("SELECT xor8_filter_contains(filter, 42) FROM table");
			desc.categories = {"bitfilters", "probabilistic"};
			info.descriptions.push_back(desc);
		}
		BitfiltersApplyParameterNames(info.functions, info.descriptions);
		loader.RegisterFunction(info);
	}
}

} // namespace duckdb
