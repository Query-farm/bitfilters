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
#include "vendor/fastfilter/binaryfusefilter.h"

#include <cmath>

namespace duckdb {

namespace {

// Serialized layout written by binary_fuse{8,16}_serialize (native endianness):
//
//   uint64 Seed | uint32 Size | uint32 SegmentLength | uint32 SegmentCount |
//   uint32 SegmentCountLength | uint32 ArrayLength | fingerprint[ArrayLength]
//
// SegmentLengthMask is *not* serialized; it is recomputed on read.
constexpr idx_t BINARY_FUSE_HEADER_BYTES = sizeof(uint64_t) + 5 * sizeof(uint32_t);

// binary_fuse{8,16}_allocate caps the segment length at this value.
constexpr uint32_t BINARY_FUSE_MAX_SEGMENT_LENGTH = 262144;

// The arity of the fuse graph, hardcoded to 3 in the vendored implementation.
constexpr uint64_t BINARY_FUSE_ARITY = 3;

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
struct BinaryFuseTraits;

template <>
struct BinaryFuseTraits<binary_fuse8_t> {
	using FingerprintType = uint8_t;

	static const char *Name() {
		return "binary_fuse8";
	}
	static BitfilterType Type() {
		return BitfilterType::BINARY_FUSE8;
	}
	static bool Allocate(uint32_t size, binary_fuse8_t *filter) {
		return binary_fuse8_allocate(size, filter);
	}
	static bool Populate(uint64_t *keys, uint32_t size, binary_fuse8_t *filter) {
		return binary_fuse8_populate(keys, size, filter);
	}
	static size_t SerializationBytes(binary_fuse8_t *filter) {
		return binary_fuse8_serialization_bytes(filter);
	}
	static void Serialize(const binary_fuse8_t *filter, char *buffer) {
		binary_fuse8_serialize(filter, buffer);
	}
	static void Free(binary_fuse8_t *filter) {
		binary_fuse8_free(filter);
	}
	static bool Contains(uint64_t item, const binary_fuse8_t *filter) {
		return binary_fuse8_contain(item, filter);
	}
	static uint8_t Fingerprint(uint64_t hash) {
		return binary_fuse8_fingerprint(hash);
	}
	static binary_hashes_t Positions(uint64_t hash, const binary_fuse8_t *filter) {
		return binary_fuse8_hash_batch(hash, filter);
	}
	static uint8_t Load(const uint8_t *fingerprints, uint32_t index) {
		return fingerprints[index];
	}
	static void SetFingerprints(binary_fuse8_t *filter, const char *data) {
		filter->Fingerprints = reinterpret_cast<uint8_t *>(const_cast<char *>(data));
	}
};

template <>
struct BinaryFuseTraits<binary_fuse16_t> {
	using FingerprintType = uint16_t;

	static const char *Name() {
		return "binary_fuse16";
	}
	static BitfilterType Type() {
		return BitfilterType::BINARY_FUSE16;
	}
	static bool Allocate(uint32_t size, binary_fuse16_t *filter) {
		return binary_fuse16_allocate(size, filter);
	}
	static bool Populate(uint64_t *keys, uint32_t size, binary_fuse16_t *filter) {
		return binary_fuse16_populate(keys, size, filter);
	}
	static size_t SerializationBytes(binary_fuse16_t *filter) {
		return binary_fuse16_serialization_bytes(filter);
	}
	static void Serialize(const binary_fuse16_t *filter, char *buffer) {
		binary_fuse16_serialize(filter, buffer);
	}
	static void Free(binary_fuse16_t *filter) {
		binary_fuse16_free(filter);
	}
	// binary_fuse16_contain, with unaligned-safe fingerprint loads.
	static bool Contains(uint64_t item, const binary_fuse16_t *filter) {
		uint64_t hash = binary_fuse_mix_split(item, filter->Seed);
		uint16_t f = binary_fuse16_fingerprint(hash);
		binary_hashes_t hashes = binary_fuse16_hash_batch(hash, filter);
		f ^= LoadUnaligned16(filter->Fingerprints, hashes.h0) ^ LoadUnaligned16(filter->Fingerprints, hashes.h1) ^
		     LoadUnaligned16(filter->Fingerprints, hashes.h2);
		return f == 0;
	}
	static uint16_t Fingerprint(uint64_t hash) {
		return binary_fuse16_fingerprint(hash);
	}
	static binary_hashes_t Positions(uint64_t hash, const binary_fuse16_t *filter) {
		return binary_fuse16_hash_batch(hash, filter);
	}
	static uint16_t Load(const uint16_t *fingerprints, uint32_t index) {
		return LoadUnaligned16(fingerprints, index);
	}
	static void SetFingerprints(binary_fuse16_t *filter, const char *data) {
		filter->Fingerprints = reinterpret_cast<uint16_t *>(const_cast<char *>(data));
	}
};

// Number of bytes binary_fuse{8,16}_serialize actually writes for a filter with
// the given array length.
template <typename FilterType>
uint64_t BinaryFuseExactSerializedSize(uint64_t array_length) {
	return BINARY_FUSE_HEADER_BYTES + array_length * sizeof(typename BinaryFuseTraits<FilterType>::FingerprintType);
}

template <typename FilterType>
InvalidInputException BinaryFuseInvalidBlob(const char *function, const string &detail) {
	return InvalidInputException("%s: the filter argument is not a valid %s filter BLOB (%s). Filter BLOBs must be "
	                             "produced by %s_filter().",
	                             function, BinaryFuseTraits<FilterType>::Name(), detail,
	                             BinaryFuseTraits<FilterType>::Name());
}

// Reads a serialized filter (a BLOB's payload) without copying: on success
// `filter` points its Fingerprints array directly at the payload, so `filter`
// must not outlive it and must never be passed to binary_fuse*_free.
//
// Every field is validated against the invariants that binary_fuse*_allocate
// establishes. Those invariants are what bound the fingerprint indices computed
// by binary_fuse*_contain: h0 < SegmentCountLength, h1/h2 add up to two more
// segments, and the XOR with SegmentLengthMask can only set bits below
// SegmentLength -- so every index stays under (SegmentCount + 2) * SegmentLength
// == ArrayLength. Skipping these checks lets any BLOB (a truncated filter, a
// filter of the other width, an XOR filter, arbitrary user bytes) turn into
// wild reads far outside the BLOB.
template <typename FilterType>
void DeserializeFilter(FilterType &filter, const_data_ptr_t payload, idx_t payload_size, const char *function) {
	using Traits = BinaryFuseTraits<FilterType>;

	const auto size = static_cast<uint64_t>(payload_size);
	const auto *data = const_char_ptr_cast(payload);

	if (size < BINARY_FUSE_HEADER_BYTES) {
		throw BinaryFuseInvalidBlob<FilterType>(
		    function, StringUtil::Format("it is %llu bytes, which is smaller than the %llu byte header", size,
		                                 static_cast<uint64_t>(BINARY_FUSE_HEADER_BYTES)));
	}

	uint64_t seed;
	uint32_t filter_size, segment_length, segment_count, segment_count_length, array_length;
	idx_t offset = 0;
	auto read = [&](void *dest, idx_t bytes) {
		memcpy(dest, data + offset, bytes);
		offset += bytes;
	};
	read(&seed, sizeof(seed));
	read(&filter_size, sizeof(filter_size));
	read(&segment_length, sizeof(segment_length));
	read(&segment_count, sizeof(segment_count));
	read(&segment_count_length, sizeof(segment_count_length));
	read(&array_length, sizeof(array_length));
	D_ASSERT(offset == BINARY_FUSE_HEADER_BYTES);

	const bool segment_length_is_power_of_two = segment_length > 0 && (segment_length & (segment_length - 1)) == 0;
	if (!segment_length_is_power_of_two || segment_length > BINARY_FUSE_MAX_SEGMENT_LENGTH) {
		throw BinaryFuseInvalidBlob<FilterType>(
		    function, StringUtil::Format("its segment length (%u) is not a power of two in [1, %u]", segment_length,
		                                 BINARY_FUSE_MAX_SEGMENT_LENGTH));
	}
	if (segment_count == 0) {
		throw BinaryFuseInvalidBlob<FilterType>(function, "its segment count is zero");
	}
	const uint64_t expected_count_length = static_cast<uint64_t>(segment_count) * segment_length;
	const uint64_t expected_array_length =
	    (static_cast<uint64_t>(segment_count) + BINARY_FUSE_ARITY - 1) * static_cast<uint64_t>(segment_length);
	if (segment_count_length != expected_count_length || array_length != expected_array_length) {
		throw BinaryFuseInvalidBlob<FilterType>(
		    function,
		    StringUtil::Format("its header is inconsistent (segment count %u, segment length %u, segment count length "
		                       "%u, array length %u)",
		                       segment_count, segment_length, segment_count_length, array_length));
	}

	// Require an exact length match. Besides catching truncation, this is what
	// distinguishes an 8-bit filter from a 16-bit one: the headers are
	// identical, only the fingerprint width differs.
	const uint64_t exact_size = BinaryFuseExactSerializedSize<FilterType>(array_length);
	// Filters written before this check were sized with
	// binary_fuse16_serialization_bytes(), which counts the SegmentLengthMask
	// field that binary_fuse16_serialize never writes -- 4 trailing pad bytes.
	const uint64_t legacy_size =
	    exact_size + (sizeof(typename Traits::FingerprintType) == sizeof(uint16_t) ? sizeof(uint32_t) : 0);
	if (size != exact_size && size != legacy_size) {
		throw BinaryFuseInvalidBlob<FilterType>(
		    function, StringUtil::Format("its header describes a filter of %llu bytes (%llu byte header plus %llu "
		                                 "%u-bit fingerprints) but the BLOB "
		                                 "holds %llu bytes of filter data",
		                                 exact_size, static_cast<uint64_t>(BINARY_FUSE_HEADER_BYTES),
		                                 static_cast<uint64_t>(array_length),
		                                 static_cast<uint32_t>(sizeof(typename Traits::FingerprintType) * 8), size));
	}

	filter.Seed = seed;
	filter.Size = filter_size;
	filter.SegmentLength = segment_length;
	filter.SegmentLengthMask = segment_length - 1;
	filter.SegmentCount = segment_count;
	filter.SegmentCountLength = segment_count_length;
	filter.ArrayLength = array_length;
	Traits::SetFingerprints(&filter, data + BINARY_FUSE_HEADER_BYTES);
}

// Frees the vendored filter's fingerprint array when Finalize leaves early via
// an exception.
template <typename FilterType>
struct BinaryFuseFilterGuard {
	explicit BinaryFuseFilterGuard(FilterType &filter_p) : filter(filter_p) {
	}
	~BinaryFuseFilterGuard() {
		BinaryFuseTraits<FilterType>::Free(&filter);
	}
	FilterType &filter;
};

template <typename FilterType>
struct BinaryFuseFilterCreateOperation : BitfiltersKeyCollectOperation {
	using Traits = BinaryFuseTraits<FilterType>;

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
		auto count = static_cast<uint32_t>(keys.total_capacity);

		// The construction's scratch space and the filter itself are charged
		// to memory_limit like the collected keys.
		BitfiltersFastFilterAllocationScope allocation_scope(BitfiltersStateAllocator(finalize_data.input));
		// The construction may reorder and de-duplicate the keys in place, so
		// it works on a copy and the state stays intact.
		auto key_array = GatherKeys(keys, allocation_scope.GetAllocator());
		// The construction struggles with many repeated keys -- it makes
		// several full passes before it notices -- so drop repeats up front
		// when a sample shows plenty of them.
		if (LooksRepetitive(reinterpret_cast<uint64_t *>(key_array.get()), count)) {
			count = static_cast<uint32_t>(SortUnique(reinterpret_cast<uint64_t *>(key_array.get()), count));
		}

		FilterType filter;
		if (!Traits::Allocate(count, &filter)) {
			allocation_scope.ThrowIfFailed();
			throw OutOfMemoryException("%s_filter: failed to allocate a filter for %u values", Traits::Name(), count);
		}
		BinaryFuseFilterGuard<FilterType> guard(filter);

		auto key_data = reinterpret_cast<uint64_t *>(key_array.get());
		if (!Traits::Populate(key_data, count, &filter)) {
			allocation_scope.ThrowIfFailed();
			// The construction can give up on inputs with many repeated keys.
			// Retry once over the distinct keys -- gathered afresh, since the
			// failed attempt may have rearranged them.
			key_array.Reset();
			key_array = GatherKeys(keys, allocation_scope.GetAllocator());
			key_data = reinterpret_cast<uint64_t *>(key_array.get());
			count = static_cast<uint32_t>(SortUnique(key_data, count));
			Traits::Free(&filter);
			if (!Traits::Allocate(count, &filter)) {
				allocation_scope.ThrowIfFailed();
				throw OutOfMemoryException("%s_filter: failed to allocate a filter for %u values", Traits::Name(),
				                           count);
			}
			if (!Traits::Populate(key_data, count, &filter)) {
				allocation_scope.ThrowIfFailed();
				throw InvalidInputException(
				    "%s_filter: failed to construct a filter over %u values. This is usually caused by running out of "
				    "memory, or by an input distribution the construction algorithm could not resolve.",
				    Traits::Name(), count);
			}
		}

		const auto exact_size = BinaryFuseExactSerializedSize<FilterType>(filter.ArrayLength);
		if (BITFILTERS_HEADER_SIZE + exact_size > NumericLimits<uint32_t>::Maximum()) {
			throw OutOfRangeException("%s_filter: the resulting filter would be %llu bytes, which exceeds the maximum "
			                          "BLOB size of %u bytes",
			                          Traits::Name(), exact_size, NumericLimits<uint32_t>::Maximum());
		}
		// Serialize straight into the result rather than through a scratch
		// copy of the whole filter. The vendored size estimate can exceed what
		// the serializer writes (binary_fuse16 counts a field it never
		// writes), so size the result from the bytes actually written.
		D_ASSERT(Traits::SerializationBytes(&filter) >= exact_size);
		auto &key_format = finalize_data.input.bind_data->Cast<BitfiltersKeyBindData>().keys;
		target = StringVector::EmptyString(finalize_data.result, BITFILTERS_HEADER_SIZE + exact_size);
		auto out = target.GetDataWriteable();
		BitfiltersWriteHeader(data_ptr_cast(out), Traits::Type(), key_format);
		Traits::Serialize(&filter, out + BITFILTERS_HEADER_SIZE);
		target.Finalize();
	}
};

template <typename T, typename FilterType>
auto static BinaryFuseFilterCreateAggregate(const LogicalType &type, const LogicalType &result_type)
    -> AggregateFunction {
	auto aggregate = BitfiltersKeyAggregate<BinaryFuseFilterCreateOperation<FilterType>>({}, type, result_type);
	aggregate.SetFallible();
	return aggregate;
}

template <typename FilterType>
class BinaryFuseProbe : public BitfilterProbeBase<BinaryFuseProbe<FilterType>> {
public:
	using Traits = BinaryFuseTraits<FilterType>;

	BinaryFuseProbe(const_data_ptr_t payload, idx_t size, const char *function) {
		DeserializeFilter(filter, payload, size, function);
	}

	bool ContainsKey(uint64_t key) const {
		return Traits::Contains(key, &filter);
	}

	// The three fingerprint positions of a key and the fingerprint they must
	// XOR to, as binary_fuse{8,16}_contain computes them.
	struct Location {
		binary_hashes_t positions;
		typename Traits::FingerprintType fingerprint;
	};

	Location Locate(uint64_t key) const {
		const uint64_t hash = binary_fuse_mix_split(key, filter.Seed);
		Location location;
		location.fingerprint = Traits::Fingerprint(hash);
		location.positions = Traits::Positions(hash, &filter);
		BITFILTERS_PREFETCH(filter.Fingerprints + location.positions.h0);
		BITFILTERS_PREFETCH(filter.Fingerprints + location.positions.h1);
		BITFILTERS_PREFETCH(filter.Fingerprints + location.positions.h2);
		return location;
	}

	bool Check(const Location &location) const {
		return location.fingerprint ==
		       static_cast<typename Traits::FingerprintType>(Traits::Load(filter.Fingerprints, location.positions.h0) ^
		                                                     Traits::Load(filter.Fingerprints, location.positions.h1) ^
		                                                     Traits::Load(filter.Fingerprints, location.positions.h2));
	}

	void ContainsBatch(const uint64_t *keys, idx_t count, bool *result) const override {
		BitfiltersPrefetchingBatch<BinaryFuseProbe, Location>(*this, keys, count, result);
	}

	void Describe(BitfilterInfo &info) const override {
		// Size counts every value the filter was built from, repeats included.
		info.entries = filter.Size;
		info.fingerprint_bits = sizeof(typename Traits::FingerprintType) * 8;
		info.expected_fpr = std::ldexp(1.0, -static_cast<int>(info.fingerprint_bits));
		info.seed = filter.Seed;
	}

private:
	FilterType filter;
};

template <typename T, typename FilterType>
static void RegisterBinaryFuseFilterFunctionsForType(AggregateFunctionSet &BinaryFusefilter, const LogicalType &type) {
	BinaryFusefilter.AddFunction(BinaryFuseFilterCreateAggregate<T, FilterType>(type, LogicalType::BLOB));
}

template <BitfilterType TYPE>
static void RegisterBinaryFuseFilterContainsForType(ScalarFunctionSet &fs, const LogicalType &) {
	const auto type = TYPE;
	fs.AddFunction(BitfiltersContainsFunction(&type));
}
} // namespace

unique_ptr<BitfilterProbe> BinaryFuseFilterOpen(BitfilterType type, const_data_ptr_t payload, idx_t size,
                                                const char *function) {
	if (type == BitfilterType::BINARY_FUSE8) {
		return make_uniq<BinaryFuseProbe<binary_fuse8_t>>(payload, size, function);
	}
	return make_uniq<BinaryFuseProbe<binary_fuse16_t>>(payload, size, function);
}

void LoadBinaryFuseFilter(ExtensionLoader &loader) {
	// Register aggregate functions
	{
		AggregateFunctionSet BinaryFusefilter("binary_fuse16_filter");
		RegisterBinaryFuseFilterFunctionsForType<uint64_t, binary_fuse16_t>(BinaryFusefilter, LogicalType::ANY);
		CreateAggregateFunctionInfo BinaryFusefilter_create_info(BinaryFusefilter);
		{
			FunctionDescription desc;
			desc.description = "Creates a Binary Fuse 16-bit filter (~0.0015% false positive rate) by aggregating "
			                   "values of any type: UBIGINT values are used as keys as they are, other values are "
			                   "hashed with XXH64 of a fixed encoding that does not change between DuckDB versions. "
			                   "Returns a BLOB to probe with binary_fuse16_filter_contains.";
			desc.parameter_names = {"value"};
			desc.parameter_types = {LogicalType::ANY};
			desc.examples.push_back("SELECT binary_fuse16_filter(column) FROM table");
			desc.categories = {"bitfilters", "probabilistic"};
			BinaryFusefilter_create_info.descriptions.push_back(desc);
		}
		BitfiltersApplyParameterNames(BinaryFusefilter_create_info.functions,
		                              BinaryFusefilter_create_info.descriptions);
		loader.RegisterFunction(BinaryFusefilter_create_info);
	}

	{
		AggregateFunctionSet BinaryFusefilter("binary_fuse8_filter");
		RegisterBinaryFuseFilterFunctionsForType<uint64_t, binary_fuse8_t>(BinaryFusefilter, LogicalType::ANY);
		CreateAggregateFunctionInfo BinaryFusefilter_create_info(BinaryFusefilter);
		{
			FunctionDescription desc;
			desc.description = "Creates a Binary Fuse 8-bit filter (~0.4% false positive rate) by aggregating values "
			                   "of any type: UBIGINT values are used as keys as they are, other values are hashed with "
			                   "XXH64 of a fixed encoding that does not change between DuckDB versions. Returns a BLOB "
			                   "to probe with binary_fuse8_filter_contains.";
			desc.parameter_names = {"value"};
			desc.parameter_types = {LogicalType::ANY};
			desc.examples.push_back("SELECT binary_fuse8_filter(column) FROM table");
			desc.categories = {"bitfilters", "probabilistic"};
			BinaryFusefilter_create_info.descriptions.push_back(desc);
		}
		BitfiltersApplyParameterNames(BinaryFusefilter_create_info.functions,
		                              BinaryFusefilter_create_info.descriptions);
		loader.RegisterFunction(BinaryFusefilter_create_info);
	}

	// Register scalar functions
	{
		ScalarFunctionSet fs("binary_fuse16_filter_contains");
		RegisterBinaryFuseFilterContainsForType<BitfilterType::BINARY_FUSE16>(fs, LogicalType::ANY);
		fs.SetFallible();
		CreateScalarFunctionInfo info(std::move(fs));
		{
			FunctionDescription desc;
			desc.description = "Tests if a Binary Fuse 16-bit filter may contain a value. Returns true if the value "
			                   "might be in the set (with possible false positives), or false if the value "
			                   "is definitely not in the set (no false negatives).";
			desc.parameter_names = {"filter", "value"};
			desc.parameter_types = {LogicalType::BLOB, LogicalType::ANY};
			desc.examples.push_back("SELECT binary_fuse16_filter_contains(filter, 42) FROM table");
			desc.categories = {"bitfilters", "probabilistic"};
			info.descriptions.push_back(desc);
		}
		BitfiltersApplyParameterNames(info.functions, info.descriptions);
		loader.RegisterFunction(info);
	}

	{
		ScalarFunctionSet fs("binary_fuse8_filter_contains");
		RegisterBinaryFuseFilterContainsForType<BitfilterType::BINARY_FUSE8>(fs, LogicalType::ANY);
		fs.SetFallible();
		CreateScalarFunctionInfo info(std::move(fs));
		{
			FunctionDescription desc;
			desc.description = "Tests if a Binary Fuse 8-bit filter may contain a value. Returns true if the value "
			                   "might be in the set (with possible false positives), or false if the value "
			                   "is definitely not in the set (no false negatives).";
			desc.parameter_names = {"filter", "value"};
			desc.parameter_types = {LogicalType::BLOB, LogicalType::ANY};
			desc.examples.push_back("SELECT binary_fuse8_filter_contains(filter, 42) FROM table");
			desc.categories = {"bitfilters", "probabilistic"};
			info.descriptions.push_back(desc);
		}
		BitfiltersApplyParameterNames(info.functions, info.descriptions);
		loader.RegisterFunction(info);
	}
}

} // namespace duckdb
