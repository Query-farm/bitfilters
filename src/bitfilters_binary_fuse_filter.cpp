#include "bitfilters_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/common/vector/constant_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "vendor/fastfilter/binaryfusefilter.h"

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
InvalidInputException BinaryFuseInvalidBlob(const string &detail) {
	return InvalidInputException("%s_filter_contains: the filter argument is not a valid %s filter BLOB (%s). Filter "
	                             "BLOBs must be produced by %s_filter().",
	                             BinaryFuseTraits<FilterType>::Name(), BinaryFuseTraits<FilterType>::Name(), detail,
	                             BinaryFuseTraits<FilterType>::Name());
}

// Reads a serialized filter out of `blob` without copying: on success `filter`
// points its Fingerprints array directly at the BLOB's bytes, so `filter` must
// not outlive `blob` and must never be passed to binary_fuse*_free.
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
void DeserializeFilter(FilterType &filter, const string_t &blob) {
	using Traits = BinaryFuseTraits<FilterType>;

	const auto size = static_cast<uint64_t>(blob.GetSize());
	const auto *data = blob.GetData();

	if (size < BINARY_FUSE_HEADER_BYTES) {
		throw BinaryFuseInvalidBlob<FilterType>(
		    StringUtil::Format("it is %llu bytes, which is smaller than the %llu byte header", size,
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
		    StringUtil::Format("its segment length (%u) is not a power of two in [1, %u]", segment_length,
		                       BINARY_FUSE_MAX_SEGMENT_LENGTH));
	}
	if (segment_count == 0) {
		throw BinaryFuseInvalidBlob<FilterType>("its segment count is zero");
	}
	const uint64_t expected_count_length = static_cast<uint64_t>(segment_count) * segment_length;
	const uint64_t expected_array_length =
	    (static_cast<uint64_t>(segment_count) + BINARY_FUSE_ARITY - 1) * static_cast<uint64_t>(segment_length);
	if (segment_count_length != expected_count_length || array_length != expected_array_length) {
		throw BinaryFuseInvalidBlob<FilterType>(
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
		throw BinaryFuseInvalidBlob<FilterType>(StringUtil::Format(
		    "its header describes a filter of %llu bytes (%llu byte header plus %llu %u-bit fingerprints) but the BLOB "
		    "is %llu bytes",
		    exact_size, static_cast<uint64_t>(BINARY_FUSE_HEADER_BYTES), static_cast<uint64_t>(array_length),
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

struct BinaryFuseFilterState {
	// Raw pointer rather than unique_ptr: aggregate states are handed to us as
	// uninitialized memory, so a smart pointer's assignment operator would run
	// over (and try to free) a garbage pointer.
	std::vector<uint64_t> *entries;
};

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
struct BinaryFuseFilterCreateOperation {
	using Traits = BinaryFuseTraits<FilterType>;

	template <class STATE>
	static void Initialize(STATE &state) {
		state.entries = nullptr;
	}

	static bool IgnoreNull() {
		return true;
	}

	template <class STATE>
	static void Destroy(STATE &state, AggregateInputData &) {
		delete state.entries;
		state.entries = nullptr;
	}

	template <class STATE>
	static std::vector<uint64_t> &Entries(STATE &state) {
		if (!state.entries) {
			state.entries = new std::vector<uint64_t>();
		}
		return *state.entries;
	}

	template <class A_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const A_TYPE &a_data, AggregateUnaryInput &) {
		Entries(state).push_back(a_data);
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &, idx_t count) {
		auto &entries = Entries(state);
		entries.insert(entries.end(), count, input);
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		if (!source.entries || source.entries->empty()) {
			return;
		}
		auto &target_entries = Entries(target);
		target_entries.reserve(target_entries.size() + source.entries->size());
		target_entries.insert(target_entries.end(), source.entries->begin(), source.entries->end());
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (!state.entries || state.entries->empty()) {
			finalize_data.ReturnNull();
			return;
		}
		auto &entries = *state.entries;
		if (entries.size() > NumericLimits<uint32_t>::Maximum()) {
			throw OutOfRangeException("%s_filter: cannot build a filter over %llu values, the maximum is %u",
			                          Traits::Name(), static_cast<uint64_t>(entries.size()),
			                          NumericLimits<uint32_t>::Maximum());
		}
		const auto count = static_cast<uint32_t>(entries.size());

		FilterType filter;
		if (!Traits::Allocate(count, &filter)) {
			throw OutOfMemoryException("%s_filter: failed to allocate a filter for %u values", Traits::Name(), count);
		}
		BinaryFuseFilterGuard<FilterType> guard(filter);

		// Populate rewrites `entries` in place (it sorts and de-duplicates the
		// keys when it detects duplicates), which is fine -- the state is dead
		// once we have finalized it.
		if (!Traits::Populate(entries.data(), count, &filter)) {
			throw InvalidInputException(
			    "%s_filter: failed to construct a filter over %u values. This is usually caused by running out of "
			    "memory, or by an input distribution the construction algorithm could not resolve.",
			    Traits::Name(), count);
		}

		const auto exact_size = BinaryFuseExactSerializedSize<FilterType>(filter.ArrayLength);
		if (exact_size > NumericLimits<uint32_t>::Maximum()) {
			throw OutOfRangeException("%s_filter: the resulting filter would be %llu bytes, which exceeds the maximum "
			                          "BLOB size of %u bytes",
			                          Traits::Name(), exact_size, NumericLimits<uint32_t>::Maximum());
		}
		// The vendored serializer's size estimate can exceed what it writes, so
		// allocate with the estimate but only keep the bytes it wrote.
		const auto buffer_size = Traits::SerializationBytes(&filter);
		D_ASSERT(buffer_size >= exact_size);
		std::vector<char> buffer(buffer_size);
		Traits::Serialize(&filter, buffer.data());
		target = StringVector::AddStringOrBlob(finalize_data.result, buffer.data(), exact_size);
	}
};

template <typename T, typename FilterType>
auto static BinaryFuseFilterCreateAggregate(const LogicalType &type, const LogicalType &result_type)
    -> AggregateFunction {
	auto aggregate =
	    AggregateFunction::UnaryAggregate<BinaryFuseFilterState, T, string_t,
	                                      BinaryFuseFilterCreateOperation<FilterType>, AggregateDestructorType::LEGACY>(
	        type, result_type);
	aggregate.SetFallible();
	return aggregate;
}

template <class T, typename FilterType>
static inline void BinaryFuseFilterContains(DataChunk &args, ExpressionState &state, Vector &result) {
	using Traits = BinaryFuseTraits<FilterType>;

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

// Helper function to register BinaryFuse filter functions for a specific type
template <typename T, typename FilterType>
static void RegisterBinaryFuseFilterFunctionsForType(AggregateFunctionSet &BinaryFusefilter, const LogicalType &type) {
	// Register create aggregate function
	BinaryFusefilter.AddFunction(BinaryFuseFilterCreateAggregate<T, FilterType>(type, LogicalType::BLOB));
}

// Helper function to register scalar contains functions for a specific type
template <typename T, typename FilterType>
static void RegisterBinaryFuseFilterContainsForType(ScalarFunctionSet &fs, const LogicalType &type) {
	fs.AddFunction(
	    ScalarFunction({LogicalType::BLOB, type}, LogicalType::BOOLEAN, BinaryFuseFilterContains<T, FilterType>));
}
} // namespace

void LoadBinaryFuseFilter(ExtensionLoader &loader) {
	// Register aggregate functions
	{
		AggregateFunctionSet BinaryFusefilter("binary_fuse16_filter");
		RegisterBinaryFuseFilterFunctionsForType<uint64_t, binary_fuse16_t>(BinaryFusefilter, LogicalType::UBIGINT);
		CreateAggregateFunctionInfo BinaryFusefilter_create_info(BinaryFusefilter);
		{
			FunctionDescription desc;
			desc.description = "Creates a Binary Fuse 16-bit filter (~0.0015% false positive rate) by aggregating "
			                   "UBIGINT values. Returns a BLOB to probe with binary_fuse16_filter_contains.";
			desc.parameter_names = {"value"};
			desc.parameter_types = {LogicalType::UBIGINT};
			desc.examples.push_back("SELECT binary_fuse16_filter(hash(column)) FROM table");
			desc.categories = {"bitfilters", "probabilistic"};
			BinaryFusefilter_create_info.descriptions.push_back(desc);
		}
		loader.RegisterFunction(BinaryFusefilter_create_info);
	}

	{
		AggregateFunctionSet BinaryFusefilter("binary_fuse8_filter");
		RegisterBinaryFuseFilterFunctionsForType<uint64_t, binary_fuse8_t>(BinaryFusefilter, LogicalType::UBIGINT);
		CreateAggregateFunctionInfo BinaryFusefilter_create_info(BinaryFusefilter);
		{
			FunctionDescription desc;
			desc.description = "Creates a Binary Fuse 8-bit filter (~0.4% false positive rate) by aggregating "
			                   "UBIGINT values. Returns a BLOB to probe with binary_fuse8_filter_contains.";
			desc.parameter_names = {"value"};
			desc.parameter_types = {LogicalType::UBIGINT};
			desc.examples.push_back("SELECT binary_fuse8_filter(hash(column)) FROM table");
			desc.categories = {"bitfilters", "probabilistic"};
			BinaryFusefilter_create_info.descriptions.push_back(desc);
		}
		loader.RegisterFunction(BinaryFusefilter_create_info);
	}

	// Register scalar functions
	{
		ScalarFunctionSet fs("binary_fuse16_filter_contains");
		RegisterBinaryFuseFilterContainsForType<uint64_t, binary_fuse16_t>(fs, LogicalType::UBIGINT);
		fs.SetFallible();
		CreateScalarFunctionInfo info(std::move(fs));
		{
			FunctionDescription desc;
			desc.description = "Tests if a Binary Fuse 16-bit filter may contain a value. Returns true if the value "
			                   "might be in the set (with possible false positives), or false if the value "
			                   "is definitely not in the set (no false negatives).";
			desc.parameter_names = {"filter", "value"};
			desc.parameter_types = {LogicalType::BLOB, LogicalType::UBIGINT};
			desc.examples.push_back("SELECT binary_fuse16_filter_contains(filter, 42) FROM table");
			desc.categories = {"bitfilters", "probabilistic"};
			info.descriptions.push_back(desc);
		}
		loader.RegisterFunction(info);
	}

	{
		ScalarFunctionSet fs("binary_fuse8_filter_contains");
		RegisterBinaryFuseFilterContainsForType<uint64_t, binary_fuse8_t>(fs, LogicalType::UBIGINT);
		fs.SetFallible();
		CreateScalarFunctionInfo info(std::move(fs));
		{
			FunctionDescription desc;
			desc.description = "Tests if a Binary Fuse 8-bit filter may contain a value. Returns true if the value "
			                   "might be in the set (with possible false positives), or false if the value "
			                   "is definitely not in the set (no false negatives).";
			desc.parameter_names = {"filter", "value"};
			desc.parameter_types = {LogicalType::BLOB, LogicalType::UBIGINT};
			desc.examples.push_back("SELECT binary_fuse8_filter_contains(filter, 42) FROM table");
			desc.categories = {"bitfilters", "probabilistic"};
			info.descriptions.push_back(desc);
		}
		loader.RegisterFunction(info);
	}
}

} // namespace duckdb
