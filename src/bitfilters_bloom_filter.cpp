#include "bitfilters_bloom_filter.hpp"
#include "bitfilters_aggregate_util.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/common/vector/string_vector.hpp"

#include <cmath>

namespace duckdb {

namespace {

// Split block Bloom filter, as specified for Parquet
// (https://github.com/apache/parquet-format/blob/master/BloomFilter.md):
//
// The filter is an array of 32 byte blocks, each eight little-endian 32-bit
// words. A 64-bit key picks a block with its upper half, and its lower half
// sets one bit in each of the block's words. Keys are XXH64 of the values'
// Parquet plain encoding, so a filter over the same values matches the Bloom
// filters Parquet writers store, bit for bit for the same size.
//
// The payload (after the bitfilters header) is just the blocks.

constexpr idx_t BLOOM_BLOCK_BYTES = 32;
constexpr idx_t BLOOM_WORDS = 8;
// The largest power of two block array that still fits in a BLOB with header.
constexpr idx_t BLOOM_MAX_BYTES = idx_t(1) << 31;

constexpr uint32_t BLOOM_SALT[BLOOM_WORDS] = {0x47b6137bU, 0x44974d91U, 0x8824ad5bU, 0xa2b7289dU,
                                              0x705495c7U, 0x2df1424bU, 0x9efc4947U, 0x5c6bfb31U};

inline data_ptr_t BloomBlock(data_ptr_t bits, uint64_t num_blocks, uint64_t key) {
	return bits + ((key >> 32) * num_blocks >> 32) * BLOOM_BLOCK_BYTES;
}

inline const_data_ptr_t BloomBlock(const_data_ptr_t bits, uint64_t num_blocks, uint64_t key) {
	return bits + ((key >> 32) * num_blocks >> 32) * BLOOM_BLOCK_BYTES;
}

inline void BloomInsert(data_ptr_t bits, uint64_t num_blocks, uint64_t key) {
	auto block = BloomBlock(bits, num_blocks, key);
	const auto x = static_cast<uint32_t>(key);
	for (idx_t i = 0; i < BLOOM_WORDS; i++) {
		auto word = Load<uint32_t>(block + i * sizeof(uint32_t));
		word |= uint32_t(1) << ((x * BLOOM_SALT[i]) >> 27);
		Store<uint32_t>(word, block + i * sizeof(uint32_t));
	}
}

inline bool BloomCheck(const_data_ptr_t bits, uint64_t num_blocks, uint64_t key) {
	auto block = BloomBlock(bits, num_blocks, key);
	const auto x = static_cast<uint32_t>(key);
	bool result = true;
	for (idx_t i = 0; i < BLOOM_WORDS; i++) {
		const auto word = Load<uint32_t>(block + i * sizeof(uint32_t));
		result &= ((word >> ((x * BLOOM_SALT[i]) >> 27)) & 1) != 0;
	}
	return result;
}

inline void BloomUnion(data_ptr_t target, const_data_ptr_t source, idx_t size) {
	for (idx_t i = 0; i < size; i += sizeof(uint64_t)) {
		Store<uint64_t>(Load<uint64_t>(target + i) | Load<uint64_t>(source + i), target + i);
	}
}

// Block array size for `ndv` distinct values at false positive rate `fpp`,
// computed the way DuckDB's Parquet writer sizes its Bloom filters (rounded up
// to a power of two), so the same values give the same filter.
idx_t BloomFilterBytes(double ndv, double fpp) {
	const double k = BLOOM_WORDS;
	const double bits = -k * ndv / std::log(1.0 - std::pow(fpp, 1.0 / k));
	const double bytes = bits / k;
	if (!(bytes <= static_cast<double>(BLOOM_MAX_BYTES))) {
		return BLOOM_MAX_BYTES + 1;
	}
	return MaxValue<idx_t>(NextPowerOfTwo(static_cast<idx_t>(bytes)), BLOOM_BLOCK_BYTES);
}

struct BloomFilterBindData : public BitfiltersKeyBindData {
	BloomFilterBindData(BitfilterKeyFormat keys_p, idx_t size_p) : BitfiltersKeyBindData(keys_p), size(size_p) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<BloomFilterBindData>(keys, size);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<BloomFilterBindData>();
		return keys == other.keys && size == other.size;
	}

	//! Size of the block array in bytes
	idx_t size;
};

Value BindConstant(ClientContext &context, Expression &argument, const char *name) {
	if (argument.HasParameter()) {
		throw ParameterNotResolvedException();
	}
	if (!argument.IsFoldable()) {
		throw BinderException("bloom_filter: %s must be a constant", name);
	}
	auto value = ExpressionExecutor::EvaluateScalar(context, argument);
	if (value.IsNull()) {
		throw BinderException("bloom_filter: %s cannot be NULL", name);
	}
	return value;
}

unique_ptr<FunctionData> BloomFilterBind(BindAggregateFunctionInput &input) {
	auto &context = input.GetClientContext();
	auto &arguments = input.GetArguments();
	D_ASSERT(arguments.size() == 3);
	const auto ndv = BindConstant(context, *arguments[0], "ndv").GetValue<int64_t>();
	const auto fpp = BindConstant(context, *arguments[1], "fpp").GetValue<double>();
	if (ndv <= 0) {
		throw BinderException("bloom_filter: ndv (the expected number of distinct values) must be positive, got %lld",
		                      static_cast<long long>(ndv));
	}
	if (!(fpp > 0 && fpp < 1)) {
		throw BinderException("bloom_filter: fpp (the target false positive rate) must be between 0 and 1, got %f",
		                      fpp);
	}
	const auto size = BloomFilterBytes(static_cast<double>(ndv), fpp);
	if (size > BLOOM_MAX_BYTES) {
		throw BinderException("bloom_filter: %lld distinct values at a false positive rate of %f need more than the "
		                      "maximum filter size of %llu bytes",
		                      static_cast<long long>(ndv), fpp, static_cast<uint64_t>(BLOOM_MAX_BYTES));
	}
	auto keys = BitfilterKeyFormat::ForValues(arguments.back()->GetReturnType(), false);
	return make_uniq<BloomFilterBindData>(keys, size);
}

void WriteBloomFilter(const_data_ptr_t bits, idx_t size, const BitfilterKeyFormat &keys, Vector &result,
                      string_t &target) {
	target = StringVector::EmptyString(result, BITFILTERS_HEADER_SIZE + size);
	auto out = data_ptr_cast(target.GetDataWriteable());
	BitfiltersWriteHeader(out, BitfilterType::BLOOM, keys);
	memcpy(out + BITFILTERS_HEADER_SIZE, bits, size);
	target.Finalize();
}

// The block array lives in the aggregate's arena: it counts against
// memory_limit and is released with the arena, so the state needs no
// destructor. It is fixed-size and touched by every row of its group, which is
// why it is not declared spillable (exporting it would rewrite every block
// array on nearly every chunk).
struct BloomFilterState {
	data_ptr_t bits;
};

struct BloomFilterCreateOperation {
	template <class STATE>
	static void Initialize(STATE &state) {
		state.bits = nullptr;
	}

	static bool IgnoreNull() {
		return true;
	}

	template <class STATE>
	static void Destroy(STATE &, AggregateInputData &) {
	}

	static data_ptr_t AllocateBits(AggregateInputData &input, idx_t size) {
		auto bits = input.allocator.AllocateAligned(size);
		memset(bits, 0, size);
		return bits;
	}

	template <class A_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const A_TYPE &key, AggregateUnaryInput &unary_input) {
		auto &bind_data = unary_input.input.bind_data->Cast<BloomFilterBindData>();
		if (!state.bits) {
			state.bits = AllocateBits(unary_input.input, bind_data.size);
		}
		BloomInsert(state.bits, bind_data.size / BLOOM_BLOCK_BYTES, key);
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input, idx_t) {
		Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &input) {
		if (!source.bits) {
			return;
		}
		auto &bind_data = input.bind_data->Cast<BloomFilterBindData>();
		if (!target.bits) {
			if (input.combine_type == AggregateCombineType::ALLOW_DESTRUCTIVE) {
				// Arenas outlive the states pointing into them.
				target.bits = source.bits;
				return;
			}
			target.bits = AllocateBits(input, bind_data.size);
		}
		BloomUnion(target.bits, source.bits, bind_data.size);
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (!state.bits) {
			finalize_data.ReturnNull();
			return;
		}
		auto &bind_data = finalize_data.input.bind_data->Cast<BloomFilterBindData>();
		WriteBloomFilter(state.bits, bind_data.size, bind_data.keys, finalize_data.result, target);
	}
};

// Checks a Bloom filter payload; throws InvalidInputException if it is not one.
void ValidateBloomPayload(idx_t size, const char *function) {
	if (size == 0 || size % BLOOM_BLOCK_BYTES != 0) {
		throw InvalidInputException("%s: the filter argument is not a valid bloom filter BLOB (its block array is "
		                            "%llu bytes, which is not a positive multiple of %llu). Filter BLOBs must be "
		                            "produced by bloom_filter().",
		                            function, static_cast<uint64_t>(size), static_cast<uint64_t>(BLOOM_BLOCK_BYTES));
	}
}

// bloom_filter_merge(filter): the union of Bloom filters of the same size.
struct BloomFilterMergeState {
	data_ptr_t bits;
	idx_t size;
	BitfilterKeyFormat keys;
};

struct BloomFilterMergeOperation {
	template <class STATE>
	static void Initialize(STATE &state) {
		state.bits = nullptr;
		state.size = 0;
		state.keys = BitfilterKeyFormat();
	}

	static bool IgnoreNull() {
		return true;
	}

	static void CheckCompatible(idx_t target_size, const BitfilterKeyFormat &target_keys, idx_t source_size,
	                            const BitfilterKeyFormat &source_keys) {
		if (target_size != source_size) {
			throw InvalidInputException("bloom_filter_merge: cannot merge Bloom filters of different sizes (%llu and "
			                            "%llu bytes); build them with the same ndv and fpp",
			                            static_cast<uint64_t>(target_size), static_cast<uint64_t>(source_size));
		}
		if (target_keys != source_keys) {
			throw InvalidInputException("bloom_filter_merge: cannot merge a Bloom filter built from %s values into one "
			                            "built from %s values",
			                            source_keys.KeyTypeName(), target_keys.KeyTypeName());
		}
	}

	template <class STATE>
	static void Absorb(STATE &state, const_data_ptr_t bits, idx_t size, const BitfilterKeyFormat &keys,
	                   AggregateInputData &input) {
		if (!state.bits) {
			state.bits = input.allocator.AllocateAligned(size);
			memcpy(state.bits, bits, size);
			state.size = size;
			state.keys = keys;
			return;
		}
		CheckCompatible(state.size, state.keys, size, keys);
		BloomUnion(state.bits, bits, size);
	}

	template <class A_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const A_TYPE &blob, AggregateUnaryInput &unary_input) {
		auto parsed = BitfilterBlob::Expect(blob, BitfilterType::BLOOM, "bloom_filter_merge");
		if (!parsed.has_header) {
			throw InvalidInputException("bloom_filter_merge: the filter argument is not a bloom filter BLOB. Filter "
			                            "BLOBs must be produced by bloom_filter().");
		}
		ValidateBloomPayload(parsed.payload_size, "bloom_filter_merge");
		Absorb(state, parsed.payload, parsed.payload_size, parsed.keys, unary_input.input);
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input, idx_t) {
		Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &input) {
		if (!source.bits) {
			return;
		}
		if (!target.bits && input.combine_type == AggregateCombineType::ALLOW_DESTRUCTIVE) {
			target = source;
			return;
		}
		Absorb(target, source.bits, source.size, source.keys, input);
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (!state.bits) {
			finalize_data.ReturnNull();
			return;
		}
		WriteBloomFilter(state.bits, state.size, state.keys, finalize_data.result, target);
	}
};

class BloomProbe : public BitfilterProbeBase<BloomProbe> {
public:
	BloomProbe(const_data_ptr_t bits_p, idx_t size_p) : bits(bits_p), size(size_p), num_blocks(size_p / 32) {
	}

	bool ContainsKey(uint64_t key) const {
		return BloomCheck(bits, num_blocks, key);
	}

	// A key's block (one 32 byte read) and the half of the key that picks
	// its bits in that block.
	struct Location {
		const_data_ptr_t block;
		uint32_t x;
	};

	Location Locate(uint64_t key) const {
		Location location {BloomBlock(bits, num_blocks, key), static_cast<uint32_t>(key)};
		// BLOBs carry no alignment guarantee, so a block can straddle two
		// cache lines.
		BITFILTERS_PREFETCH(location.block);
		BITFILTERS_PREFETCH(location.block + BLOOM_BLOCK_BYTES - 1);
		return location;
	}

	bool Check(const Location &location) const {
		bool result = true;
		for (idx_t i = 0; i < BLOOM_WORDS; i++) {
			const auto word = Load<uint32_t>(location.block + i * sizeof(uint32_t));
			result &= ((word >> ((location.x * BLOOM_SALT[i]) >> 27)) & 1) != 0;
		}
		return result;
	}

	void ContainsBatch(const uint64_t *keys, idx_t count, bool *result) const override {
		BitfiltersPrefetchingBatch<BloomProbe, Location>(*this, keys, count, result);
	}

	void Describe(BitfilterInfo &info) const override {
		// A random key hits a random block and has to find its bit set in all
		// eight words: the expected false positive rate is the mean over the
		// blocks of the product of the words' fill ratios.
		idx_t set_bits = 0;
		double fpr = 0;
		for (idx_t block = 0; block < num_blocks; block++) {
			double hit = 1;
			for (idx_t i = 0; i < BLOOM_WORDS; i++) {
				const auto ones = PopCount(Load<uint32_t>(bits + block * BLOOM_BLOCK_BYTES + i * sizeof(uint32_t)));
				set_bits += ones;
				hit *= static_cast<double>(ones) / 32.0;
			}
			fpr += hit;
		}
		info.load_factor = static_cast<double>(set_bits) / static_cast<double>(size * 8);
		info.expected_fpr = fpr / static_cast<double>(num_blocks);
	}

private:
	static idx_t PopCount(uint32_t x) {
		idx_t count = 0;
		for (; x; count++) {
			x &= x - 1;
		}
		return count;
	}

	const_data_ptr_t bits;
	idx_t size;
	uint64_t num_blocks;
};

} // namespace

unique_ptr<BitfilterProbe> BloomFilterOpen(const_data_ptr_t payload, idx_t size, const char *function) {
	ValidateBloomPayload(size, function);
	return make_uniq<BloomProbe>(payload, size);
}

void LoadBloomFilter(ExtensionLoader &loader) {
	{
		AggregateFunctionSet set("bloom_filter");
		auto fun =
		    BitfiltersUnaryAggregateWithLeadingArgs<2, BloomFilterState, uint64_t, string_t, BloomFilterCreateOperation,
		                                            AggregateDestructorType::LEGACY, true>(
		        {LogicalType::BIGINT, LogicalType::DOUBLE}, LogicalType::ANY, LogicalType::BLOB);
		fun.SetBindCallback(BloomFilterBind);
		fun.SetFallible();
		set.AddFunction(fun);
		CreateAggregateFunctionInfo info(set);
		FunctionDescription desc;
		desc.description =
		    "Creates a split block Bloom filter (the Parquet Bloom filter format) sized for ndv distinct values at a "
		    "false positive rate of fpp. Values are hashed with XXH64 of their Parquet plain encoding, so the filter "
		    "matches the Bloom filters Parquet writers produce. Bloom filters of the same size merge with "
		    "bloom_filter_merge. Returns a BLOB to probe with bloom_filter_contains.";
		desc.parameter_names = {"ndv", "fpp", "value"};
		desc.parameter_types = {LogicalType::BIGINT, LogicalType::DOUBLE, LogicalType::ANY};
		desc.examples.push_back("SELECT bloom_filter(1000000, 0.01, column) FROM table");
		desc.categories = {"bitfilters", "probabilistic"};
		info.descriptions.push_back(desc);
		BitfiltersApplyParameterNames(info.functions, info.descriptions);
		loader.RegisterFunction(info);
	}
	{
		AggregateFunctionSet set("bloom_filter_merge");
		auto fun =
		    AggregateFunction::UnaryAggregate<BloomFilterMergeState, string_t, string_t, BloomFilterMergeOperation,
		                                      AggregateDestructorType::LEGACY>(LogicalType::BLOB, LogicalType::BLOB);
		fun.SetFallible();
		set.AddFunction(fun);
		CreateAggregateFunctionInfo info(set);
		FunctionDescription desc;
		desc.description = "Merges Bloom filters (BLOBs produced by bloom_filter with the same ndv and fpp) into one "
		                   "filter containing every value of the inputs.";
		desc.parameter_names = {"filter"};
		desc.parameter_types = {LogicalType::BLOB};
		desc.examples.push_back("SELECT bloom_filter_merge(filter) FROM filters");
		desc.categories = {"bitfilters", "probabilistic"};
		info.descriptions.push_back(desc);
		BitfiltersApplyParameterNames(info.functions, info.descriptions);
		loader.RegisterFunction(info);
	}
	{
		ScalarFunctionSet fs("bloom_filter_contains");
		const auto type = BitfilterType::BLOOM;
		fs.AddFunction(BitfiltersContainsFunction(&type));
		fs.SetFallible();
		CreateScalarFunctionInfo info(std::move(fs));
		FunctionDescription desc;
		desc.description = "Tests if a Bloom filter may contain a value. Returns true if the value might be in the set "
		                   "(with possible false positives), or false if the value is definitely not in the set (no "
		                   "false negatives).";
		desc.parameter_names = {"filter", "value"};
		desc.parameter_types = {LogicalType::BLOB, LogicalType::ANY};
		desc.examples.push_back("SELECT bloom_filter_contains(filter, 42) FROM table");
		desc.categories = {"bitfilters", "probabilistic"};
		info.descriptions.push_back(desc);
		BitfiltersApplyParameterNames(info.functions, info.descriptions);
		loader.RegisterFunction(info);
	}
}

} // namespace duckdb
