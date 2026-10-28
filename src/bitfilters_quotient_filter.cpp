#include "bitfilters_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/common/vector/constant_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "bitfilters_aggregate_util.hpp"
#include "vendor/quotient-filter/quotient_filter.hpp"

#include <cmath>

namespace duckdb {

namespace {

// Whether a filter with these parameters fits in a BLOB (at most 4GB). Any
// q >= 35 is over the limit (2^35 slots of at least 4 bits each); checking it
// first keeps the size computation from overflowing.
bool QuotientFilterFits(uint32_t q, uint32_t r) {
	return q < 35 &&
	       BITFILTERS_HEADER_SIZE + QuotientFilter::serialized_size(q, r) <= NumericLimits<uint32_t>::Maximum();
}

// The value of a constant INTEGER parameter, between 1 and 63.
uint32_t BindQuotientParameter(ClientContext &context, Expression &argument, const char *name) {
	if (argument.HasParameter()) {
		throw ParameterNotResolvedException();
	}
	if (!argument.IsFoldable()) {
		throw BinderException("Quotient filter %s value can only be a constant", name);
	}
	Value value = ExpressionExecutor::EvaluateScalar(context, argument);
	if (value.IsNull()) {
		throw BinderException("Quotient filter %s cannot be NULL", name);
	}
	// The signature declares the parameters as INTEGER, so the binder has
	// already cast them; only the range is left to check.
	const auto result = value.GetValue<int32_t>();
	if (result <= 0 || result >= 64) {
		throw BinderException("Quotient filter %s must be between 1 and 63, got %d", name, result);
	}
	return static_cast<uint32_t>(result);
}

struct QuotientFilterBindData : public BitfiltersKeyBindData {
	//! q = 0 for the self-sizing aggregate, which picks q when it finalizes
	QuotientFilterBindData(BitfilterKeyFormat keys_p, uint32_t q_p, uint32_t r_p)
	    : BitfiltersKeyBindData(keys_p), q(q_p), r(r_p) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<QuotientFilterBindData>(keys, q, r);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<QuotientFilterBindData>();
		return keys == other.keys && q == other.q && r == other.r;
	}

	uint32_t q;
	uint32_t r;
};

// quotient_filter(q, r, value) and quotient_filter(q, r, filter). The q and r
// arguments stay in the argument list (DuckDB 2.0 no longer allows erasing
// arguments); the update callback skips them.
unique_ptr<FunctionData> QuotientFilterBind(BindAggregateFunctionInput &input) {
	auto &context = input.GetClientContext();
	auto &arguments = input.GetArguments();
	D_ASSERT(arguments.size() == 3);
	const auto q = BindQuotientParameter(context, *arguments[0], "q");
	const auto r = BindQuotientParameter(context, *arguments[1], "r");
	if (q + r > 64) {
		throw BinderException("Quotient filter q + r must be at most 64, got q = %u and r = %u", q, r);
	}
	// A filter this big can never be returned. Fail here rather than letting
	// the aggregate allocate a table of that size.
	if (!QuotientFilterFits(q, r)) {
		throw BinderException(
		    "Quotient filter with q = %u and r = %u would need %s, which exceeds the maximum BLOB size of %u bytes; "
		    "use a smaller q",
		    q, r,
		    q >= 35 ? string("more than 16GB")
		            : StringUtil::Format("%llu bytes", static_cast<uint64_t>(QuotientFilter::serialized_size(q, r))),
		    NumericLimits<uint32_t>::Maximum());
	}
	auto keys = BitfilterKeyFormat::ForValues(arguments.back()->GetReturnType(), true);
	return make_uniq<QuotientFilterBindData>(keys, q, r);
}

// quotient_filter(r, value): q is chosen from the input.
unique_ptr<FunctionData> QuotientFilterAutoBind(BindAggregateFunctionInput &input) {
	auto &context = input.GetClientContext();
	auto &arguments = input.GetArguments();
	D_ASSERT(arguments.size() == 2);
	const auto r = BindQuotientParameter(context, *arguments[0], "r");
	auto keys = BitfilterKeyFormat::ForValues(arguments.back()->GetReturnType(), true);
	return make_uniq<QuotientFilterBindData>(keys, 0, r);
}

// Writes `filter` with a header to `target` in `result`.
void WriteQuotientFilter(const QuotientFilter &filter, const BitfilterKeyFormat &keys, Vector &result,
                         string_t &target) {
	target = StringVector::EmptyString(result, BITFILTERS_HEADER_SIZE + filter.serialized_size());
	auto out = target.GetDataWriteable();
	BitfiltersWriteHeader(data_ptr_cast(out), BitfilterType::QUOTIENT, keys);
	filter.serialize_into(out + BITFILTERS_HEADER_SIZE);
	target.Finalize();
}

void CheckSameKeys(const BitfilterKeyFormat &target, const BitfilterKeyFormat &source) {
	if (target != source) {
		throw InvalidInputException("quotient_filter: cannot merge a filter built from %s values (hash: %s) into one "
		                            "built from %s values (hash: %s); their keys are not comparable",
		                            source.KeyTypeName(), source.HashName(), target.KeyTypeName(), target.HashName());
	}
}

struct QuotientFilterState {
	// Raw pointer rather than unique_ptr: aggregate states are handed to us as
	// uninitialized memory, so a smart pointer's assignment operator would run
	// over (and try to free) a garbage pointer.
	//
	// The filter's table comes from the aggregate's buffer-managed allocator
	// (see bitfilters_memory.hpp), so it counts against memory_limit; the
	// table remembers that allocator and frees itself through it.
	QuotientFilter *filter;
	//! How the filter's values became keys (set with the filter)
	BitfilterKeyFormat keys;
};

struct QuotientFilterOperationBase {
	template <class STATE>
	static void Initialize(STATE &state) {
		state.filter = nullptr;
		state.keys = BitfilterKeyFormat();
	}

	static bool IgnoreNull() {
		return true;
	}

	template <class STATE>
	static void Destroy(STATE &state, AggregateInputData &) {
		delete state.filter;
		state.filter = nullptr;
	}

	// Every state of one aggregate is created with the bound q and r.
	template <class STATE>
	static QuotientFilter &GetOrCreateFilter(STATE &state, AggregateInputData &input, const BitfilterKeyFormat &keys) {
		if (!state.filter) {
			auto &bind_data = input.bind_data->Cast<QuotientFilterBindData>();
			state.filter = new QuotientFilter(bind_data.q, bind_data.r, BitfiltersStateAllocator(input));
			state.keys = keys;
		} else {
			CheckSameKeys(state.keys, keys);
		}
		return *state.filter;
	}

	// Merging a partial state into another. Either side can still be empty: a
	// thread that saw no rows for a group never allocates a filter.
	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &input) {
		if (!source.filter) {
			return;
		}
		if (!target.filter) {
			if (input.combine_type == AggregateCombineType::ALLOW_DESTRUCTIVE) {
				// The source state is ours to take apart: take its filter.
				target.filter = source.filter;
				const_cast<STATE &>(source).filter = nullptr;
			} else {
				target.filter = new QuotientFilter(*source.filter, BitfiltersStateAllocator(input));
			}
			target.keys = source.keys;
			return;
		}
		// Both states have the bound q and r, so this only fails if the union
		// does not fit -- and then building the filter on one thread would
		// have failed too.
		CheckSameKeys(target.keys, source.keys);
		target.filter->merge_from(*source.filter);
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (!state.filter) {
			finalize_data.ReturnNull();
			return;
		}
		WriteQuotientFilter(*state.filter, state.keys, finalize_data.result, target);
	}
};

struct QuotientFilterMergeOperation : QuotientFilterOperationBase {
	template <class A_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const A_TYPE &a_data, AggregateUnaryInput &unary_input) {
		auto blob = BitfilterBlob::Expect(a_data, BitfilterType::QUOTIENT, "quotient_filter");
		// The constructor validates the payload and throws if it is not one of
		// ours. It reads the table in place, so this allocates nothing.
		QuotientFilter incoming(const_char_ptr_cast(blob.payload), blob.payload_size,
		                        BitfiltersStateAllocator(unary_input.input));
		GetOrCreateFilter(state, unary_input.input, blob.keys).merge_from(incoming);
	}

	// Merging the same filter again adds nothing.
	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input, idx_t) {
		Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
	}
};

struct QuotientFilterCreateOperation : QuotientFilterOperationBase {
	template <class A_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const A_TYPE &a_data, AggregateUnaryInput &unary_input) {
		auto &keys = unary_input.input.bind_data->Cast<QuotientFilterBindData>().keys;
		auto &filter = GetOrCreateFilter(state, unary_input.input, keys);
		if (!filter.insert(a_data)) {
			throw InvalidInputException("quotient_filter: the filter is full (q = %u holds at most %llu values); "
			                            "increase q to hold more values",
			                            filter.q(), filter.capacity());
		}
	}

	// Inserting the same value again adds nothing.
	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input, idx_t) {
		Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
	}
};

// quotient_filter(r, value): collects the keys (spillably, like the xor and
// binary fuse filters) and builds a filter just large enough for them, so
// nobody has to guess q. The distinct keys fill at most 3/4 of the slots,
// past which quotient filter clusters grow long and lookups slow down.
struct QuotientFilterAutoOperation : BitfiltersKeyCollectOperation {
	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (state.keys.total_capacity == 0) {
			finalize_data.ReturnNull();
			return;
		}
		auto &bind_data = finalize_data.input.bind_data->Cast<QuotientFilterBindData>();
		auto &allocator = BitfiltersStateAllocator(finalize_data.input);

		auto key_array = GatherKeys(state.keys, allocator);
		auto keys = reinterpret_cast<uint64_t *>(key_array.get());
		const auto distinct = SortUnique(keys, state.keys.total_capacity);
		const auto end = keys + distinct;

		uint32_t q = 1;
		while ((1ULL << q) / 4 * 3 < distinct) {
			q++;
		}
		const auto r = bind_data.r;
		if (q + r > 64) {
			throw InvalidInputException("quotient_filter: %llu distinct values need q = %u, and q + r = %u exceeds 64; "
			                            "use a smaller r",
			                            distinct, q, q + r);
		}
		if (!QuotientFilterFits(q, r)) {
			throw OutOfRangeException("quotient_filter: %llu distinct values need q = %u, and the filter would exceed "
			                          "the maximum BLOB size of %u bytes",
			                          distinct, q, NumericLimits<uint32_t>::Maximum());
		}

		QuotientFilter filter(q, r, allocator);
		for (auto key = keys; key != end; key++) {
			// Never fails: the filter has room for every distinct key, and
			// keys that share a fingerprint are only stored once.
			filter.insert(*key);
		}
		WriteQuotientFilter(filter, bind_data.keys, finalize_data.result, target);
	}
};

AggregateFunction QuotientFilterMergeAggregate() {
	return BitfiltersUnaryAggregateWithLeadingArgs<2, QuotientFilterState, string_t, string_t,
	                                               QuotientFilterMergeOperation, AggregateDestructorType::LEGACY>(
	    {LogicalType::INTEGER, LogicalType::INTEGER}, LogicalType::BLOB, LogicalType::BLOB);
}

AggregateFunction QuotientFilterCreateAggregate() {
	return BitfiltersUnaryAggregateWithLeadingArgs<2, QuotientFilterState, uint64_t, string_t,
	                                               QuotientFilterCreateOperation, AggregateDestructorType::LEGACY,
	                                               true>({LogicalType::INTEGER, LogicalType::INTEGER}, LogicalType::ANY,
	                                                     LogicalType::BLOB);
}

class QuotientFilterProbe : public BitfilterProbeBase<QuotientFilterProbe> {
public:
	QuotientFilterProbe(const_data_ptr_t payload, idx_t size, Allocator &allocator)
	    : filter(const_char_ptr_cast(payload), size, allocator) {
	}

	bool ContainsKey(uint64_t key) const {
		return filter.may_contain(key);
	}

	void Describe(BitfilterInfo &info) const override {
		const auto slots = static_cast<double>(1ULL << filter.q());
		info.entries = filter.entries();
		info.capacity = filter.capacity();
		info.load_factor = static_cast<double>(filter.entries()) / slots;
		info.fingerprint_bits = static_cast<uint8_t>(filter.r());
		// A lookup can only match a stored remainder in its own run.
		info.expected_fpr = 1.0 - std::exp(-info.load_factor * std::ldexp(1.0, -static_cast<int>(filter.r())));
		info.q = static_cast<uint8_t>(filter.q());
		info.r = static_cast<uint8_t>(filter.r());
	}

private:
	QuotientFilter filter;
};

void QuotientFilterContains(DataChunk &args, ExpressionState &state, Vector &result) {
	static const BitfilterType type = BitfilterType::QUOTIENT;
	BitfiltersContains(args, state, result, &type, "quotient_filter_contains");
}

} // namespace

unique_ptr<BitfilterProbe> QuotientFilterOpen(const_data_ptr_t payload, idx_t size, Allocator &allocator,
                                              const char *) {
	return make_uniq<QuotientFilterProbe>(payload, size, allocator);
}

void LoadQuotientFilter(ExtensionLoader &loader) {
	{
		AggregateFunctionSet quotient_filter("quotient_filter");
		{
			auto fun = QuotientFilterCreateAggregate();
			fun.SetBindCallback(QuotientFilterBind);
			fun.SetFallible();
			quotient_filter.AddFunction(fun);
		}
		{
			auto fun = QuotientFilterMergeAggregate();
			fun.SetBindCallback(QuotientFilterBind);
			fun.SetFallible();
			quotient_filter.AddFunction(fun);
		}
		{
			auto fun = BitfiltersKeyAggregate<QuotientFilterAutoOperation>({LogicalType::INTEGER}, LogicalType::ANY,
			                                                               LogicalType::BLOB, QuotientFilterAutoBind);
			fun.SetFallible();
			quotient_filter.AddFunction(fun);
		}

		CreateAggregateFunctionInfo info(quotient_filter);
		{
			FunctionDescription desc;
			desc.description = "Creates a Quotient filter from values of any type: UBIGINT values are used as keys as "
			                   "they are, other values are hashed with XXH64 of a fixed encoding that does not change "
			                   "between DuckDB versions. q is the number of quotient bits (the filter holds at most "
			                   "2^q - 1 values) and r the number of remainder bits (false positive rate ~2^-r). q + r "
			                   "must be <= 64. Returns a BLOB to probe with quotient_filter_contains.";
			desc.parameter_names = {"q", "r", "value"};
			desc.parameter_types = {LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::ANY};
			desc.examples.push_back("SELECT quotient_filter(16, 8, column) FROM table");
			desc.categories = {"bitfilters", "probabilistic"};
			info.descriptions.push_back(desc);
		}
		{
			// The merge overload takes filter BLOBs instead of values; duckdb_functions()
			// only attaches a description whose parameter_types match the overload.
			FunctionDescription desc;
			desc.description = "Merges Quotient filters (BLOBs produced by quotient_filter) into a single filter "
			                   "containing every value of the inputs. q and r size the merged filter, which holds at "
			                   "most 2^q - 1 values. Each input must keep at least q + r bits of each hash (its own "
			                   "q + r must be >= q + r), so a merge can grow q only by giving up as many remainder "
			                   "bits. The inputs must be built from the same type of values. q + r must be <= 64.";
			desc.parameter_names = {"q", "r", "filter"};
			desc.parameter_types = {LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::BLOB};
			desc.examples.push_back("SELECT quotient_filter(16, 8, filter) FROM filters");
			desc.categories = {"bitfilters", "probabilistic"};
			info.descriptions.push_back(desc);
		}
		{
			FunctionDescription desc;
			desc.description = "Creates a Quotient filter sized to fit its input: q is chosen so that the distinct "
			                   "values fill at most 3/4 of the 2^q slots. r is the number of remainder bits (false "
			                   "positive rate ~2^-r). Values are handled as by quotient_filter(q, r, value).";
			desc.parameter_names = {"r", "value"};
			desc.parameter_types = {LogicalType::INTEGER, LogicalType::ANY};
			desc.examples.push_back("SELECT quotient_filter(8, column) FROM table");
			desc.categories = {"bitfilters", "probabilistic"};
			info.descriptions.push_back(desc);
		}
		BitfiltersApplyParameterNames(info.functions, info.descriptions);
		loader.RegisterFunction(info);
	}

	{
		ScalarFunctionSet fs("quotient_filter_contains");
		fs.AddFunction(
		    ScalarFunction({LogicalType::BLOB, LogicalType::ANY}, LogicalType::BOOLEAN, QuotientFilterContains));
		fs.SetFallible();
		CreateScalarFunctionInfo info(std::move(fs));
		{
			FunctionDescription desc;
			desc.description = "Tests if a Quotient filter may contain a value. Returns true if the value "
			                   "might be in the set (with possible false positives), or false if the value "
			                   "is definitely not in the set (no false negatives).";
			desc.parameter_names = {"filter", "value"};
			desc.parameter_types = {LogicalType::BLOB, LogicalType::ANY};
			desc.examples.push_back("SELECT quotient_filter_contains(filter, 42) FROM table");
			desc.categories = {"bitfilters", "probabilistic"};
			info.descriptions.push_back(desc);
		}
		BitfiltersApplyParameterNames(info.functions, info.descriptions);
		loader.RegisterFunction(info);
	}
}

} // namespace duckdb
