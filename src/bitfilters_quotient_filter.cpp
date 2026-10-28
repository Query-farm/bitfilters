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

namespace duckdb {

namespace {

struct QuotientFilterBindData : public FunctionData {
	QuotientFilterBindData() {
	}
	explicit QuotientFilterBindData(uint32_t q, uint32_t r) : q(q), r(r) {
		// Validate parameters during construction
		if (q == 0) {
			throw InvalidInputException("Quotient filter q must be > 0");
		}
		if (r == 0) {
			throw InvalidInputException("Quotient filter r must be > 0");
		}
		if (q + r > 64) {
			throw InvalidInputException("Quotient filter q + r must be <= 64");
		}
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<QuotientFilterBindData>(q, r);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<QuotientFilterBindData>();
		return q == other.q && r == other.r;
	}

	uint32_t q;

	uint32_t r;
};

unique_ptr<FunctionData> QuotientFilterBind(BindAggregateFunctionInput &input) {
	auto &context = input.GetClientContext();
	auto &arguments = input.GetArguments();
	if (arguments.size() > 3) {
		throw BinderException("Quotient filter aggregate function requires exactly 3 arguments");
	}
	if (arguments[0]->HasParameter()) {
		throw ParameterNotResolvedException();
	}
	if (!arguments[0]->IsFoldable()) {
		throw BinderException("Quotient filter q value can only be a constant");
	}
	Value q = ExpressionExecutor::EvaluateScalar(context, *arguments[0]);
	if (q.IsNull()) {
		throw BinderException("Quotient filter q cannot be NULL");
	}
	// The signature declares q and r as INTEGER, so the binder has already
	// cast them; only the range is left to check.
	const auto q_value = q.GetValue<int32_t>();
	if (q_value <= 0 || q_value >= 64) {
		throw BinderException("Quotient filter q must be between 1 and 63, got %d", q_value);
	}
	const auto actual_q = static_cast<uint32_t>(q_value);

	if (arguments[1]->HasParameter()) {
		throw ParameterNotResolvedException();
	}
	if (!arguments[1]->IsFoldable()) {
		throw BinderException("Quotient filter r value can only be a constant");
	}
	Value r = ExpressionExecutor::EvaluateScalar(context, *arguments[1]);
	if (r.IsNull()) {
		throw BinderException("Quotient filter r cannot be NULL");
	}
	const auto r_value = r.GetValue<int32_t>();
	if (r_value <= 0 || r_value >= 64) {
		throw BinderException("Quotient filter r must be between 1 and 63, got %d", r_value);
	}
	const auto actual_r = static_cast<uint32_t>(r_value);
	if (actual_r + actual_q > 64) {
		throw BinderException("Quotient filter q + r must be at most 64, got q = %u and r = %u", actual_q, actual_r);
	}

	// A filter this big can never be returned: DuckDB BLOBs top out at 4GB. Fail
	// here rather than letting the aggregate allocate a table of that size.
	// Any q >= 35 is over the limit (2^35 slots of at least 4 bits each), and
	// rejecting it first keeps the size computation from overflowing.
	if (actual_q >= 35 || QuotientFilter::serialized_size(actual_q, actual_r) > NumericLimits<uint32_t>::Maximum()) {
		throw BinderException(
		    "Quotient filter with q = %u and r = %u would need %s, which exceeds the maximum BLOB "
		    "size of %u bytes; use a smaller q",
		    actual_q, actual_r,
		    actual_q >= 35 ? string("more than 16GB")
		                   : StringUtil::Format("%llu bytes", static_cast<uint64_t>(
		                                                          QuotientFilter::serialized_size(actual_q, actual_r))),
		    NumericLimits<uint32_t>::Maximum());
	}

	// The q and r arguments stay in the argument list (DuckDB 2.0 no longer allows
	// erasing arguments); the update callback skips them.

	return make_uniq<QuotientFilterBindData>(actual_q, actual_r);
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
};

struct QuotientFilterOperationBase {
	template <class STATE>
	static void Initialize(STATE &state) {
		state.filter = nullptr;
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
	static QuotientFilter &GetOrCreateFilter(STATE &state, AggregateInputData &input) {
		if (!state.filter) {
			auto &bind_data = input.bind_data->Cast<QuotientFilterBindData>();
			state.filter = new QuotientFilter(bind_data.q, bind_data.r, BitfiltersStateAllocator(input));
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
			return;
		}
		// Both states have the bound q and r, so this only fails if the union
		// does not fit -- and then building the filter on one thread would
		// have failed too.
		target.filter->merge_from(*source.filter);
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (!state.filter) {
			finalize_data.ReturnNull();
			return;
		}
		target = StringVector::EmptyString(finalize_data.result, state.filter->serialized_size());
		state.filter->serialize_into(target.GetDataWriteable());
		target.Finalize();
	}
};

struct QuotientFilterMergeOperation : QuotientFilterOperationBase {
	template <class A_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const A_TYPE &a_data, AggregateUnaryInput &unary_input) {
		auto &filter = GetOrCreateFilter(state, unary_input.input);
		// The constructor validates the BLOB and throws if it is not one of
		// ours. It reads the table in place, so this allocates nothing.
		QuotientFilter incoming(a_data.GetData(), a_data.GetSize(), BitfiltersStateAllocator(unary_input.input));
		filter.merge_from(incoming);
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
		auto &filter = GetOrCreateFilter(state, unary_input.input);
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

template <typename T>
auto static QuotientFilterMergeAggregate(const LogicalType &type, const LogicalType &result_type) -> AggregateFunction

{
	return BitfiltersUnaryAggregateWithLeadingArgs<2, QuotientFilterState, string_t, string_t,
	                                               QuotientFilterMergeOperation, AggregateDestructorType::LEGACY>(
	    {LogicalType::INTEGER, LogicalType::INTEGER}, result_type, result_type);
}

template <typename T>
auto static QuotientFilterCreateAggregate(const LogicalType &type, const LogicalType &result_type)
    -> AggregateFunction {
	return BitfiltersUnaryAggregateWithLeadingArgs<2, QuotientFilterState, T, string_t, QuotientFilterCreateOperation,
	                                               AggregateDestructorType::LEGACY>(
	    {LogicalType::INTEGER, LogicalType::INTEGER}, type, result_type);
}

template <class T>
static inline void QuotientFilterContains(DataChunk &args, ExpressionState &state, Vector &result) {
	// Get the references to the incoming vectors.
	D_ASSERT(args.ColumnCount() == 2);

	auto &filter_vector = args.data[0];
	auto &item_vector = args.data[1];

	// Filters read their table straight out of the BLOB; only filters written
	// before the table was padded to whole words are copied, and those copies
	// count against memory_limit.
	auto &allocator = BufferAllocator::Get(state.GetContext());

	if (filter_vector.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		if (ConstantVector::IsNull(filter_vector)) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(result, true);
			return;
		}
		auto &blob = ConstantVector::GetData<string_t>(filter_vector)[0];
		// The constructor validates the BLOB and throws if it is not one of ours.
		QuotientFilter filter(blob.GetData(), blob.GetSize(), allocator);
		UnaryExecutor::Execute<T, bool>(item_vector, result,
		                                [&](T item_data) { return filter.may_contain(item_data); });
		return;
	}

	// Per-row filters. The same filter is usually repeated across many rows (a
	// join against a table of filters), so remember the last blob we validated.
	// Only blobs that passed validation are cached, and those are always longer
	// than the 12 byte inline threshold, so their data pointers are stable heap
	// addresses rather than pointers into a by-value string_t on the stack.
	unique_ptr<QuotientFilter> cached_filter;
	const char *cached_data = nullptr;
	uint32_t cached_size = 0;
	BinaryExecutor::Execute<string_t, T, bool>(
	    filter_vector, item_vector, result, [&](string_t filter_data, T item_data) {
		    if (!cached_filter || filter_data.GetData() != cached_data || filter_data.GetSize() != cached_size) {
			    cached_filter = make_uniq<QuotientFilter>(filter_data.GetData(), filter_data.GetSize(), allocator);
			    cached_data = filter_data.GetData();
			    cached_size = filter_data.GetSize();
		    }
		    return cached_filter->may_contain(item_data);
	    });
}

// Helper function to register Quotient filter functions for a specific type
template <typename T>
static void RegisterQuotientFilterFunctionsForType(AggregateFunctionSet &Quotientfilter, const LogicalType &type) {
	// Register create aggregate function
	{
		auto fun = QuotientFilterCreateAggregate<T>(type, LogicalType::BLOB);
		fun.SetBindCallback(QuotientFilterBind);
		fun.SetFallible();
		Quotientfilter.AddFunction(fun);
	}

	// Register merge aggregate function
	{
		auto fun = QuotientFilterMergeAggregate<T>(type, LogicalType::BLOB);
		fun.SetBindCallback(QuotientFilterBind);
		fun.SetFallible();
		Quotientfilter.AddFunction(fun);
	}
}

// Helper function to register scalar contains functions for a specific type
template <typename T>
static void RegisterQuotientFilterContainsForType(ScalarFunctionSet &fs, const LogicalType &type) {
	fs.AddFunction(ScalarFunction({LogicalType::BLOB, type}, LogicalType::BOOLEAN, QuotientFilterContains<T>));
}
} // namespace

void LoadQuotientFilter(ExtensionLoader &loader) {
	// Register aggregate functions
	{
		AggregateFunctionSet Quotientfilter("quotient_filter");

		RegisterQuotientFilterFunctionsForType<uint64_t>(Quotientfilter, LogicalType::UBIGINT);

		CreateAggregateFunctionInfo Quotientfilter_create_info(Quotientfilter);

		{
			FunctionDescription desc;
			desc.description = "Creates a Quotient filter by aggregating UBIGINT values. "
			                   "q is the number of quotient bits (controls capacity, ~2^q slots) and r is the number "
			                   "of remainder bits (controls false positive rate, ~2^-r). q + r must be <= 64. "
			                   "Returns a BLOB to probe with quotient_filter_contains.";
			desc.parameter_names = {"q", "r", "value"};
			desc.parameter_types = {LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::UBIGINT};
			desc.examples.push_back("SELECT quotient_filter(16, 8, hash(column)) FROM table");
			desc.categories = {"bitfilters", "probabilistic"};
			Quotientfilter_create_info.descriptions.push_back(desc);
		}
		{
			// The merge overload takes filter BLOBs instead of values; duckdb_functions()
			// only attaches a description whose parameter_types match the overload.
			FunctionDescription desc;
			desc.description = "Merges Quotient filters (BLOBs produced by quotient_filter) into a single filter "
			                   "containing every value of the inputs. q and r size the merged filter, which holds at "
			                   "most 2^q - 1 values. Each input must keep at least q + r bits of each hash (its own "
			                   "q + r must be >= q + r), so a merge can grow q only by giving up as many remainder "
			                   "bits. q + r must be <= 64.";
			desc.parameter_names = {"q", "r", "filter"};
			desc.parameter_types = {LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::BLOB};
			desc.examples.push_back("SELECT quotient_filter(16, 8, filter) FROM filters");
			desc.categories = {"bitfilters", "probabilistic"};
			Quotientfilter_create_info.descriptions.push_back(desc);
		}

		BitfiltersApplyParameterNames(Quotientfilter_create_info.functions, Quotientfilter_create_info.descriptions);
		loader.RegisterFunction(Quotientfilter_create_info);
	}

	// Register scalar functions
	{
		ScalarFunctionSet fs("quotient_filter_contains");

		RegisterQuotientFilterContainsForType<uint64_t>(fs, LogicalType::UBIGINT);

		fs.SetFallible();
		CreateScalarFunctionInfo info(std::move(fs));

		{
			FunctionDescription desc;
			desc.description = "Tests if a Quotient filter may contain a value. Returns true if the value "
			                   "might be in the set (with possible false positives), or false if the value "
			                   "is definitely not in the set (no false negatives).";
			desc.parameter_names = {"filter", "value"};
			desc.parameter_types = {LogicalType::BLOB, LogicalType::UBIGINT};
			desc.examples.push_back("SELECT quotient_filter_contains(filter, 42) FROM table");
			desc.categories = {"bitfilters", "probabilistic"};
			info.descriptions.push_back(desc);
		}

		BitfiltersApplyParameterNames(info.functions, info.descriptions);
		loader.RegisterFunction(info);
	}
}

} // namespace duckdb
