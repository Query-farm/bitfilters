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
	// FIXME: should check the type
	q.CastAs(context, LogicalType::UINTEGER);
	auto actual_q = q.GetValue<uint32_t>();
	if (actual_q == 0) {
		throw BinderException("Quotient filter capacity must be greater than 0");
	}

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
	// FIXME: should check the type
	r.CastAs(context, LogicalType::UINTEGER);
	auto actual_r = r.GetValue<uint32_t>();
	if (actual_r == 0) {
		throw BinderException("Quotient filter r must be greater than 0");
	}
	if (actual_r + actual_q > 64) {
		throw BinderException("Quotient filter q+r must be between 0 and 64");
	}

	// A filter this big can never be returned: DuckDB BLOBs top out at 4GB. Fail
	// here rather than letting the aggregate allocate a table of that size and
	// take the process down with it.
	const uint64_t filter_bytes =
	    sizeof(QuotientFilterValues) + (((1ULL << actual_q) * (uint64_t)(actual_r + 3)) + 7) / 8;
	if (filter_bytes > NumericLimits<uint32_t>::Maximum()) {
		throw BinderException("Quotient filter with q = %u and r = %u would need %llu bytes, which exceeds the maximum "
		                      "BLOB size of %u bytes; use a smaller q",
		                      actual_q, actual_r, filter_bytes, NumericLimits<uint32_t>::Maximum());
	}

	// The q and r arguments stay in the argument list (DuckDB 2.0 no longer allows
	// erasing arguments); the update callback skips them.

	return make_uniq<QuotientFilterBindData>(actual_q, actual_r);
}

template <class T>
struct QuotientFilterState {
	// Raw pointer rather than unique_ptr: aggregate states are handed to us as
	// uninitialized memory, so a smart pointer's assignment operator would run
	// over (and try to free) a garbage pointer.
	QuotientFilter *filter;

	void CreateFilter(uint32_t q, uint32_t r) {
		D_ASSERT(!filter);
		filter = new QuotientFilter(q, r);
	}

	void CreateFilter(const QuotientFilterState &existing) {
		D_ASSERT(!filter);
		if (existing.filter) {
			filter = new QuotientFilter(*existing.filter);
		}
	}

	std::string serialize() const {
		D_ASSERT(filter != nullptr);
		return filter->serialize();
	}
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
	static void Destroy(STATE &state, AggregateInputData &aggr_input_data) {
		delete state.filter;
		state.filter = nullptr;
	}

	// Merging a partial state into another. Either side can still be empty: a
	// thread that saw no rows for a group never allocates a filter, and
	// dereferencing that was a null deref.
	template <class STATE>
	static void CombineStates(const STATE &source, STATE &target) {
		if (!source.filter) {
			return;
		}
		if (!target.filter) {
			target.CreateFilter(source);
		} else {
			*target.filter = target.filter->merge(*source.filter);
		}
	}
};

template <class BIND_DATA_TYPE>
struct QuotientFilterMergeOperation : QuotientFilterOperationBase {
	template <class A_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const A_TYPE &a_data, AggregateUnaryInput &idata) {
		if (!state.filter) {
			auto &bind_data = idata.input.bind_data->template Cast<BIND_DATA_TYPE>();
			state.CreateFilter(bind_data.q, bind_data.r);
		}

		// this is a filter in b_data, so we need to deserialize it. The
		// constructor validates the BLOB and throws if it is not one of ours.
		QuotientFilter incoming(a_data.GetData(), a_data.GetSize());
		*state.filter = state.filter->merge(incoming);
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
	                              idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &aggr_input_data) {
		QuotientFilterOperationBase::CombineStates(source, target);
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (!state.filter) {
			finalize_data.ReturnNull();
		} else {
			target = StringVector::AddStringOrBlob(finalize_data.result, state.serialize());
		}
	}
};

template <class BIND_DATA_TYPE>
struct QuotientFilterCreateOperation : QuotientFilterOperationBase {
	template <class A_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const A_TYPE &a_data, AggregateUnaryInput &idata) {
		auto &bind_data = idata.input.bind_data->template Cast<BIND_DATA_TYPE>();
		if (!state.filter) {
			state.CreateFilter(bind_data.q, bind_data.r);
		}
		auto result = state.filter->insert(a_data);
		if (!result) {
			throw InvalidInputException(
			    "quotient_filter: the filter is full (q = %u gives it %llu slots); increase q to hold more values",
			    bind_data.q, 1ULL << bind_data.q);
		}
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
	                              idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &aggr_input_data) {
		QuotientFilterOperationBase::CombineStates(source, target);
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (!state.filter) {
			finalize_data.ReturnNull();
		} else {
			target = StringVector::AddStringOrBlob(finalize_data.result, state.serialize());
		}
	}
};

template <typename T>
auto static QuotientFilterMergeAggregate(const LogicalType &type, const LogicalType &result_type) -> AggregateFunction

{
	return BitfiltersUnaryAggregateWithLeadingArgs<2, QuotientFilterState<T>, string_t, string_t,
	                                               QuotientFilterMergeOperation<QuotientFilterBindData>,
	                                               AggregateDestructorType::LEGACY>(
	    {LogicalType::INTEGER, LogicalType::INTEGER}, result_type, result_type);
}

template <typename T>
auto static QuotientFilterCreateAggregate(const LogicalType &type, const LogicalType &result_type)
    -> AggregateFunction {
	return BitfiltersUnaryAggregateWithLeadingArgs<2, QuotientFilterState<T>, T, string_t,
	                                               QuotientFilterCreateOperation<QuotientFilterBindData>,
	                                               AggregateDestructorType::LEGACY>(
	    {LogicalType::INTEGER, LogicalType::INTEGER}, type, result_type);
}

template <class T>

static inline void QuotientFilterContains(DataChunk &args, ExpressionState &state, Vector &result) {
	// Get the references to the incoming vectors.
	D_ASSERT(args.ColumnCount() == 2);

	auto &filter_vector = args.data[0];
	auto &item_vector = args.data[1];

	if (filter_vector.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		if (ConstantVector::IsNull(filter_vector)) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(result, true);
			return;
		}
		auto &blob = ConstantVector::GetData<string_t>(filter_vector)[0];
		// The constructor validates the BLOB and throws if it is not one of ours.
		QuotientFilter filter(blob.GetData(), blob.GetSize());
		UnaryExecutor::Execute<T, bool>(item_vector, result,
		                                [&](T item_data) { return filter.may_contain(item_data); });
		return;
	}

	// Per-row filters. Deserializing copies the whole table, and the same filter
	// is usually repeated across many rows (a join against a table of filters),
	// so remember the last blob we decoded. Only blobs that passed validation
	// are cached, and those are always longer than the 12 byte inline threshold,
	// so their data pointers are stable heap addresses rather than pointers into
	// a by-value string_t on the stack.
	unique_ptr<QuotientFilter> cached_filter;
	const char *cached_data = nullptr;
	uint32_t cached_size = 0;
	BinaryExecutor::Execute<string_t, T, bool>(
	    filter_vector, item_vector, result, [&](string_t filter_data, T item_data) {
		    if (!cached_filter || filter_data.GetData() != cached_data || filter_data.GetSize() != cached_size) {
			    cached_filter = make_uniq<QuotientFilter>(filter_data.GetData(), filter_data.GetSize());
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
			                   "containing every value of the inputs. q and r size the merged filter; it uses the "
			                   "larger of these and each input's quotient and remainder bits, so q must leave room "
			                   "for all merged values. q + r must be <= 64.";
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
