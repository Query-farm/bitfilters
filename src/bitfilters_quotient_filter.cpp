#include "bitfilters_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
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

unique_ptr<FunctionData> QuotientFilterBind(ClientContext &context, AggregateFunction &function,
                                            vector<unique_ptr<Expression>> &arguments) {

	if (arguments.size() == 1) {
		return nullptr;
	}
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
	if (actual_r + actual_q > 64) {
		throw BinderException("Quotient filter q+r must be between 0 and 64");
	}

	// So doing this causes problems.
	Function::EraseArgument(function, arguments, 0);
	Function::EraseArgument(function, arguments, 0);

	return make_uniq<QuotientFilterBindData>(actual_q, actual_r);
}

template <class T>
struct QuotientFilterState {

	std::unique_ptr<QuotientFilter> filter = nullptr;

	QuotientFilterState() = delete;

	explicit QuotientFilterState(const string_t &data) {
		filter = make_uniq<QuotientFilter>(data.GetDataUnsafe(), data.GetSize());
	}

	explicit QuotientFilterState(const std::string &data) {
		filter = make_uniq<QuotientFilter>(data);
	}

	void CreateFilter(uint32_t q, uint32_t r) {
		D_ASSERT(!filter);
		filter = make_uniq<QuotientFilter>(q, r);
	}

	void CreateFilter(const QuotientFilterState &existing) {
		if (existing.filter) {
			filter = make_uniq<QuotientFilter>(*existing.filter);
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
		state.filter.release();
		state.filter = nullptr;
	}

	static bool IgnoreNull() {
		return true;
	}

	template <class STATE>
	static void Destroy(STATE &state, AggregateInputData &aggr_input_data) {
		if (state.filter) {
			state.filter = nullptr;
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

		// this is a filter in b_data, so we need to deserialize it.
		*state.filter = state.filter->merge(*(STATE(a_data.GetString()).filter));
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
		if (!target.filter) {
			target.CreateFilter(source);
		} else {
			*target.filter = target.filter->merge(*source.filter);
		}
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
		if (!state.filter) {
			auto &bind_data = idata.input.bind_data->template Cast<BIND_DATA_TYPE>();
			state.CreateFilter(bind_data.q, bind_data.r);
		}
		auto result = state.filter->insert(a_data);
		if (!result) {
			throw IOException("Failed to insert item into quotient filter, likely it is full");
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
		if (!target.filter) {
			target.CreateFilter(source);
		} else {
			*target.filter = target.filter->merge(*source.filter);
		}
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
	return AggregateFunction::UnaryAggregateDestructor<QuotientFilterState<T>, string_t, string_t,
	                                                   QuotientFilterMergeOperation<QuotientFilterBindData>,
	                                                   AggregateDestructorType::LEGACY>(result_type, result_type);
}

template <typename T>
auto static QuotientFilterCreateAggregate(const LogicalType &type, const LogicalType &result_type)
    -> AggregateFunction {

	return AggregateFunction::UnaryAggregateDestructor<QuotientFilterState<T>, T, string_t,
	                                                   QuotientFilterCreateOperation<QuotientFilterBindData>,
	                                                   AggregateDestructorType::LEGACY>(type, result_type);
}

template <class T>

static inline void QuotientFilterContains(DataChunk &args, ExpressionState &state, Vector &result) {
	// Get the references to the incoming vectors.
	D_ASSERT(args.ColumnCount() == 2);

	auto &filter_vector = args.data[0];
	auto &item_vector = args.data[1];

	if (filter_vector.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		auto state = QuotientFilterState<T>(ConstantVector::GetData<string_t>(filter_vector)[0]);
		UnaryExecutor::Execute<T, bool>(item_vector, result, args.size(),
		                                [&](T item_data) { return state.filter->may_contain(item_data); });
	} else {
		BinaryExecutor::Execute<string_t, T, bool>(filter_vector, item_vector, result, args.size(),
		                                           [&](string_t filter_data, T item_data) {
			                                           auto state = QuotientFilterState<T>(filter_data);

			                                           return state.filter->may_contain(item_data);
		                                           });
	}
}

// Helper function to register Quotient filter functions for a specific type
template <typename T>
static void RegisterQuotientFilterFunctionsForType(AggregateFunctionSet &Quotientfilter, const LogicalType &type) {
	// Register create aggregate function
	{
		auto fun = QuotientFilterCreateAggregate<T>(type, LogicalType::BLOB);
		fun.bind = QuotientFilterBind;
		fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
		fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
		Quotientfilter.AddFunction(fun);
	}

	// Register merge aggregate function
	{
		auto fun = QuotientFilterMergeAggregate<T>(type, LogicalType::BLOB);
		fun.bind = QuotientFilterBind;
		fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
		fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
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
			desc.description = "Creates a Quotient filter by aggregating values or by merging other Quotient filters. "
			                   "Takes q and r as number of bits.";
			desc.examples.push_back("SELECT quotient_filter(16, 8, column) FROM table");
			Quotientfilter_create_info.descriptions.push_back(desc);
		}

		loader.RegisterFunction(Quotientfilter_create_info);
	}

	// Register scalar functions
	{
		ScalarFunctionSet fs("quotient_filter_contains");

		RegisterQuotientFilterContainsForType<uint64_t>(fs, LogicalType::UBIGINT);

		CreateScalarFunctionInfo info(std::move(fs));

		{
			FunctionDescription desc;
			desc.description = "Tests if a Quotient filter may contain a value. Returns true if the value "
			                   "might be in the set (with possible false positives), or false if the value "
			                   "is definitely not in the set (no false negatives).";
			desc.examples.push_back("SELECT quotient_filter_contains(filter, 42) FROM table");
			info.descriptions.push_back(desc);
		}

		loader.RegisterFunction(info);
	}
}

} // namespace duckdb