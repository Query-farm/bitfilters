#pragma once

#include "duckdb.hpp"
#include "duckdb/function/aggregate_function.hpp"

namespace duckdb {

// DuckDB 2.0 removed Function::EraseArgument: constant arguments folded into the
// bind data stay part of the argument list and are still passed to the update
// callback. These helpers build a unary aggregate whose value argument follows
// LEADING constant arguments, and whose update skips those leading arguments.

template <idx_t LEADING, class STATE, class INPUT_TYPE, class OP>
static void BitfiltersSkipLeadingScatterUpdate(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count,
                                               Vector &states, idx_t count) {
	D_ASSERT(input_count > LEADING);
	AggregateExecutor::UnaryScatter<STATE, INPUT_TYPE, OP>(inputs[LEADING], states, aggr_input_data, count);
}

template <idx_t LEADING, class STATE, class INPUT_TYPE, class RESULT_TYPE, class OP,
          AggregateDestructorType destructor_type = AggregateDestructorType::STANDARD>
static AggregateFunction BitfiltersUnaryAggregateWithLeadingArgs(const vector<LogicalType> &leading_types,
                                                                 const LogicalType &input_type,
                                                                 const LogicalType &return_type) {
	D_ASSERT(leading_types.size() == LEADING);
	vector<LogicalType> arguments = leading_types;
	arguments.push_back(input_type);
	AggregateFunction result(
	    arguments, return_type, AggregateFunction::StateSize<STATE>,
	    AggregateFunction::StateInitialize<STATE, OP, destructor_type>,
	    BitfiltersSkipLeadingScatterUpdate<LEADING, STATE, INPUT_TYPE, OP>, AggregateFunction::StateCombine<STATE, OP>,
	    AggregateFunction::StateFinalize<STATE, RESULT_TYPE, OP>, FunctionNullHandling::DEFAULT_NULL_HANDLING);
	result.SetStateDestructorCallback(AggregateFunction::StateDestroy<STATE, OP>);
	return result;
}

} // namespace duckdb
