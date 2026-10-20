#pragma once

#include "duckdb.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/parser/parsed_data/create_function_info.hpp"

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

// DuckDB 2.0 takes the parameter names shown by duckdb_functions() from each
// overload's signature, and a plain type list names them col0, col1, ...
// FunctionDescription.parameter_names alone no longer reaches the output.
// This names every overload in the set from the description whose
// parameter_types match that overload's (non-variadic) parameters, so the
// names live in one place. It throws if an overload has no matching
// description, which would otherwise leave it undocumented in
// duckdb_functions() (descriptions only attach to overloads whose types match).
template <class FUNCTION_SET>
static void BitfiltersApplyParameterNames(FUNCTION_SET &set, const vector<FunctionDescription> &descriptions) {
	set.ApplyToFunctions([&](auto &fun) {
		auto &signature = fun.GetSignature();
		vector<idx_t> positional;
		for (idx_t i = 0; i < signature.GetParameterCount(); i++) {
			if (!signature.GetParameter(i).IsVariadic()) {
				positional.push_back(i);
			}
		}
		for (auto &desc : descriptions) {
			if (desc.parameter_types.size() != positional.size() || desc.parameter_names.size() != positional.size()) {
				continue;
			}
			bool matches = true;
			for (idx_t i = 0; i < positional.size(); i++) {
				if (desc.parameter_types[i] != signature.GetParameter(positional[i]).GetType()) {
					matches = false;
					break;
				}
			}
			if (!matches) {
				continue;
			}
			for (idx_t i = 0; i < positional.size(); i++) {
				signature.GetParameter(positional[i]).SetName(Identifier(desc.parameter_names[i]));
			}
			return;
		}
		throw InternalException("bitfilters: no FunctionDescription matches overload %s of %s", signature.ToString(),
		                        set.name);
	});
}

} // namespace duckdb
