#pragma once

#include "duckdb.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/parser/parsed_data/create_function_info.hpp"
#include "bitfilters_memory.hpp"
#include "duckdb/common/types/list_segment.hpp"
#include "duckdb/function/aggregate_state_layout.hpp"

namespace duckdb {

// State of an aggregate that has to see every key before it can build its
// filter (the xor and binary fuse filters are constructed in one shot).
//
// The keys are kept the way DuckDB's own list() keeps its values: in a linked
// list of segments allocated from the aggregate's arena. The arena sits on the
// buffer manager's allocator, so the keys count against memory_limit; the
// state owns nothing outside the arena, so it needs no destructor; and
// STATE_TYPE describes it as a LIST(UBIGINT), which lets the grouped hash
// aggregate export states to spillable storage when memory runs short.
struct BitfiltersKeyState {
	using STATE_TYPE = StateListType<StateTypedValue<uint64_t, StateInputType<0>>>;

	LinkedList keys;
};

// Update, combine and key extraction for BitfiltersKeyState. The filter
// specific operation derives from this and supplies Finalize.
struct BitfiltersKeyCollectOperation {
	template <class STATE>
	static void Initialize(STATE &state) {
		state.keys = LinkedList();
	}

	static bool IgnoreNull() {
		return true;
	}

	static ListSegmentFunctions KeyFunctions() {
		ListSegmentFunctions functions;
		GetSegmentDataFunctions(functions, LogicalType::UBIGINT);
		return functions;
	}

	// Appends rows [start, end) of `input` to `keys`, skipping NULLs. Runs of
	// valid rows are appended as one batch.
	static void AppendRange(const ListSegmentFunctions &functions, ArenaAllocator &allocator, LinkedList &keys,
	                        RecursiveUnifiedVectorFormat &input, idx_t start, idx_t end) {
		auto append = [&](idx_t run_start, idx_t run_end) {
			if (run_end > run_start) {
				allocator.AlignNext();
				functions.AppendRows(allocator, keys, input, run_start, run_end - run_start);
			}
		};
		auto &validity = input.unified.validity;
		if (validity.CannotHaveNull()) {
			append(start, end);
			return;
		}
		idx_t run_start = start;
		for (idx_t i = start; i < end; i++) {
			if (!validity.RowIsValid(input.unified.sel->get_index(i))) {
				append(run_start, i);
				run_start = i + 1;
			}
		}
		append(run_start, end);
	}

	static void ScatterUpdate(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count,
	                          Vector &state_vector, idx_t count) {
		D_ASSERT(input_count == 1);
		RecursiveUnifiedVectorFormat input;
		Vector::RecursiveToUnifiedFormat(inputs[0], input);
		const auto functions = KeyFunctions();

		if (state_vector.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			// a single state (an ungrouped aggregate): append the whole chunk
			auto &state = *ConstantVector::GetData<BitfiltersKeyState *>(state_vector)[0];
			AppendRange(functions, aggr_input_data.allocator, state.keys, input, 0, count);
			return;
		}
		auto states = state_vector.Values<BitfiltersKeyState *>();
		for (idx_t i = 0; i < count; i++) {
			AppendRange(functions, aggr_input_data.allocator, states[i].GetValue()->keys, input, i, i + 1);
		}
	}

	static void ClusterUpdate(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count,
	                          const ClusteredAggr &clustered, idx_t) {
		D_ASSERT(input_count == 1);
		RecursiveUnifiedVectorFormat input;
		Vector::RecursiveToUnifiedFormat(inputs[0], input);
		const auto functions = KeyFunctions();

		for (auto &run : clustered.runs()) {
			auto &state = *reinterpret_cast<BitfiltersKeyState *>(run.state);
			if (!run.sel) {
				// a contiguous run covering [0, run.count)
				AppendRange(functions, aggr_input_data.allocator, state.keys, input, 0, run.count);
				continue;
			}
			for (idx_t k = 0; k < run.count; k++) {
				AppendRange(functions, aggr_input_data.allocator, state.keys, input, run.sel[k], run.sel[k] + 1);
			}
		}
	}

	// Appends a copy of every key of `source` to `target`.
	static void CopyKeys(const ListSegmentFunctions &functions, ArenaAllocator &allocator, const LinkedList &source,
	                     LinkedList &target) {
		ListSegmentScanState scan_state;
		functions.InitializeScan(source, scan_state);
		Vector chunk(LogicalType::UBIGINT, STANDARD_VECTOR_SIZE);
		for (;;) {
			const auto scanned = functions.Scan(scan_state, chunk);
			if (!scanned) {
				break;
			}
			RecursiveUnifiedVectorFormat chunk_data;
			Vector::RecursiveToUnifiedFormat(chunk, chunk_data);
			allocator.AlignNext();
			functions.AppendRows(allocator, target, chunk_data, 0, scanned);
		}
	}

	static void Combine(Vector &source_vector, Vector &target_vector, AggregateInputData &aggr_input_data,
	                    idx_t count) {
		auto sources = source_vector.Values<BitfiltersKeyState *>();
		auto targets = target_vector.Values<BitfiltersKeyState *>();
		const auto functions = KeyFunctions();
		const bool destructive = aggr_input_data.combine_type == AggregateCombineType::ALLOW_DESTRUCTIVE;

		// A filter is a set, so combining a state more than once adds nothing;
		// only a multiplicity of zero (skip the state) matters.
		UnifiedVectorFormat multiplicities;
		const int64_t *multiplicity_data = nullptr;
		if (aggr_input_data.combine_multiplicities) {
			aggr_input_data.combine_multiplicities->ToUnifiedFormat(multiplicities);
			multiplicity_data = UnifiedVectorFormat::GetData<int64_t>(multiplicities);
		}

		for (idx_t i = 0; i < count; i++) {
			if (multiplicity_data) {
				const auto idx = multiplicities.sel->get_index(i);
				if (!multiplicities.validity.RowIsValid(idx) || multiplicity_data[idx] == 0) {
					continue;
				}
				if (multiplicity_data[idx] < 0) {
					throw InvalidInputException("combine_aggr multiplicity must be non-negative");
				}
			}
			auto &source = sources[i].GetValue()->keys;
			auto &target = targets[i].GetValue()->keys;
			if (source.total_capacity == 0) {
				continue;
			}
			if (!destructive) {
				CopyKeys(functions, aggr_input_data.allocator, source, target);
				continue;
			}
			// Both lists live in arenas that outlive the combined state, so
			// the source's segments can simply be linked onto the target.
			if (target.total_capacity == 0) {
				target = source;
			} else {
				target.last_segment->next = source.first_segment;
				target.last_segment = source.last_segment;
				target.total_capacity += source.total_capacity;
			}
			source = LinkedList();
		}
	}

	// Copies the state's keys into one contiguous array, which the filter
	// constructions need (they also reorder it in place). The array comes from
	// `allocator` and counts against memory_limit.
	static AllocatedData GatherKeys(const LinkedList &keys, Allocator &allocator) {
		const auto functions = KeyFunctions();
		auto result = allocator.Allocate(keys.total_capacity * sizeof(uint64_t));
		auto target = reinterpret_cast<uint64_t *>(result.get());

		ListSegmentScanState scan_state;
		functions.InitializeScan(keys, scan_state);
		Vector chunk(LogicalType::UBIGINT, STANDARD_VECTOR_SIZE);
		idx_t gathered = 0;
		for (;;) {
			const auto scanned = functions.Scan(scan_state, chunk);
			if (!scanned) {
				break;
			}
			memcpy(target + gathered, FlatVector::GetData<uint64_t>(chunk), scanned * sizeof(uint64_t));
			gathered += scanned;
		}
		D_ASSERT(gathered == keys.total_capacity);
		return result;
	}
};

// Builds the aggregate for a filter whose operation derives from
// BitfiltersKeyCollectOperation.
template <class OP>
static AggregateFunction BitfiltersKeyAggregate(const LogicalType &input_type, const LogicalType &return_type) {
	AggregateFunction result({input_type}, return_type, AggregateFunction::StateSize<BitfiltersKeyState>,
	                         AggregateFunction::StateInitialize<BitfiltersKeyState, OP>, OP::ScatterUpdate, OP::Combine,
	                         AggregateFunction::StateFinalize<BitfiltersKeyState, string_t, OP>, OP::ClusterUpdate);
	AggregateFunction::WireStructStateType<BitfiltersKeyState>(result);
	return result;
}

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
