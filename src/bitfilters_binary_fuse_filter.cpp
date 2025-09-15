#include "bitfilters_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "vendor/fastfilter/binaryfusefilter.h"

namespace duckdb {

namespace {

struct BinaryFuseFilterBindData : public FunctionData {
	explicit BinaryFuseFilterBindData() {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<BinaryFuseFilterBindData>();
	}

	bool Equals(const FunctionData &other_p) const override {
		//		auto &other = other_p.Cast<BinaryFuseFilterBindData>();
		return true;
	}
};

unique_ptr<FunctionData> BinaryFuseFilterBind(ClientContext &context, AggregateFunction &function,
                                              vector<unique_ptr<Expression>> &arguments) {
	return make_uniq<BinaryFuseFilterBindData>();
}

template <class T>
struct BinaryFuseFilterState {
	std::unique_ptr<std::vector<uint64_t>> entries = nullptr;

	BinaryFuseFilterState() = delete;

	explicit BinaryFuseFilterState(const string_t &data) {
		entries = make_uniq<std::vector<uint64_t>>();
		entries->reserve(STANDARD_VECTOR_SIZE); // Reserve space to avoid reallocations
	}
};

struct BinaryFuseFilterOperationBase {
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

template <typename FilterType>
struct BinaryFuseCreateTraits;

template <>
struct BinaryFuseCreateTraits<binary_fuse16_t> {
	static bool Allocate(size_t n, binary_fuse16_t *filter) {
		return binary_fuse16_allocate(n, filter);
	}
	static bool Populate(uint64_t *data, size_t n, binary_fuse16_t *filter) {
		return binary_fuse16_populate(data, n, filter);
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
};

template <>
struct BinaryFuseCreateTraits<binary_fuse8_t> {
	static bool Allocate(size_t n, binary_fuse8_t *filter) {
		return binary_fuse8_allocate(n, filter);
	}
	static bool Populate(uint64_t *data, size_t n, binary_fuse8_t *filter) {
		return binary_fuse8_populate(data, n, filter);
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
};

template <class BIND_DATA_TYPE, typename FilterType>
struct BinaryFuseFilterCreateOperation {

	using Traits = BinaryFuseCreateTraits<FilterType>;

	template <class STATE>
	static void Initialize(STATE &state) {
		state.entries.release();
		state.entries = nullptr;
	}

	static bool IgnoreNull() {
		return true;
	}

	template <class STATE>
	static void Destroy(STATE &state, AggregateInputData &) {
		if (state.entries) {
			state.entries = nullptr;
		}
	}

	template <class A_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const A_TYPE &a_data, AggregateUnaryInput &) {
		if (!state.entries) {
			state.entries = make_uniq<std::vector<uint64_t>>();
			state.entries->reserve(STANDARD_VECTOR_SIZE);
		}
		state.entries->push_back(a_data);
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
	                              idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		if (!source.entries || source.entries->empty()) {
			return;
		}
		if (!target.entries) {
			target.entries = make_uniq<std::vector<uint64_t>>();
		}
		target.entries->reserve(target.entries->size() + source.entries->size());
		target.entries->insert(target.entries->end(), std::make_move_iterator(source.entries->begin()),
		                       std::make_move_iterator(source.entries->end()));
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (!state.entries || state.entries->empty()) {
			finalize_data.ReturnNull();
		} else {
			FilterType filter;
			if (!Traits::Allocate(state.entries->size(), &filter)) {
				throw IOException("Failed to allocate filter");
			}
			if (!Traits::Populate(state.entries->data(), state.entries->size(), &filter)) {
				Traits::Free(&filter);
				throw IOException("Failed to populate filter");
			}
			size_t serial_size = Traits::SerializationBytes(&filter);
			std::vector<char> buffer(serial_size);
			Traits::Serialize(&filter, buffer.data());
			target = StringVector::AddStringOrBlob(finalize_data.result, buffer.data(), serial_size);
			Traits::Free(&filter);
		}
	}
};

template <typename T, typename FilterType>
auto static BinaryFuseFilterCreateAggregate(const LogicalType &type, const LogicalType &result_type)
    -> AggregateFunction {

	return AggregateFunction::UnaryAggregateDestructor<
	    BinaryFuseFilterState<T>, T, string_t, BinaryFuseFilterCreateOperation<BinaryFuseFilterBindData, FilterType>,
	    AggregateDestructorType::LEGACY>(type, result_type);
}

template <typename FilterType>
struct BinaryFuseFilterContainsTraits;

template <>
struct BinaryFuseFilterContainsTraits<binary_fuse16_t> {
	static void Deserialize(binary_fuse16_t *filter, const char *data) {
		filter->Fingerprints = (uint16_t *)binary_fuse16_deserialize_header(filter, data);
	}
	static bool Contains(uint64_t item, const binary_fuse16_t *filter) {
		return binary_fuse16_contain(item, filter);
	}
	static void Free(binary_fuse16_t *filter) {
		//		binary_fuse16_free(filter);
	}
};

template <>
struct BinaryFuseFilterContainsTraits<binary_fuse8_t> {
	static void Deserialize(binary_fuse8_t *filter, const char *data) {
		filter->Fingerprints = (uint8_t *)binary_fuse8_deserialize_header(filter, data);
	}
	static bool Contains(uint64_t item, const binary_fuse8_t *filter) {
		return binary_fuse8_contain(item, filter);
	}
	static void Free(binary_fuse8_t *filter) {
		//		binary_fuse8_free(filter);
	}
};

template <class T, typename FilterType>
static inline void BinaryFuseFilterContains(DataChunk &args, ExpressionState &state, Vector &result) {
	using Traits = BinaryFuseFilterContainsTraits<FilterType>;

	D_ASSERT(args.ColumnCount() == 2);

	auto &filter_vector = args.data[0];
	auto &item_vector = args.data[1];

	if (filter_vector.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		FilterType deserialized_filter;
		auto &data = ConstantVector::GetData<string_t>(filter_vector)[0];

		Traits::Deserialize(&deserialized_filter, data.GetData());

		UnaryExecutor::Execute<T, bool>(item_vector, result, args.size(),
		                                [&](T item_data) { return Traits::Contains(item_data, &deserialized_filter); });

		Traits::Free(&deserialized_filter);
	} else {
		BinaryExecutor::Execute<string_t, T, bool>(filter_vector, item_vector, result, args.size(),
		                                           [&](string_t filter_data, T item_data) {
			                                           FilterType deserialized_filter;

			                                           Traits::Deserialize(&deserialized_filter, filter_data.GetData());

			                                           bool result = Traits::Contains(item_data, &deserialized_filter);
			                                           Traits::Free(&deserialized_filter);
			                                           return result;
		                                           });
	}
}

// Helper function to register BinaryFuse filter functions for a specific type
template <typename T, typename FilterType>
static void RegisterBinaryFuseFilterFunctionsForType(AggregateFunctionSet &BinaryFusefilter, const LogicalType &type) {
	// Register create aggregate function
	auto fun = BinaryFuseFilterCreateAggregate<T, FilterType>(type, LogicalType::BLOB);
	fun.bind = BinaryFuseFilterBind;
	BinaryFusefilter.AddFunction(fun);
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
			desc.description = "Creates a BinaryFuse16 filter.";
			desc.examples.push_back("SELECT xor_filter(16, 8, column) FROM table");
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
			desc.description = "Creates a BinaryFuse16 filter.";
			desc.examples.push_back("SELECT xor_filter(16, 8, column) FROM table");
			BinaryFusefilter_create_info.descriptions.push_back(desc);
		}
		loader.RegisterFunction(BinaryFusefilter_create_info);
	}

	// Register scalar functions
	{
		ScalarFunctionSet fs("binary_fuse16_filter_contains");
		RegisterBinaryFuseFilterContainsForType<uint64_t, binary_fuse16_t>(fs, LogicalType::UBIGINT);
		CreateScalarFunctionInfo info(std::move(fs));
		{
			FunctionDescription desc;
			desc.description = "Tests if a BinaryFuse16 filter may contain a value. Returns true if the value "
			                   "might be in the set (with possible false positives), or false if the value "
			                   "is definitely not in the set (no false negatives).";
			desc.examples.push_back("SELECT binary_fuse16_filter_contains(filter, 42) FROM table");
			info.descriptions.push_back(desc);
		}
		loader.RegisterFunction(info);
	}

	{
		ScalarFunctionSet fs("binary_fuse8_filter_contains");
		RegisterBinaryFuseFilterContainsForType<uint64_t, binary_fuse8_t>(fs, LogicalType::UBIGINT);
		CreateScalarFunctionInfo info(std::move(fs));
		{
			FunctionDescription desc;
			desc.description = "Tests if a BinaryFuse8 filter may contain a value. Returns true if the value "
			                   "might be in the set (with possible false positives), or false if the value "
			                   "is definitely not in the set (no false negatives).";
			desc.examples.push_back("SELECT binary_fuse8_filter_contains(filter, 42) FROM table");
			info.descriptions.push_back(desc);
		}
		loader.RegisterFunction(info);
	}
}

} // namespace duckdb