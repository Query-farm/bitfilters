#include "bitfilters_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include <boost/bloom/filter.hpp>

namespace duckdb {

namespace {
struct BloomFilterBindData : public FunctionData {
	BloomFilterBindData() {
	}
	explicit BloomFilterBindData(uint64_t capacity, double fpr) : capacity(capacity), fpr(fpr) {
		// Validate parameters during construction
		if (capacity == 0) {
			throw InvalidInputException("Bloom filter capacity must be greater than 0");
		}
		if (fpr <= 0.0 || fpr >= 1.0) {
			throw InvalidInputException("Bloom filter false positive rate must be between 0 and 1 (exclusive)");
		}
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<BloomFilterBindData>(capacity, fpr);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<BloomFilterBindData>();
		return capacity == other.capacity && fpr == other.fpr;
	}

	// The capacity of the bloom filter in items
	uint64_t capacity;

	// The false positive rate of the bloom filter
	double fpr;
};

unique_ptr<FunctionData> BloomFilterBind(ClientContext &context, AggregateFunction &function,
                                         vector<unique_ptr<Expression>> &arguments) {

	if (arguments.size() == 1) {
		return nullptr;
	}
	if (arguments.size() > 3) {
		throw BinderException("Bloom filter aggregate function requires exactly 3 arguments");
	}
	if (arguments[0]->HasParameter()) {
		throw ParameterNotResolvedException();
	}
	if (!arguments[0]->IsFoldable()) {
		throw BinderException("Bloom filter can only take a constant capacity");
	}
	Value capacity_val = ExpressionExecutor::EvaluateScalar(context, *arguments[0]);
	if (capacity_val.IsNull()) {
		throw BinderException("Bloom filter capacity cannot be NULL");
	}
	// FIXME: should check the type
	capacity_val.CastAs(context, LogicalType::UBIGINT);
	auto actual_capacity = capacity_val.GetValue<uint64_t>();
	if (actual_capacity == 0) {
		throw BinderException("Bloom filter capacity must be greater than 0");
	}

	if (arguments[1]->HasParameter()) {
		throw ParameterNotResolvedException();
	}
	if (!arguments[1]->IsFoldable()) {
		throw BinderException("Bloom false positive rate can only take a constant value");
	}
	Value fpr_val = ExpressionExecutor::EvaluateScalar(context, *arguments[1]);
	if (fpr_val.IsNull()) {
		throw BinderException("Bloom false positive rate cannot be NULL");
	}
	// FIXME: should check the type
	fpr_val.CastAs(context, LogicalType::FLOAT);
	auto actual_fpr = fpr_val.GetValue<double>();
	if (actual_fpr <= 0 || actual_fpr >= 1) {
		throw BinderException("Bloom false positive rate must be between 0 and 1");
	}

	// So doing this causes problems.
	Function::EraseArgument(function, arguments, 0);
	Function::EraseArgument(function, arguments, 0);

	return make_uniq<BloomFilterBindData>(actual_capacity, actual_fpr);
}

template <class T>
struct BloomFilterState {
	using Filter = boost::bloom::filter<T, 5>;
	std::unique_ptr<Filter> filter = nullptr;

	BloomFilterState() = delete;

	explicit BloomFilterState(const string_t &data) {
		if (data.GetSize() < sizeof(uint64_t)) {
			throw BinderException("Bloom filter data is too short to contain capacity current length: " +
			                      std::to_string(data.GetSize()));
		}
		uint64_t capacity;
		memcpy(&capacity, data.GetDataUnsafe(), sizeof(capacity)); // read capacity
		if (capacity == 0) {
			throw BinderException("Bloom filter capacity cannot be 0");
		}
		if (data.GetSize() < sizeof(uint64_t) + capacity) {
			throw BinderException("Bloom filter data is too short to contain the filter array");
		}

		filter = make_uniq<Filter>(capacity);
		boost::span<unsigned char> s2 = filter->array();
		memcpy(s2.data(), data.GetDataUnsafe() + sizeof(capacity), s2.size()); // load array
	}

	explicit BloomFilterState(const std::string &data) {
		if (data.size() < sizeof(uint64_t)) {
			throw BinderException("Bloom filter data is too short to contain capacity current length: " +
			                      std::to_string(data.size()));
		}
		uint64_t capacity;
		memcpy(&capacity, data.data(), sizeof(capacity)); // read capacity
		if (capacity == 0) {
			throw BinderException("Bloom filter capacity cannot be 0");
		}
		if (data.size() < sizeof(uint64_t) + capacity) {
			throw BinderException("Bloom filter data is too short to contain the filter array");
		}

		filter = make_uniq<Filter>(capacity);
		boost::span<unsigned char> s2 = filter->array();
		memcpy(s2.data(), data.data() + sizeof(capacity), s2.size()); // load array
	}

	void CreateFilter(uint64_t capacity, double fpr) {
		D_ASSERT(!filter);
		filter = make_uniq<Filter>(Filter::capacity_for(capacity, fpr));
	}

	void CreateFilter(const BloomFilterState &existing) {
		if (existing.filter) {
			filter = make_uniq<Filter>(*existing.filter);
		}
	}

	std::string serialize() const {
		D_ASSERT(filter != nullptr);
		std::size_t c1 = filter->capacity();

		// Allocate a string that can hold the serialized data
		std::string serialized_data;
		serialized_data.resize(c1 + sizeof(c1));
		memcpy((char *)serialized_data.data(), &c1, sizeof(c1)); // save capacity (bits
		boost::span<const unsigned char> s1 = filter->array();
		memcpy((char *)serialized_data.data() + sizeof(c1), s1.data(), s1.size()); // save array
		return serialized_data;
	}
};

struct BloomFilterOperationBase {
	template <class STATE>
	static void Initialize(STATE &state) {
		state.filter.release();
		state.filter = nullptr;
	}

	static bool IgnoreNull() {
		return true;
	}
};

template <class BIND_DATA_TYPE>
struct BloomFilterMergeOperation : BloomFilterOperationBase {

	template <class A_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const A_TYPE &a_data, AggregateUnaryInput &idata) {
		if (!state.filter) {
			auto &bind_data = idata.input.bind_data->template Cast<BIND_DATA_TYPE>();
			state.CreateFilter(bind_data.capacity, bind_data.fpr);
		}

		// this is a filter in b_data, so we need to deserialize it.
		*state.filter |= *(STATE(a_data.GetString()).filter);
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
			*target.filter |= *source.filter;
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
struct BloomFilterCreateOperation : BloomFilterOperationBase {
	template <class A_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const A_TYPE &a_data, AggregateUnaryInput &idata) {
		if (!state.filter) {
			auto &bind_data = idata.input.bind_data->template Cast<BIND_DATA_TYPE>();
			state.CreateFilter(bind_data.capacity, bind_data.fpr);
		}
		state.filter->insert(a_data);
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
			*target.filter |= *source.filter;
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
auto static BloomFilterMergeAggregate(const LogicalType &type, const LogicalType &result_type) -> AggregateFunction

{
	return AggregateFunction::UnaryAggregateDestructor<BloomFilterState<T>, string_t, string_t,
	                                                   BloomFilterMergeOperation<BloomFilterBindData>,
	                                                   AggregateDestructorType::LEGACY>(result_type, result_type);
}

template <typename T>
auto static BloomFilterCreateAggregate(const LogicalType &type, const LogicalType &result_type) -> AggregateFunction {

	return AggregateFunction::UnaryAggregateDestructor<BloomFilterState<T>, T, string_t,
	                                                   BloomFilterCreateOperation<BloomFilterBindData>,
	                                                   AggregateDestructorType::LEGACY>(type, result_type);
}

template <class T>

static inline void BloomFilterContains(DataChunk &args, ExpressionState &state, Vector &result) {
	// Get the references to the incoming vectors.
	D_ASSERT(args.ColumnCount() == 2);

	auto &filter_vector = args.data[0];
	auto &item_vector = args.data[1];

	if (filter_vector.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		auto state = BloomFilterState<T>(ConstantVector::GetData<string_t>(filter_vector)[0]);
		UnaryExecutor::Execute<T, bool>(item_vector, result, args.size(),
		                                [&](T item_data) { return state.filter->may_contain(item_data); });
	} else {
		BinaryExecutor::Execute<string_t, T, bool>(filter_vector, item_vector, result, args.size(),
		                                           [&](string_t filter_data, T item_data) {
			                                           auto state = BloomFilterState<T>(filter_data);

			                                           return state.filter->may_contain(item_data);
		                                           });
	}
}

// Helper function to register bloom filter functions for a specific type
template <typename T>
static void RegisterBloomFilterFunctionsForType(AggregateFunctionSet &bloomfilter, const LogicalType &type) {
	// Register create aggregate function
	{
		auto fun = BloomFilterCreateAggregate<T>(type, LogicalType::BLOB);
		fun.bind = BloomFilterBind;
		fun.arguments.insert(fun.arguments.begin(), LogicalType::FLOAT);
		fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
		bloomfilter.AddFunction(fun);
	}

	// Register merge aggregate function
	{
		auto fun = BloomFilterMergeAggregate<T>(type, LogicalType::BLOB);
		fun.bind = BloomFilterBind;
		fun.arguments.insert(fun.arguments.begin(), LogicalType::FLOAT);
		fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
		bloomfilter.AddFunction(fun);
	}
}

// Helper function to register scalar contains functions for a specific type
template <typename T>
static void RegisterBloomFilterContainsForType(ScalarFunctionSet &fs, const LogicalType &type) {
	fs.AddFunction(ScalarFunction({LogicalType::BLOB, type}, LogicalType::BOOLEAN, BloomFilterContains<T>));
}
} // namespace

void LoadBloomFilter(DatabaseInstance &instance) {
	auto &system_catalog = Catalog::GetSystemCatalog(instance);
	auto data = CatalogTransaction::GetSystemTransaction(instance);

	// Register aggregate functions
	{
		AggregateFunctionSet bloomfilter("bloomfilter");

		// Register functions for all supported types
		RegisterBloomFilterFunctionsForType<int8_t>(bloomfilter, LogicalType::TINYINT);
		RegisterBloomFilterFunctionsForType<int16_t>(bloomfilter, LogicalType::SMALLINT);
		RegisterBloomFilterFunctionsForType<int32_t>(bloomfilter, LogicalType::INTEGER);
		RegisterBloomFilterFunctionsForType<int64_t>(bloomfilter, LogicalType::BIGINT);
		RegisterBloomFilterFunctionsForType<float>(bloomfilter, LogicalType::FLOAT);
		RegisterBloomFilterFunctionsForType<double>(bloomfilter, LogicalType::DOUBLE);
		RegisterBloomFilterFunctionsForType<uint8_t>(bloomfilter, LogicalType::UTINYINT);
		RegisterBloomFilterFunctionsForType<uint16_t>(bloomfilter, LogicalType::USMALLINT);
		RegisterBloomFilterFunctionsForType<uint32_t>(bloomfilter, LogicalType::UINTEGER);
		RegisterBloomFilterFunctionsForType<uint64_t>(bloomfilter, LogicalType::UBIGINT);

		CreateAggregateFunctionInfo bloomfilter_create_info(bloomfilter);

		{
			FunctionDescription desc;
			desc.description = "Creates a Bloom filter by aggregating values or by merging other Bloom filters. "
			                   "Takes capacity (integer), false positive rate (0.0-1.0), and data to aggregate.";
			desc.examples.push_back("SELECT bloomfilter(1000, 0.01, column) FROM table");
			desc.examples.push_back("SELECT bloomfilter(1000, 0.01, existing_filter) FROM filters");
			bloomfilter_create_info.descriptions.push_back(desc);
		}

		system_catalog.CreateFunction(data, bloomfilter_create_info);
	}

	// Register scalar functions
	{
		ScalarFunctionSet fs("bloomfilter_contains");

		// Register contains functions for all supported types
		RegisterBloomFilterContainsForType<int8_t>(fs, LogicalType::TINYINT);
		RegisterBloomFilterContainsForType<int16_t>(fs, LogicalType::SMALLINT);
		RegisterBloomFilterContainsForType<int32_t>(fs, LogicalType::INTEGER);
		RegisterBloomFilterContainsForType<int64_t>(fs, LogicalType::BIGINT);
		RegisterBloomFilterContainsForType<float>(fs, LogicalType::FLOAT);
		RegisterBloomFilterContainsForType<double>(fs, LogicalType::DOUBLE);
		RegisterBloomFilterContainsForType<uint8_t>(fs, LogicalType::UTINYINT);
		RegisterBloomFilterContainsForType<uint16_t>(fs, LogicalType::USMALLINT);
		RegisterBloomFilterContainsForType<uint32_t>(fs, LogicalType::UINTEGER);
		RegisterBloomFilterContainsForType<uint64_t>(fs, LogicalType::UBIGINT);

		CreateScalarFunctionInfo info(std::move(fs));

		{
			FunctionDescription desc;
			desc.description = "Tests if a Bloom filter may contain a value. Returns true if the value "
			                   "might be in the set (with possible false positives), or false if the value "
			                   "is definitely not in the set (no false negatives).";
			desc.examples.push_back("SELECT bloomfilter_contains(filter, 42) FROM table");
			desc.examples.push_back("SELECT * FROM table WHERE bloomfilter_contains(precomputed_filter, id)");
			info.descriptions.push_back(desc);
		}

		system_catalog.CreateFunction(data, info);
	}
}

} // namespace duckdb