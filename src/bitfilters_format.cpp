#include "bitfilters_format.hpp"
#include "bitfilters_xxhash.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/vector/dictionary_vector.hpp"
#include "duckdb/common/vector/vector_iterator.hpp"
#include "duckdb/common/vector/vector_writer.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

namespace {

constexpr char BITFILTERS_MAGIC[4] = {'B', 'F', 'L', 'T'};

bool IsKnownType(uint8_t type) {
	return type >= static_cast<uint8_t>(BitfilterType::QUOTIENT) && type <= static_cast<uint8_t>(BitfilterType::BLOOM);
}

bool IsKnownHash(uint8_t hash) {
	return hash <= static_cast<uint8_t>(BitfilterHash::XXH64);
}

// The Parquet plain encoding of a fixed-width value, hashed the way Parquet
// Bloom filters hash it.
template <class STORED, class T>
uint64_t HashFixed(T value) {
	auto stored = static_cast<STORED>(value);
	return BitfiltersXXH64::Hash(const_data_ptr_cast(&stored), sizeof(stored));
}

template <class T, class OP>
void WriteKeys(Vector &input, Vector &keys, idx_t count, OP &&op) {
	auto values = input.Values<T>();
	auto writer = FlatVector::Writer<uint64_t>(keys, count);
	for (idx_t i = 0; i < count; i++) {
		auto entry = values[i];
		if (!entry.IsValid()) {
			writer.WriteNull();
		} else {
			writer.WriteValue(op(entry.GetValue()));
		}
	}
}

// XXH64 of each value's Parquet plain encoding, as DuckDB's Parquet reader
// computes it for Bloom filter pruning: small integers are INT32, UINT32 stays
// 4 bytes, 64-bit integers 8 bytes, strings and blobs their bytes, UUIDs their
// 16 byte big-endian form. Types Parquet Bloom filters do not cover hash their
// fixed-size on-disk representation. This encoding must never change: it is
// what makes a stored filter answer the same in every version.
void ComputeXXH64Keys(Vector &input, Vector &keys, idx_t count) {
	auto &type = input.GetType();
	if (type.id() == LogicalTypeId::SQLNULL) {
		WriteKeys<int32_t>(input, keys, count, [](int32_t) -> uint64_t {
			throw InternalException("bitfilters: a NULL literal has no values to hash");
		});
		return;
	}
	if (type.id() == LogicalTypeId::UUID) {
		WriteKeys<hugeint_t>(input, keys, count, [](hugeint_t value) {
			data_t bytes[16];
			BaseUUID::ToBlob(value, bytes);
			return BitfiltersXXH64::Hash(bytes, sizeof(bytes));
		});
		return;
	}
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
		WriteKeys<bool>(input, keys, count, [](bool value) { return HashFixed<uint8_t>(value); });
		break;
	case PhysicalType::INT8:
		WriteKeys<int8_t>(input, keys, count, [](int8_t value) { return HashFixed<int32_t>(value); });
		break;
	case PhysicalType::UINT8:
		WriteKeys<uint8_t>(input, keys, count, [](uint8_t value) { return HashFixed<int32_t>(value); });
		break;
	case PhysicalType::INT16:
		WriteKeys<int16_t>(input, keys, count, [](int16_t value) { return HashFixed<int32_t>(value); });
		break;
	case PhysicalType::UINT16:
		WriteKeys<uint16_t>(input, keys, count, [](uint16_t value) { return HashFixed<int32_t>(value); });
		break;
	case PhysicalType::INT32:
		WriteKeys<int32_t>(input, keys, count, [](int32_t value) { return HashFixed<int32_t>(value); });
		break;
	case PhysicalType::UINT32:
		WriteKeys<uint32_t>(input, keys, count, [](uint32_t value) { return HashFixed<uint32_t>(value); });
		break;
	case PhysicalType::INT64:
		WriteKeys<int64_t>(input, keys, count, [](int64_t value) { return HashFixed<int64_t>(value); });
		break;
	case PhysicalType::UINT64:
		WriteKeys<uint64_t>(input, keys, count, [](uint64_t value) { return HashFixed<uint64_t>(value); });
		break;
	case PhysicalType::FLOAT:
		WriteKeys<float>(input, keys, count, [](float value) { return HashFixed<float>(value); });
		break;
	case PhysicalType::DOUBLE:
		WriteKeys<double>(input, keys, count, [](double value) { return HashFixed<double>(value); });
		break;
	case PhysicalType::INT128:
		WriteKeys<hugeint_t>(input, keys, count, [](hugeint_t value) { return HashFixed<hugeint_t>(value); });
		break;
	case PhysicalType::UINT128:
		WriteKeys<uhugeint_t>(input, keys, count, [](uhugeint_t value) { return HashFixed<uhugeint_t>(value); });
		break;
	case PhysicalType::INTERVAL:
		WriteKeys<interval_t>(input, keys, count, [](interval_t value) { return HashFixed<interval_t>(value); });
		break;
	case PhysicalType::VARCHAR:
		WriteKeys<string_t>(input, keys, count, [](string_t value) {
			return BitfiltersXXH64::Hash(const_data_ptr_cast(value.GetData()), value.GetSize());
		});
		break;
	default:
		throw InternalException("bitfilters: cannot hash %s values", type.ToString());
	}
}

// Whether values of `type` have a stable encoding ComputeXXH64Keys can hash.
// An ENUM is stored as its position in the type's dictionary, which differs
// between ENUM types with the same labels; nested types would need an
// encoding of their own.
bool CanHash(const LogicalType &type) {
	if (type.id() == LogicalTypeId::ENUM) {
		return false;
	}
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
	case PhysicalType::UINT8:
	case PhysicalType::INT16:
	case PhysicalType::UINT16:
	case PhysicalType::INT32:
	case PhysicalType::UINT32:
	case PhysicalType::INT64:
	case PhysicalType::UINT64:
	case PhysicalType::FLOAT:
	case PhysicalType::DOUBLE:
	case PhysicalType::INT128:
	case PhysicalType::UINT128:
	case PhysicalType::INTERVAL:
	case PhysicalType::VARCHAR:
		return true;
	default:
		return type.id() == LogicalTypeId::SQLNULL;
	}
}

} // namespace

const char *BitfilterTypeName(BitfilterType type) {
	switch (type) {
	case BitfilterType::QUOTIENT:
		return "quotient_filter";
	case BitfilterType::XOR8:
		return "xor8_filter";
	case BitfilterType::XOR16:
		return "xor16_filter";
	case BitfilterType::BINARY_FUSE8:
		return "binary_fuse8_filter";
	case BitfilterType::BINARY_FUSE16:
		return "binary_fuse16_filter";
	case BitfilterType::BLOOM:
		return "bloom_filter";
	}
	throw InternalException("unknown bitfilter type");
}

BitfilterKeyFormat BitfilterKeyFormat::ForValues(const LogicalType &type, bool ubigint_keys) {
	BitfilterKeyFormat result;
	if (ubigint_keys && type.id() == LogicalTypeId::UBIGINT) {
		return result;
	}
	if (!CanHash(type)) {
		throw BinderException("bitfilters: cannot build a filter from %s values, which have no stable encoding to "
		                      "hash. Cast them to VARCHAR, or compute a UBIGINT key for each value yourself.",
		                      type.ToString());
	}
	result.hash = BitfilterHash::XXH64;
	result.key_type = type.id();
	if (type.id() == LogicalTypeId::DECIMAL) {
		result.width = DecimalType::GetWidth(type);
		result.scale = DecimalType::GetScale(type);
	}
	return result;
}

LogicalType BitfilterKeyFormat::KeyLogicalType() const {
	switch (key_type) {
	case LogicalTypeId::DECIMAL:
		return LogicalType::DECIMAL(width, scale);
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::UHUGEINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_NS:
	case LogicalTypeId::TIME_TZ:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::INTERVAL:
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BLOB:
	case LogicalTypeId::UUID:
	case LogicalTypeId::BIT:
		return LogicalType(key_type);
	default:
		return LogicalType::INVALID;
	}
}

string BitfilterKeyFormat::KeyTypeName() const {
	auto type = KeyLogicalType();
	return type.id() == LogicalTypeId::INVALID ? EnumUtil::ToString(key_type) : type.ToString();
}

const char *BitfilterKeyFormat::HashName() const {
	switch (hash) {
	case BitfilterHash::NONE:
		return "none";
	case BitfilterHash::XXH64:
		return "xxhash64";
	}
	throw InternalException("unknown bitfilter hash");
}

void BitfiltersWriteHeader(data_ptr_t out, BitfilterType type, const BitfilterKeyFormat &keys) {
	memset(out, 0, BITFILTERS_HEADER_SIZE);
	memcpy(out, BITFILTERS_MAGIC, sizeof(BITFILTERS_MAGIC));
	out[4] = BITFILTERS_FORMAT_VERSION;
	out[5] = static_cast<uint8_t>(type);
	out[6] = static_cast<uint8_t>(keys.hash);
	out[7] = static_cast<uint8_t>(keys.key_type);
	out[8] = keys.width;
	out[9] = keys.scale;
}

BitfilterBlob BitfilterBlob::Parse(const string_t &blob, const char *function) {
	BitfilterBlob result;
	auto data = const_data_ptr_cast(blob.GetData());
	const auto size = blob.GetSize();
	if (size < BITFILTERS_HEADER_SIZE || memcmp(data, BITFILTERS_MAGIC, sizeof(BITFILTERS_MAGIC)) != 0) {
		result.payload = data;
		result.payload_size = size;
		return result;
	}
	result.has_header = true;
	result.version = data[4];
	if (result.version != BITFILTERS_FORMAT_VERSION) {
		throw InvalidInputException("%s: the filter BLOB has format version %u, but this version of bitfilters only "
		                            "reads version %u. Upgrade the bitfilters extension to read it.",
		                            function, static_cast<uint32_t>(result.version),
		                            static_cast<uint32_t>(BITFILTERS_FORMAT_VERSION));
	}
	if (!IsKnownType(data[5]) || !IsKnownHash(data[6])) {
		throw InvalidInputException("%s: the filter BLOB has a bitfilters header with an unknown filter type (%u) or "
		                            "hash (%u)",
		                            function, static_cast<uint32_t>(data[5]), static_cast<uint32_t>(data[6]));
	}
	result.type = static_cast<BitfilterType>(data[5]);
	result.keys.hash = static_cast<BitfilterHash>(data[6]);
	result.keys.key_type = static_cast<LogicalTypeId>(data[7]);
	result.keys.width = data[8];
	result.keys.scale = data[9];
	result.payload = data + BITFILTERS_HEADER_SIZE;
	result.payload_size = size - BITFILTERS_HEADER_SIZE;
	return result;
}

BitfilterBlob BitfilterBlob::Expect(const string_t &blob, BitfilterType type, const char *function) {
	auto result = Parse(blob, function);
	if (!result.has_header) {
		result.type = type;
		return result;
	}
	if (result.type != type) {
		throw InvalidInputException("%s: the filter BLOB is a %s, not a %s", function, BitfilterTypeName(result.type),
		                            BitfilterTypeName(type));
	}
	return result;
}

void BitfiltersComputeKeys(const BitfilterKeyFormat &format, Vector &input, Vector &keys, idx_t count) {
	switch (format.hash) {
	case BitfilterHash::NONE:
		D_ASSERT(input.GetType().id() == LogicalTypeId::UBIGINT);
		WriteKeys<uint64_t>(input, keys, count, [](uint64_t value) { return value; });
		break;
	case BitfilterHash::XXH64:
		if (input.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
			// Strings read from Parquet and other dictionary-encoded sources
			// repeat a few distinct values many times: hash each dictionary
			// entry once rather than every row.
			const auto dictionary_size = DictionaryVector::DictionarySize(input);
			if (dictionary_size.IsValid() && dictionary_size.GetIndex() < count) {
				const auto size = dictionary_size.GetIndex();
				Vector dictionary_keys(LogicalType::UBIGINT, size);
				ComputeXXH64Keys(DictionaryVector::Child(input), dictionary_keys, size);
				auto entries = dictionary_keys.Values<uint64_t>();
				auto &sel = DictionaryVector::SelVector(input);
				auto writer = FlatVector::Writer<uint64_t>(keys, count);
				for (idx_t i = 0; i < count; i++) {
					auto entry = entries[sel.get_index(i)];
					if (!entry.IsValid()) {
						writer.WriteNull();
					} else {
						writer.WriteValue(entry.GetValue());
					}
				}
				break;
			}
		}
		ComputeXXH64Keys(input, keys, count);
		break;
	}
}

BitfiltersProbeKeys::BitfiltersProbeKeys(Vector &values_p, idx_t count_p, const char *function_p)
    : values(values_p), count(count_p), function(function_p) {
}

const BitfiltersProbeKeys::Keys &BitfiltersProbeKeys::Get(const BitfilterKeyFormat &format) {
	for (auto &entry : computed) {
		if (entry->format == format) {
			return *entry;
		}
	}
	auto entry = make_uniq<Keys>();
	entry->format = format;
	entry->keys = make_uniq<Vector>(LogicalType::UBIGINT, count);
	entry->castable.Initialize(count);

	auto &value_type = values.GetType();
	auto key_type = format.KeyLogicalType();
	if (format.hash == BitfilterHash::NONE && value_type.id() != LogicalTypeId::UBIGINT && !value_type.IsIntegral()) {
		// Other integers are cast to UBIGINT keys below (as a UBIGINT
		// parameter used to cast them), but hashing anything else would not
		// give the keys the filter holds.
		throw InvalidInputException("%s: the filter was built from UBIGINT keys, so it has "
		                            "to be probed with UBIGINT keys too, not %s values",
		                            function, value_type.ToString());
	}
	if (value_type == key_type || (key_type.id() == LogicalTypeId::INVALID && value_type.id() == format.key_type)) {
		BitfiltersComputeKeys(format, values, *entry->keys, count);
	} else if (key_type.id() == LogicalTypeId::INVALID) {
		throw InvalidInputException("%s: the filter was built from %s values; probe it with values of that type, "
		                            "not %s",
		                            function, format.KeyTypeName(), value_type.ToString());
	} else {
		// A value that does not survive the cast to the filter's type cannot
		// have been one of the values the filter was built from.
		Vector cast_values(key_type, count);
		string error;
		VectorOperations::DefaultTryCast(values, cast_values, count, &error);
		auto original = values.Validity();
		auto cast = cast_values.Validity();
		for (idx_t i = 0; i < count; i++) {
			if (original.IsValid(i) && !cast.IsValid(i)) {
				entry->castable.SetInvalid(i);
			}
		}
		BitfiltersComputeKeys(format, cast_values, *entry->keys, count);
	}
	computed.push_back(std::move(entry));
	return *computed.back();
}

LogicalType BitfilterInfo::GetType() {
	child_list_t<LogicalType> children;
	children.emplace_back("filter_type", LogicalType::VARCHAR);
	children.emplace_back("format_version", LogicalType::UTINYINT);
	children.emplace_back("key_hash", LogicalType::VARCHAR);
	children.emplace_back("key_type", LogicalType::VARCHAR);
	children.emplace_back("size_bytes", LogicalType::UBIGINT);
	children.emplace_back("entries", LogicalType::UBIGINT);
	children.emplace_back("capacity", LogicalType::UBIGINT);
	children.emplace_back("load_factor", LogicalType::DOUBLE);
	children.emplace_back("fingerprint_bits", LogicalType::UTINYINT);
	children.emplace_back("bits_per_entry", LogicalType::DOUBLE);
	children.emplace_back("expected_fpr", LogicalType::DOUBLE);
	children.emplace_back("q", LogicalType::UTINYINT);
	children.emplace_back("r", LogicalType::UTINYINT);
	children.emplace_back("seed", LogicalType::UBIGINT);
	return LogicalType::STRUCT(std::move(children));
}

Value BitfilterInfo::ToValue() const {
	auto optional_count = [](const optional_idx &value) {
		return value.IsValid() ? Value::UBIGINT(value.GetIndex()) : Value(LogicalType::UBIGINT);
	};
	auto optional_double = [](double value) {
		return value >= 0 ? Value::DOUBLE(value) : Value(LogicalType::DOUBLE);
	};
	auto optional_bits = [](uint8_t value) {
		return value > 0 ? Value::UTINYINT(value) : Value(LogicalType::UTINYINT);
	};
	child_list_t<Value> children;
	children.emplace_back("filter_type", Value(filter_type));
	children.emplace_back("format_version", Value::UTINYINT(format_version));
	children.emplace_back("key_hash", Value(key_hash));
	children.emplace_back("key_type", Value(key_type));
	children.emplace_back("size_bytes", Value::UBIGINT(size_bytes));
	children.emplace_back("entries", optional_count(entries));
	children.emplace_back("capacity", optional_count(capacity));
	children.emplace_back("load_factor", optional_double(load_factor));
	children.emplace_back("fingerprint_bits", optional_bits(fingerprint_bits));
	children.emplace_back("bits_per_entry", entries.IsValid() && entries.GetIndex() > 0
	                                            ? Value::DOUBLE(static_cast<double>(size_bytes) * 8.0 /
	                                                            static_cast<double>(entries.GetIndex()))
	                                            : Value(LogicalType::DOUBLE));
	children.emplace_back("expected_fpr", optional_double(expected_fpr));
	children.emplace_back("q", optional_bits(q));
	children.emplace_back("r", optional_bits(r));
	children.emplace_back("seed", optional_count(seed));
	return Value::STRUCT(std::move(children));
}

unique_ptr<BitfilterProbe> BitfiltersOpen(BitfilterType type, const_data_ptr_t payload, idx_t size,
                                          Allocator &allocator, const char *function) {
	switch (type) {
	case BitfilterType::QUOTIENT:
		return QuotientFilterOpen(payload, size, allocator, function);
	case BitfilterType::XOR8:
	case BitfilterType::XOR16:
		return XorFilterOpen(type, payload, size, function);
	case BitfilterType::BINARY_FUSE8:
	case BitfilterType::BINARY_FUSE16:
		return BinaryFuseFilterOpen(type, payload, size, function);
	case BitfilterType::BLOOM:
		return BloomFilterOpen(payload, size, function);
	}
	throw InternalException("unknown bitfilter type");
}

namespace {

const char *ContainsFunctionName(optional_ptr<const BitfilterType> type) {
	if (!type) {
		return "bitfilter_contains";
	}
	switch (*type) {
	case BitfilterType::QUOTIENT:
		return "quotient_filter_contains";
	case BitfilterType::XOR8:
		return "xor8_filter_contains";
	case BitfilterType::XOR16:
		return "xor16_filter_contains";
	case BitfilterType::BINARY_FUSE8:
		return "binary_fuse8_filter_contains";
	case BitfilterType::BINARY_FUSE16:
		return "binary_fuse16_filter_contains";
	case BitfilterType::BLOOM:
		return "bloom_filter_contains";
	}
	throw InternalException("unknown bitfilter type");
}

// Opens `blob`, as a filter of `type` if given; without a type the BLOB must
// say what it is in its header.
unique_ptr<BitfilterProbe> OpenFilter(const string_t &blob, optional_ptr<const BitfilterType> type,
                                      Allocator &allocator, const char *function, BitfilterKeyFormat &keys) {
	auto parsed = type ? BitfilterBlob::Expect(blob, *type, function) : BitfilterBlob::Parse(blob, function);
	if (!parsed.has_header && !type) {
		throw InvalidInputException("%s: the filter BLOB has no bitfilters header (it was written by an older "
		                            "version); probe it with the function for its filter type, e.g. "
		                            "xor8_filter_contains",
		                            function);
	}
	keys = parsed.keys;
	return BitfiltersOpen(parsed.type, parsed.payload, parsed.payload_size, allocator, function);
}

// Bind data of a _contains function whose filter argument is a constant (a
// literal, a variable, anything foldable). DuckDB materializes a constant BLOB
// into every chunk it evaluates, so a multi-megabyte filter used to be copied
// for every 2048 rows probed -- far more work than the probes. The filter is
// kept here instead, and the argument replaced by an empty placeholder.
struct ConstantFilterData : public FunctionData {
	ConstantFilterData() = default;
	explicit ConstantFilterData(string filter_p) : filter(std::move(filter_p)), has_filter(true) {
	}

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<ConstantFilterData>();
		result->filter = filter;
		result->has_filter = has_filter;
		return std::move(result);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<ConstantFilterData>();
		return has_filter == other.has_filter && filter == other.filter;
	}

	static void Serialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data,
	                      const BoundScalarFunction &) {
		auto &data = bind_data->Cast<ConstantFilterData>();
		serializer.WriteProperty(100, "filter",
		                         data.has_filter
		                             ? Value::BLOB(const_data_ptr_cast(data.filter.data()), data.filter.size())
		                             : Value(LogicalType::BLOB));
	}

	static unique_ptr<FunctionData> Deserialize(Deserializer &deserializer, BoundScalarFunction &) {
		auto filter = deserializer.ReadProperty<Value>(100, "filter");
		if (filter.IsNull()) {
			return make_uniq<ConstantFilterData>();
		}
		return make_uniq<ConstantFilterData>(StringValue::Get(filter));
	}

	string filter;
	bool has_filter = false;
};

// The filter a thread probes when it is a constant, opened once.
struct ConstantFilterState : public FunctionLocalState {
	unique_ptr<BitfilterProbe> probe;
	BitfilterKeyFormat keys;
};

template <uint8_t TYPE_ID>
struct ContainsFunction {
	static optional_ptr<const BitfilterType> Type() {
		static const auto type = static_cast<BitfilterType>(TYPE_ID);
		return TYPE_ID == 0 ? nullptr : &type;
	}

	static unique_ptr<FunctionData> Bind(BindScalarFunctionInput &input) {
		auto &arguments = input.GetArguments();
		auto &filter = *arguments[0];
		if (filter.HasParameter() || !filter.IsFoldable()) {
			return make_uniq<ConstantFilterData>();
		}
		auto &context = input.GetClientContext();
		auto value = ExpressionExecutor::EvaluateScalar(context, filter);
		if (value.IsNull()) {
			return make_uniq<ConstantFilterData>();
		}
		auto data = make_uniq<ConstantFilterData>(StringValue::Get(value));
		// Validate now, so a bad constant filter fails before execution.
		BitfilterKeyFormat keys;
		OpenFilter(string_t(data->filter), Type(), BufferAllocator::Get(context), ContainsFunctionName(Type()), keys);
		arguments[0] = make_uniq<BoundConstantExpression>(Value::BLOB(string()));
		return std::move(data);
	}

	static unique_ptr<FunctionLocalState> InitLocalState(ExpressionState &state, const BoundFunctionExpression &,
	                                                     FunctionData *bind_data) {
		auto &data = bind_data->Cast<ConstantFilterData>();
		if (!data.has_filter) {
			return nullptr;
		}
		auto result = make_uniq<ConstantFilterState>();
		result->probe = OpenFilter(string_t(data.filter), Type(), BufferAllocator::Get(state.GetContext()),
		                           ContainsFunctionName(Type()), result->keys);
		return std::move(result);
	}

	static void Execute(DataChunk &args, ExpressionState &state, Vector &result);
};

// Writes the probe results for the rows of `values`: NULL for NULL values,
// false for values that cannot be cast to the filter's type.
void WriteResults(BitfilterProbe &probe, const BitfiltersProbeKeys::Keys &keys, Vector &values, Vector &result,
                  idx_t count) {
	auto value_validity = values.Validity();
	auto writer = FlatVector::Writer<bool>(result, count);
	auto found = make_unsafe_uniq_array<bool>(count);
	probe.ContainsBatch(FlatVector::GetData<uint64_t>(*keys.keys), count, found.get());
	for (idx_t i = 0; i < count; i++) {
		if (!value_validity.IsValid(i)) {
			writer.WriteNull();
		} else {
			writer.WriteValue(found[i] && keys.castable.RowIsValid(i));
		}
	}
}

template <uint8_t TYPE_ID>
void ContainsFunction<TYPE_ID>::Execute(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 2);
	auto type = Type();
	const auto function = ContainsFunctionName(type);
	auto &filters = args.data[0];
	auto &values = args.data[1];
	const auto count = args.size();
	BitfiltersProbeKeys probe_keys(values, count, function);

	auto local_state = ExecuteFunctionState::GetFunctionState(state);
	if (local_state) {
		auto &constant = local_state->Cast<ConstantFilterState>();
		WriteResults(*constant.probe, probe_keys.Get(constant.keys), values, result, count);
		return;
	}

	auto &allocator = BufferAllocator::Get(state.GetContext());
	auto filter_values = filters.Values<string_t>();
	if (filters.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		if (count == 0) {
			return;
		}
		auto filter = filter_values[0];
		if (!filter.IsValid()) {
			auto writer = FlatVector::Writer<bool>(result, count);
			for (idx_t i = 0; i < count; i++) {
				writer.WriteNull();
			}
			return;
		}
		BitfilterKeyFormat format;
		auto probe = OpenFilter(filter.GetValue(), type, allocator, function, format);
		WriteResults(*probe, probe_keys.Get(format), values, result, count);
		return;
	}

	// Per-row filters, as from a join against a table of filters: rows usually
	// cycle through a handful of filters. Open each distinct filter once per
	// chunk, then probe each filter's rows as one batch, so the probes get the
	// same prefetching as a constant filter.
	struct FilterRows {
		unique_ptr<BitfilterProbe> probe;
		BitfilterKeyFormat format;
		uint32_t size = 0;
		vector<sel_t> rows;
	};
	vector<FilterRows> opened;
	// Keyed by the BLOB's data pointer and size. Only opened (validated) BLOBs
	// are entered; they are longer than the inline threshold, so their data
	// pointers are stable heap addresses.
	unordered_map<const char *, idx_t> opened_index;
	auto value_validity = values.Validity();
	auto is_null = make_unsafe_uniq_array<bool>(count);
	for (idx_t i = 0; i < count; i++) {
		auto filter = filter_values[i];
		is_null[i] = !filter.IsValid() || !value_validity.IsValid(i);
		if (is_null[i]) {
			continue;
		}
		auto blob = filter.GetValue();
		auto entry = opened_index.find(blob.GetData());
		if (entry == opened_index.end() || opened[entry->second].size != blob.GetSize()) {
			FilterRows filter_rows;
			filter_rows.probe = OpenFilter(blob, type, allocator, function, filter_rows.format);
			filter_rows.size = blob.GetSize();
			entry = opened_index.insert_or_assign(blob.GetData(), opened.size()).first;
			opened.push_back(std::move(filter_rows));
		}
		opened[entry->second].rows.push_back(UnsafeNumericCast<sel_t>(i));
	}

	auto found = make_unsafe_uniq_array<bool>(count);
	auto batch_keys = make_unsafe_uniq_array<uint64_t>(count);
	auto batch_found = make_unsafe_uniq_array<bool>(count);
	for (auto &filter_rows : opened) {
		auto &keys = probe_keys.Get(filter_rows.format);
		auto key_data = FlatVector::GetData<uint64_t>(*keys.keys);
		const auto row_count = filter_rows.rows.size();
		for (idx_t r = 0; r < row_count; r++) {
			batch_keys[r] = key_data[filter_rows.rows[r]];
		}
		filter_rows.probe->ContainsBatch(batch_keys.get(), row_count, batch_found.get());
		for (idx_t r = 0; r < row_count; r++) {
			const auto row = filter_rows.rows[r];
			found[row] = batch_found[r] && keys.castable.RowIsValid(row);
		}
	}

	auto writer = FlatVector::Writer<bool>(result, count);
	for (idx_t i = 0; i < count; i++) {
		if (is_null[i]) {
			writer.WriteNull();
		} else {
			writer.WriteValue(found[i]);
		}
	}
}

template <uint8_t TYPE_ID>
ScalarFunction MakeContainsFunction() {
	using OP = ContainsFunction<TYPE_ID>;
	ScalarFunction function({LogicalType::BLOB, LogicalType::ANY}, LogicalType::BOOLEAN, OP::Execute);
	function.SetBindCallback(OP::Bind);
	function.SetInitStateCallback(OP::InitLocalState);
	function.SetSerializeCallback(ConstantFilterData::Serialize);
	function.SetDeserializeCallback(ConstantFilterData::Deserialize);
	return function;
}

} // namespace

ScalarFunction BitfiltersContainsFunction(optional_ptr<const BitfilterType> type) {
	if (!type) {
		return MakeContainsFunction<0>();
	}
	switch (*type) {
	case BitfilterType::QUOTIENT:
		return MakeContainsFunction<static_cast<uint8_t>(BitfilterType::QUOTIENT)>();
	case BitfilterType::XOR8:
		return MakeContainsFunction<static_cast<uint8_t>(BitfilterType::XOR8)>();
	case BitfilterType::XOR16:
		return MakeContainsFunction<static_cast<uint8_t>(BitfilterType::XOR16)>();
	case BitfilterType::BINARY_FUSE8:
		return MakeContainsFunction<static_cast<uint8_t>(BitfilterType::BINARY_FUSE8)>();
	case BitfilterType::BINARY_FUSE16:
		return MakeContainsFunction<static_cast<uint8_t>(BitfilterType::BINARY_FUSE16)>();
	case BitfilterType::BLOOM:
		return MakeContainsFunction<static_cast<uint8_t>(BitfilterType::BLOOM)>();
	}
	throw InternalException("unknown bitfilter type");
}

} // namespace duckdb
