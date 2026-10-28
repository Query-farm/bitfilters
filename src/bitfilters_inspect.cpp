#include "bitfilters_inspect.hpp"
#include "bitfilters_aggregate_util.hpp"
#include "bitfilters_format.hpp"
#include "duckdb/common/vector/vector_iterator.hpp"
#include "duckdb/common/vector/vector_writer.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

namespace duckdb {

namespace {

constexpr BitfilterType ALL_TYPES[] = {BitfilterType::QUOTIENT,     BitfilterType::XOR8,          BitfilterType::XOR16,
                                       BitfilterType::BINARY_FUSE8, BitfilterType::BINARY_FUSE16, BitfilterType::BLOOM};

// The function names are built at load time; they have to outlive the
// functions that report them in errors.
struct InspectNames {
	string contains;
	string info;
	string is_valid;
};

const InspectNames &NamesFor(optional_ptr<const BitfilterType> type) {
	static const auto names = [] {
		vector<InspectNames> result;
		result.push_back({"bitfilter_contains", "bitfilter_info", "bitfilter_is_valid"});
		for (auto type : ALL_TYPES) {
			string base = BitfilterTypeName(type);
			result.push_back({base + "_contains", base + "_info", base + "_is_valid"});
		}
		return result;
	}();
	return type ? names[static_cast<idx_t>(*type)] : names[0];
}

// Opens `blob`, as a filter of `type` if given. Without a type the BLOB must
// carry a header saying what it is.
unique_ptr<BitfilterProbe> OpenBlob(const string_t &blob, optional_ptr<const BitfilterType> type, Allocator &allocator,
                                    const char *function, BitfilterBlob &parsed) {
	parsed = type ? BitfilterBlob::Expect(blob, *type, function) : BitfilterBlob::Parse(blob, function);
	if (!parsed.has_header && !type) {
		throw InvalidInputException("%s: the filter BLOB has no bitfilters header (it was written by an older "
		                            "version); use the function for its filter type, e.g. xor8_filter_info",
		                            function);
	}
	return BitfiltersOpen(parsed.type, parsed.payload, parsed.payload_size, allocator, function);
}

template <const BitfilterType *TYPE>
void InfoFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	optional_ptr<const BitfilterType> type(TYPE);
	auto &allocator = BufferAllocator::Get(state.GetContext());
	const auto function = NamesFor(type).info.c_str();
	const auto count = args.size();
	auto filters = args.data[0].Values<string_t>();
	for (idx_t i = 0; i < count; i++) {
		auto filter = filters[i];
		if (!filter.IsValid()) {
			result.SetValue(i, Value(result.GetType()));
			continue;
		}
		auto blob = filter.GetValue();
		BitfilterBlob parsed;
		auto probe = OpenBlob(blob, type, allocator, function, parsed);
		BitfilterInfo info;
		info.filter_type = BitfilterTypeName(parsed.type);
		info.format_version = parsed.has_header ? parsed.version : 0;
		info.key_hash = parsed.keys.HashName();
		info.key_type = parsed.keys.KeyTypeName();
		info.size_bytes = blob.GetSize();
		probe->Describe(info);
		result.SetValue(i, info.ToValue());
	}
}

template <const BitfilterType *TYPE>
void IsValidFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	optional_ptr<const BitfilterType> type(TYPE);
	auto &allocator = BufferAllocator::Get(state.GetContext());
	const auto function = NamesFor(type).is_valid.c_str();
	const auto count = args.size();
	auto filters = args.data[0].Values<string_t>();
	auto writer = FlatVector::Writer<bool>(result, count);
	for (idx_t i = 0; i < count; i++) {
		auto filter = filters[i];
		if (!filter.IsValid()) {
			writer.WriteNull();
			continue;
		}
		bool valid = true;
		try {
			BitfilterBlob parsed;
			OpenBlob(filter.GetValue(), type, allocator, function, parsed);
		} catch (InvalidInputException &) {
			valid = false;
		}
		writer.WriteValue(valid);
	}
}

FunctionDescription Describe(const string &text, vector<string> names, vector<LogicalType> types, string example) {
	FunctionDescription desc;
	desc.description = text;
	desc.parameter_names = std::move(names);
	desc.parameter_types = std::move(types);
	desc.examples.push_back(std::move(example));
	desc.categories = {"bitfilters", "probabilistic"};
	return desc;
}

void RegisterScalar(ExtensionLoader &loader, const string &name, ScalarFunction function, FunctionDescription desc) {
	ScalarFunctionSet set {Identifier(name)};
	set.AddFunction(std::move(function));
	set.SetFallible();
	CreateScalarFunctionInfo info(std::move(set));
	info.descriptions.push_back(std::move(desc));
	BitfiltersApplyParameterNames(info.functions, info.descriptions);
	loader.RegisterFunction(info);
}

constexpr const char *INFO_FIELDS =
    "filter_type, format_version (0 for a BLOB without header), key_hash (none or xxhash64), "
    "key_type, size_bytes, entries, capacity, load_factor, fingerprint_bits, bits_per_entry, expected_fpr, q, r "
    "and seed; fields a filter type does not have are NULL";

template <BitfilterType TYPE>
struct TypeConstant {
	static constexpr BitfilterType value = TYPE;
};

template <BitfilterType TYPE>
void RegisterTypeFunctions(ExtensionLoader &loader) {
	const string base = BitfilterTypeName(TYPE);
	RegisterScalar(
	    loader, base + "_info",
	    ScalarFunction({LogicalType::BLOB}, BitfilterInfo::GetType(), InfoFunction<&TypeConstant<TYPE>::value>),
	    Describe(StringUtil::Format("Describes a %s BLOB: %s.", base, INFO_FIELDS), {"filter"}, {LogicalType::BLOB},
	             StringUtil::Format("SELECT %s_info(filter) FROM filters", base)));
	RegisterScalar(
	    loader, base + "_is_valid",
	    ScalarFunction({LogicalType::BLOB}, LogicalType::BOOLEAN, IsValidFunction<&TypeConstant<TYPE>::value>),
	    Describe(StringUtil::Format("Returns whether a BLOB is a structurally valid %s, instead of raising the error "
	                                "%s_contains would.",
	                                base, base),
	             {"filter"}, {LogicalType::BLOB}, StringUtil::Format("SELECT %s_is_valid(filter) FROM filters", base)));
}

} // namespace

void LoadBitfiltersInspect(ExtensionLoader &loader) {
	RegisterTypeFunctions<BitfilterType::QUOTIENT>(loader);
	RegisterTypeFunctions<BitfilterType::XOR8>(loader);
	RegisterTypeFunctions<BitfilterType::XOR16>(loader);
	RegisterTypeFunctions<BitfilterType::BINARY_FUSE8>(loader);
	RegisterTypeFunctions<BitfilterType::BINARY_FUSE16>(loader);
	RegisterTypeFunctions<BitfilterType::BLOOM>(loader);

	RegisterScalar(loader, "bitfilter_contains", BitfiltersContainsFunction(nullptr),
	               Describe("Tests if any bitfilters filter may contain a value; the filter's header says what kind "
	                        "of filter it is and how its values were hashed. Returns true if the value might be in "
	                        "the set, or false if it is definitely not.",
	                        {"filter", "value"}, {LogicalType::BLOB, LogicalType::ANY},
	                        "SELECT bitfilter_contains(filter, 42) FROM filters"));
	RegisterScalar(loader, "bitfilter_info",
	               ScalarFunction({LogicalType::BLOB}, BitfilterInfo::GetType(), InfoFunction<nullptr>),
	               Describe(StringUtil::Format("Describes any bitfilters filter BLOB: %s.", INFO_FIELDS), {"filter"},
	                        {LogicalType::BLOB}, "SELECT bitfilter_info(filter) FROM filters"));
	RegisterScalar(loader, "bitfilter_is_valid",
	               ScalarFunction({LogicalType::BLOB}, LogicalType::BOOLEAN, IsValidFunction<nullptr>),
	               Describe("Returns whether a BLOB is a structurally valid bitfilters filter with a header.",
	                        {"filter"}, {LogicalType::BLOB}, "SELECT bitfilter_is_valid(filter) FROM filters"));
}

} // namespace duckdb
