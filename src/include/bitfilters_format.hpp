#pragma once

#include "duckdb.hpp"
#include "duckdb/function/function.hpp"

namespace duckdb {

// Filter BLOB format
// ==================
//
// Every filter BLOB starts with a 16 byte header (multi-byte fields little-endian):
//
//   0   "BFLT"           magic
//   4   uint8 version    BITFILTERS_FORMAT_VERSION
//   5   uint8 type       BitfilterType
//   6   uint8 hash       BitfilterHash: how the values became 64-bit keys
//   7   uint8 key_type   LogicalTypeId of the values that were hashed
//   8   uint8 width      DECIMAL width of the values, else 0
//   9   uint8 scale      DECIMAL scale of the values, else 0
//   10  6 bytes          reserved, zero
//   16  payload          the filter itself, laid out by its type
//
// BLOBs written before the header existed are a bare payload. The
// type-specific functions still read them, as filters over UBIGINT keys.

constexpr idx_t BITFILTERS_HEADER_SIZE = 16;
constexpr uint8_t BITFILTERS_FORMAT_VERSION = 1;

enum class BitfilterType : uint8_t {
	QUOTIENT = 1,
	XOR8 = 2,
	XOR16 = 3,
	BINARY_FUSE8 = 4,
	BINARY_FUSE16 = 5,
	BLOOM = 6,
};

//! The aggregate that builds a filter type, e.g. "xor8_filter".
const char *BitfilterTypeName(BitfilterType type);

// How values become 64-bit keys. Filters are stored and probed later, possibly
// by another DuckDB version, so keys must never depend on anything DuckDB is
// free to change -- in particular not on hash(). Values are hashed with XXH64
// of a fixed byte encoding: the Parquet plain encoding where Parquet defines
// one, else DuckDB's on-disk representation of the value.
enum class BitfilterHash : uint8_t {
	//! The values are UBIGINT keys the caller computed, used as they are
	NONE = 0,
	//! The values were hashed with XXH64 of their encoding
	XXH64 = 1,
};

//! How a filter's values became 64-bit keys.
struct BitfilterKeyFormat {
	BitfilterHash hash = BitfilterHash::NONE;
	LogicalTypeId key_type = LogicalTypeId::UBIGINT;
	uint8_t width = 0;
	uint8_t scale = 0;

	//! The format for building a filter from values of `type`. With
	//! `ubigint_keys`, UBIGINT values are taken as keys as they are. Throws a
	//! BinderException for types that have no stable encoding to hash.
	static BitfilterKeyFormat ForValues(const LogicalType &type, bool ubigint_keys);
	//! The type of the values, or LogicalType::INVALID if it cannot be
	//! rebuilt from the header (nested and parameterized types).
	LogicalType KeyLogicalType() const;
	string KeyTypeName() const;
	const char *HashName() const;

	bool operator==(const BitfilterKeyFormat &other) const {
		return hash == other.hash && key_type == other.key_type && width == other.width && scale == other.scale;
	}
	bool operator!=(const BitfilterKeyFormat &other) const {
		return !(*this == other);
	}
};

//! Writes a header for a filter of `type` to `out` (BITFILTERS_HEADER_SIZE bytes).
void BitfiltersWriteHeader(data_ptr_t out, BitfilterType type, const BitfilterKeyFormat &keys);

//! A filter BLOB split into its header and payload.
struct BitfilterBlob {
	//! False for a BLOB written before the header existed
	bool has_header = false;
	uint8_t version = 0;
	BitfilterType type = BitfilterType::QUOTIENT;
	BitfilterKeyFormat keys;
	const_data_ptr_t payload = nullptr;
	idx_t payload_size = 0;

	//! Splits `blob`. A BLOB without the magic is taken to be headerless;
	//! one with the magic but an unknown version or type is an error.
	static BitfilterBlob Parse(const string_t &blob, const char *function);
	//! Splits `blob`, which must be a filter of `type`. A headerless BLOB is
	//! taken to be one.
	static BitfilterBlob Expect(const string_t &blob, BitfilterType type, const char *function);
};

// Keys
// ====

//! Computes the 64-bit key of each of the `count` values in `input` into
//! `keys`, a UBIGINT vector, which ends up flat. NULL values get NULL keys.
void BitfiltersComputeKeys(const BitfilterKeyFormat &format, Vector &input, Vector &keys, idx_t count);

//! The keys to probe a filter with, for the values of one chunk. Values of a
//! different type than the filter was built from are cast to that type first.
//! A value that cannot be cast cannot be in the filter: it is marked in
//! `castable`, and the probe answers false for it.
struct BitfiltersProbeKeys {
	BitfiltersProbeKeys(Vector &values, idx_t count, const char *function);

	struct Keys {
		BitfilterKeyFormat format;
		unique_ptr<Vector> keys;
		ValidityMask castable;
	};

	//! The keys for a filter with `format`, computed on first use.
	const Keys &Get(const BitfilterKeyFormat &format);

private:
	Vector &values;
	idx_t count;
	const char *function;
	vector<unique_ptr<Keys>> computed;
};

// Probing and describing filters
// ==============================

//! What bitfilter_info() and the <type>_info() functions report. Fields a
//! filter type does not have stay NULL.
struct BitfilterInfo {
	string filter_type;
	uint8_t format_version = 0;
	string key_hash;
	string key_type;
	uint64_t size_bytes = 0;
	optional_idx entries;
	optional_idx capacity;
	double load_factor = -1;
	uint8_t fingerprint_bits = 0;
	double expected_fpr = -1;
	uint8_t q = 0;
	uint8_t r = 0;
	optional_idx seed;

	static LogicalType GetType();
	Value ToValue() const;
};

//! An opened filter. It reads the filter's payload in place, so the BLOB must
//! outlive it.
class BitfilterProbe {
public:
	virtual ~BitfilterProbe() = default;
	virtual bool Contains(uint64_t key) const = 0;
	//! result[i] = Contains(keys[i]) for every i < count.
	virtual void ContainsBatch(const uint64_t *keys, idx_t count, bool *result) const = 0;
	//! Fills in the type-specific fields of `info`.
	virtual void Describe(BitfilterInfo &info) const = 0;
};

//! Implements ContainsBatch with a loop the compiler can inline.
template <class DERIVED>
class BitfilterProbeBase : public BitfilterProbe {
public:
	bool Contains(uint64_t key) const override {
		return static_cast<const DERIVED &>(*this).ContainsKey(key);
	}
	void ContainsBatch(const uint64_t *keys, idx_t count, bool *result) const override {
		auto &self = static_cast<const DERIVED &>(*this);
		for (idx_t i = 0; i < count; i++) {
			result[i] = self.ContainsKey(keys[i]);
		}
	}
};

//! Opens the payload of a filter of `type`, validating it; throws
//! InvalidInputException naming `function` if it is not a valid filter.
//! Payloads that have to be copied (old quotient filters) use `allocator`.
unique_ptr<BitfilterProbe> BitfiltersOpen(BitfilterType type, const_data_ptr_t payload, idx_t size,
                                          Allocator &allocator, const char *function);

// Implemented by each filter type's source file.
unique_ptr<BitfilterProbe> QuotientFilterOpen(const_data_ptr_t payload, idx_t size, Allocator &allocator,
                                              const char *function);
unique_ptr<BitfilterProbe> XorFilterOpen(BitfilterType type, const_data_ptr_t payload, idx_t size,
                                         const char *function);
unique_ptr<BitfilterProbe> BinaryFuseFilterOpen(BitfilterType type, const_data_ptr_t payload, idx_t size,
                                                const char *function);
unique_ptr<BitfilterProbe> BloomFilterOpen(const_data_ptr_t payload, idx_t size, const char *function);

//! The body of every <type>_contains function, and of bitfilter_contains when
//! `type` is not given: args are (filter BLOB, value ANY).
void BitfiltersContains(DataChunk &args, ExpressionState &state, Vector &result, optional_ptr<const BitfilterType> type,
                        const char *function);

} // namespace duckdb
