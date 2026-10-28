#pragma once

#include "duckdb.hpp"

namespace duckdb {

// XXH64 (https://github.com/Cyan4973/xxHash), the hash the Parquet
// specification uses for its split block Bloom filters.
struct BitfiltersXXH64 {
	static constexpr uint64_t PRIME1 = 0x9E3779B185EBCA87ULL;
	static constexpr uint64_t PRIME2 = 0xC2B2AE3D27D4EB4FULL;
	static constexpr uint64_t PRIME3 = 0x165667B19E3779F9ULL;
	static constexpr uint64_t PRIME4 = 0x85EBCA77C2B2AE63ULL;
	static constexpr uint64_t PRIME5 = 0x27D4EB2F165667C5ULL;

	static inline uint64_t Rotl(uint64_t x, int r) {
		return (x << r) | (x >> (64 - r));
	}

	static inline uint64_t Read64(const_data_ptr_t p) {
		uint64_t v;
		memcpy(&v, p, sizeof(v));
		return v;
	}

	static inline uint32_t Read32(const_data_ptr_t p) {
		uint32_t v;
		memcpy(&v, p, sizeof(v));
		return v;
	}

	static inline uint64_t Round(uint64_t acc, uint64_t input) {
		acc += input * PRIME2;
		acc = Rotl(acc, 31);
		return acc * PRIME1;
	}

	static inline uint64_t MergeRound(uint64_t acc, uint64_t val) {
		acc ^= Round(0, val);
		return acc * PRIME1 + PRIME4;
	}

	static uint64_t Hash(const_data_ptr_t p, idx_t len, uint64_t seed = 0) {
		const auto end = p + len;
		uint64_t h;
		if (len >= 32) {
			const auto limit = end - 32;
			uint64_t v1 = seed + PRIME1 + PRIME2;
			uint64_t v2 = seed + PRIME2;
			uint64_t v3 = seed;
			uint64_t v4 = seed - PRIME1;
			do {
				v1 = Round(v1, Read64(p));
				v2 = Round(v2, Read64(p + 8));
				v3 = Round(v3, Read64(p + 16));
				v4 = Round(v4, Read64(p + 24));
				p += 32;
			} while (p <= limit);
			h = Rotl(v1, 1) + Rotl(v2, 7) + Rotl(v3, 12) + Rotl(v4, 18);
			h = MergeRound(h, v1);
			h = MergeRound(h, v2);
			h = MergeRound(h, v3);
			h = MergeRound(h, v4);
		} else {
			h = seed + PRIME5;
		}
		h += static_cast<uint64_t>(len);
		while (p + 8 <= end) {
			h ^= Round(0, Read64(p));
			h = Rotl(h, 27) * PRIME1 + PRIME4;
			p += 8;
		}
		if (p + 4 <= end) {
			h ^= static_cast<uint64_t>(Read32(p)) * PRIME1;
			h = Rotl(h, 23) * PRIME2 + PRIME3;
			p += 4;
		}
		while (p < end) {
			h ^= static_cast<uint64_t>(*p) * PRIME5;
			h = Rotl(h, 11) * PRIME1;
			p++;
		}
		h ^= h >> 33;
		h *= PRIME2;
		h ^= h >> 29;
		h *= PRIME3;
		h ^= h >> 32;
		return h;
	}
};

} // namespace duckdb
