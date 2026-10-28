#pragma once

#include "duckdb.hpp"

#include <algorithm>

namespace duckdb {

// Whether `keys` looks like it repeats values a lot, judged from a sample of
// evenly spaced keys. The xor and binary fuse constructions struggle with many
// repeated keys -- they make several full passes before they notice -- so the
// keys are de-duplicated up front when the sample shows plenty of repeats.
inline bool BitfiltersLooksRepetitive(const uint64_t *keys, idx_t count) {
	constexpr idx_t SAMPLE = 4096;
	if (count < 2 * SAMPLE) {
		return false;
	}
	vector<uint64_t> sample(SAMPLE);
	const auto stride = count / SAMPLE;
	for (idx_t i = 0; i < SAMPLE; i++) {
		sample[i] = keys[i * stride];
	}
	std::sort(sample.begin(), sample.end());
	idx_t repeats = 0;
	for (idx_t i = 1; i < SAMPLE; i++) {
		repeats += sample[i] == sample[i - 1];
	}
	return repeats > SAMPLE / 100;
}

// Sorts `keys` and drops repeats; returns how many distinct keys remain.
inline idx_t BitfiltersSortUnique(uint64_t *keys, idx_t count) {
	std::sort(keys, keys + count);
	return static_cast<idx_t>(std::unique(keys, keys + count) - keys);
}

} // namespace duckdb
