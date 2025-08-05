/*
 * qf.h
 *
 * Copyright (c) 2014 Vedant Kumar <vsk@berkeley.edu>
 */

#pragma once

#include <stdint.h>
#include <stdbool.h>
#include <vector>

struct QuotientFilterValues {

	explicit QuotientFilterValues() : QuotientFilterValues(1, 2) {
	}

	QuotientFilterValues(uint32_t q, uint32_t r);
	uint8_t qbits;
	uint8_t rbits;
	uint8_t elem_bits;
	uint32_t entries;
	uint64_t index_mask;
	uint64_t rmask;
	uint64_t elem_mask;
	uint64_t max_size;
};

class QuotientFilter {
public:
	QuotientFilter(const QuotientFilter &other);
	QuotientFilter(uint32_t q, uint32_t r);
	QuotientFilter(const std::string &data);
	QuotientFilter(const char *data, size_t length);

	/*
	 * Inserts a hash into the QF.
	 * Only the lowest q+r bits are actually inserted into the QF table.
	 *
	 * Returns false if the QF is full.
	 */
	bool insert(uint64_t hash);

	/*
	 * Removes a hash from the QF.
	 *
	 * Caution: If you plan on using this function, make sure that your hash
	 * function emits no more than q+r bits. Consider the following scenario;
	 *
	 * Returns false if the hash uses more than q+r bits.
	 */
	bool remove(uint64_t hash);

	bool may_contain(uint64_t hash) const;

	/*
	 * Initializes copies over all elements from another QuotientFilter.
	 */
	QuotientFilter merge(const QuotientFilter &other) const;

	/*
	 * Resets the QF table. This function does not deallocate any memory.
	 */
	void clear();

	/*
	 * Finds the size (in bytes) of a QF table.
	 *
	 * Caution: sizeof(struct quotient_filter) is not included.
	 */
	static size_t table_size(uint32_t q, uint32_t r);
	static size_t table_size(const QuotientFilterValues &values);
	friend class QuotientFilterIterator;

private:
	QuotientFilterValues values;
	std::vector<uint64_t> table;

	void set_elem(uint64_t idx, uint64_t elt);
	uint64_t get_elem(uint64_t idx) const;
	uint64_t incr(uint64_t idx) const;
	uint64_t decr(uint64_t idx) const;
	uint64_t hash_to_quotient(uint64_t hash) const;
	uint64_t hash_to_remainder(uint64_t hash) const;
	uint64_t find_run_index(uint64_t fq) const;
	void insert_into(uint64_t s, uint64_t elt);
	void delete_entry(uint64_t s, uint64_t quot);

public:
	[[nodiscard]] std::string serialize() const {
		// Serialize the QuotientFilter to a string
		size_t needed_bytes = sizeof(QuotientFilterValues) + table_size(values);
		std::string out(needed_bytes, '\0');
		memcpy((char *)out.data(), &values, sizeof(QuotientFilterValues));
		memcpy((char *)out.data() + sizeof(QuotientFilterValues), table.data(), table.size());
		return out;
	}

	[[nodiscard]] static QuotientFilter deserialize(const std::string &data) {
		// Deserialize the QuotientFilter from a string
		QuotientFilter qf(data);
		return qf;
	}
};

class QuotientFilterIterator {

public:
	QuotientFilterIterator(const QuotientFilter &qf);
	bool done() const;
	uint64_t next();

private:
	const QuotientFilter &qf;
	uint64_t index;
	uint64_t quotient;
	uint64_t visited;
};
