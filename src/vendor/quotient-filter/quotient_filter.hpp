/*
 * qf.h
 *
 * Copyright (c) 2014 Vedant Kumar <vsk@berkeley.edu>
 */

#pragma once

#include <stdint.h>
#include <stdbool.h>
#include <vector>
#include <string>

#include "duckdb/common/allocator.hpp"

struct QuotientFilterValues {
	explicit QuotientFilterValues() : QuotientFilterValues(1, 2) {
	}

	QuotientFilterValues(uint32_t q, uint32_t r);
	uint8_t qbits;
	uint8_t rbits;
	uint8_t elem_bits;
	// Occupies what would otherwise be a padding byte. The struct is serialized
	// verbatim, so naming the byte keeps it initialized instead of writing
	// indeterminate memory into the filter BLOB.
	uint8_t reserved;
	uint32_t entries;
	uint64_t index_mask;
	uint64_t rmask;
	uint64_t elem_mask;
	uint64_t max_size;
};

class QuotientFilter {
public:
	/*
	 * An empty filter with 2^q slots and r remainder bits per slot. The table
	 * is allocated from `allocator`, which in an aggregate is DuckDB's buffer
	 * managed allocator, so it counts against memory_limit.
	 */
	QuotientFilter(uint32_t q, uint32_t r, duckdb::Allocator &allocator);

	/*
	 * A filter read from a serialize()d BLOB. The BLOB is validated first.
	 *
	 * The table is not copied when the BLOB holds it as whole 64-bit words
	 * (every BLOB serialize() produces does): the filter then reads straight
	 * out of `data`, which must outlive it, and is read-only. Older BLOBs whose
	 * table ends mid-word are copied into a table from `allocator`.
	 */
	QuotientFilter(const char *data, size_t length, duckdb::Allocator &allocator);

	/* A writable deep copy of `other`. */
	QuotientFilter(const QuotientFilter &other, duckdb::Allocator &allocator);

	QuotientFilter(const QuotientFilter &) = delete;
	QuotientFilter &operator=(const QuotientFilter &) = delete;

	/*
	 * Inserts a hash into the QF.
	 * Only the lowest q+r bits are actually inserted into the QF table.
	 *
	 * Returns false if the QF is full and does not already contain the hash.
	 * A filter keeps at least one slot empty (a table with no empty slot has
	 * no cluster boundaries, so lookups cannot terminate), so it holds at most
	 * capacity() = 2^q - 1 entries.
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
	 * Inserts every entry of `other` into this filter, in place.
	 *
	 * A filter only remembers the low q+r bits of each hash, so `other` must
	 * carry at least as many fingerprint bits as this filter uses
	 * (other.q + other.r >= q + r); merging from a filter with fewer bits would
	 * store fingerprints that lookups can never match. Throws
	 * InvalidInputException if that does not hold, or if this filter fills up.
	 */
	void merge_from(const QuotientFilter &other);

	/*
	 * Resets the QF table. This function does not deallocate any memory.
	 */
	void clear();

	uint32_t q() const {
		return values.qbits;
	}
	uint32_t r() const {
		return values.rbits;
	}
	uint64_t entries() const {
		return values.entries;
	}
	uint64_t capacity() const {
		return values.max_size - 1;
	}

	/*
	 * Finds the size (in bytes) of a QF table.
	 *
	 * Caution: sizeof(struct quotient_filter) is not included.
	 */
	static size_t table_size(uint32_t q, uint32_t r);
	static size_t table_size(const QuotientFilterValues &values);

	/*
	 * Number of uint64_t slots needed to back a table of `q` quotient bits and
	 * `r` remainder bits. This rounds up: the last element is partially used
	 * whenever the bit count is not a multiple of 64, and get_elem/set_elem
	 * still address it.
	 */
	static size_t table_elements(uint32_t q, uint32_t r);
	static size_t table_elements(const QuotientFilterValues &values);

	/*
	 * Size of a serialized filter with these parameters: the header plus the
	 * table as whole 64-bit words.
	 */
	static size_t serialized_size(uint32_t q, uint32_t r);
	size_t serialized_size() const {
		return serialized_size(values.qbits, values.rbits);
	}

	/* Writes serialized_size() bytes to `out`. */
	void serialize_into(char *out) const;

	/*
	 * Throws if `data` is not a well-formed serialization produced by
	 * serialize(). Every deserializing constructor runs this first: the header
	 * is copied verbatim out of the BLOB, and an inconsistent one would make
	 * may_contain() index the table out of bounds.
	 */
	static void validate_serialized(const char *data, size_t length);

	friend class QuotientFilterIterator;

private:
	QuotientFilterValues values;
	// The table as table_elements() 64-bit words. Either `owned_table` (a
	// writable, aligned table this filter allocated) or a read-only view of a
	// BLOB, which carries no alignment guarantee -- so words are always
	// accessed through load_word/store_word rather than as uint64_t.
	duckdb::AllocatedData owned_table;
	duckdb::const_data_ptr_t table;

	void allocate_table(duckdb::Allocator &allocator);
	uint64_t load_word(size_t idx) const;
	void store_word(size_t idx, uint64_t word);

	void set_elem(uint64_t idx, uint64_t elt);
	uint64_t get_elem(uint64_t idx) const;
	uint64_t incr(uint64_t idx) const;
	uint64_t decr(uint64_t idx) const;
	uint64_t hash_to_quotient(uint64_t hash) const;
	uint64_t hash_to_remainder(uint64_t hash) const;
	uint64_t find_run_index(uint64_t fq) const;
	void insert_into(uint64_t s, uint64_t elt);
	void delete_entry(uint64_t s, uint64_t quot);
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
