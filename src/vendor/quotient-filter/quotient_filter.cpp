/*
 * qf.c
 *
 * Copyright (c) 2014 Vedant Kumar <vsk@berkeley.edu>
 */

#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "duckdb.hpp"

#include "quotient_filter.hpp"

#define MAX(a, b)   ((a) > (b) ? (a) : (b))
#define LOW_MASK(n) ((1ULL << (n)) - 1ULL)

QuotientFilter::QuotientFilter(const QuotientFilter &other) : values(other.values), table(other.table) {
}

QuotientFilterValues::QuotientFilterValues(uint32_t q, uint32_t r)
    : qbits(q), rbits(r), elem_bits(r + 3), reserved(0), entries(0), index_mask(LOW_MASK(q)), rmask(LOW_MASK(r)),
      elem_mask(LOW_MASK(elem_bits)), max_size(1ULL << q) {
	assert(q > 0);
	assert(r > 0);
	assert(q + r <= 64);
}

QuotientFilter::QuotientFilter(uint32_t q, uint32_t r) : values(q, r), table(QuotientFilter::table_elements(q, r), 0) {
}

QuotientFilter::QuotientFilter(const std::string &data) : QuotientFilter(data.data(), data.size()) {
}

QuotientFilter::QuotientFilter(const char *data, size_t length) {
	// Validate before copying anything: the header is read straight out of the
	// buffer and drives every subsequent table index.
	QuotientFilter::validate_serialized(data, length);
	memcpy(&values, data, sizeof(QuotientFilterValues));

	// Round the element count up. The last uint64_t is partially used whenever
	// the table's bit count is not a multiple of 64, and get_elem/set_elem still
	// address it -- rounding down leaves the filter reading past its own table.
	size_t table_bytes = QuotientFilter::table_size(values);
	table.assign(QuotientFilter::table_elements(values), 0);
	memcpy(table.data(), data + sizeof(QuotientFilterValues), table_bytes);
}

/* Return QF[idx] in the lower bits. */
inline uint64_t QuotientFilter::get_elem(uint64_t idx) const {
	uint64_t elt = 0;
	size_t bitpos = values.elem_bits * idx;
	size_t tabpos = bitpos / 64;
	size_t slotpos = bitpos % 64;
	int spillbits = (slotpos + values.elem_bits) - 64;
	elt = (table[tabpos] >> slotpos) & values.elem_mask;
	if (spillbits > 0) {
		++tabpos;
		uint64_t x = table[tabpos] & LOW_MASK(spillbits);
		elt |= x << (values.elem_bits - spillbits);
	}
	return elt;
}

/* Store the lower bits of elt into QF[idx]. */
void QuotientFilter::set_elem(uint64_t idx, uint64_t elt) {
	size_t bitpos = values.elem_bits * idx;
	size_t tabpos = bitpos / 64;
	size_t slotpos = bitpos % 64;
	int spillbits = (slotpos + values.elem_bits) - 64;
	elt &= values.elem_mask;
	table[tabpos] &= ~(values.elem_mask << slotpos);
	table[tabpos] |= elt << slotpos;
	if (spillbits > 0) {
		++tabpos;
		table[tabpos] &= ~LOW_MASK(spillbits);
		table[tabpos] |= elt >> (values.elem_bits - spillbits);
	}
}

inline uint64_t QuotientFilter::incr(uint64_t idx) const {
	return (idx + 1) & values.index_mask;
}

inline uint64_t QuotientFilter::decr(uint64_t idx) const {
	return (idx - 1) & values.index_mask;
}

static inline int is_occupied(uint64_t elt) {
	return elt & 1;
}

static inline uint64_t set_occupied(uint64_t elt) {
	return elt | 1;
}

static inline uint64_t clr_occupied(uint64_t elt) {
	return elt & ~1;
}

static inline int is_continuation(uint64_t elt) {
	return elt & 2;
}

static inline uint64_t set_continuation(uint64_t elt) {
	return elt | 2;
}

static inline uint64_t clr_continuation(uint64_t elt) {
	return elt & ~2;
}

static inline int is_shifted(uint64_t elt) {
	return elt & 4;
}

static inline uint64_t set_shifted(uint64_t elt) {
	return elt | 4;
}

static inline uint64_t clr_shifted(uint64_t elt) {
	return elt & ~4;
}

static inline uint64_t get_remainder(uint64_t elt) {
	return elt >> 3;
}

static inline bool is_empty_element(uint64_t elt) {
	return (elt & 7) == 0;
}

static inline bool is_cluster_start(uint64_t elt) {
	return is_occupied(elt) && !is_continuation(elt) && !is_shifted(elt);
}

static inline bool is_run_start(uint64_t elt) {
	return !is_continuation(elt) && (is_occupied(elt) || is_shifted(elt));
}

inline uint64_t QuotientFilter::hash_to_quotient(uint64_t hash) const {
	return (hash >> values.rbits) & values.index_mask;
}

inline uint64_t QuotientFilter::hash_to_remainder(uint64_t hash) const {
	return hash & values.rmask;
}

/* Find the start index of the run for fq (given that the run exists). */
/*
 * Every scan below walks slots with incr()/decr(), which mask the index, so a
 * corrupt table can never read out of bounds -- but it can make a scan wrap
 * forever. No scan of a well-formed filter visits more slots than the table
 * has, so tripping this limit means the BLOB's slot metadata is not a filter.
 */
static void QuotientFilterScanStep(uint64_t &steps, uint64_t limit) {
	if (++steps > limit) {
		throw duckdb::InvalidInputException(
		    "quotient filter BLOB is corrupt: its slot metadata does not describe a valid filter");
	}
}

inline uint64_t QuotientFilter::find_run_index(uint64_t fq) const {
	const uint64_t limit = this->values.max_size;
	uint64_t steps = 0;

	/* Find the start of the cluster. */
	uint64_t b = fq;
	while (is_shifted(this->get_elem(b))) {
		QuotientFilterScanStep(steps, limit);
		b = decr(b);
	}

	/* Find the start of the run for fq. */
	uint64_t s = b;
	while (b != fq) {
		do {
			QuotientFilterScanStep(steps, limit);
			s = incr(s);
		} while (is_continuation(this->get_elem(s)));

		do {
			QuotientFilterScanStep(steps, limit);
			b = incr(b);
		} while (!is_occupied(this->get_elem(b)));
	}
	return s;
}

/* Insert elt into QF[s], shifting over elements as necessary. */
void QuotientFilter::insert_into(uint64_t s, uint64_t elt) {
	uint64_t prev;
	uint64_t curr = elt;
	bool empty;

	do {
		prev = this->get_elem(s);
		empty = is_empty_element(prev);
		if (!empty) {
			/* Fix up `is_shifted' and `is_occupied'. */
			prev = set_shifted(prev);
			if (is_occupied(prev)) {
				curr = set_occupied(curr);
				prev = clr_occupied(prev);
			}
		}
		this->set_elem(s, curr);
		curr = prev;
		s = this->incr(s);
	} while (!empty);
}

bool QuotientFilter::insert(uint64_t hash) {
	if (this->values.entries >= this->values.max_size) {
		return false;
	}

	uint64_t fq = this->hash_to_quotient(hash);
	uint64_t fr = this->hash_to_remainder(hash);
	uint64_t T_fq = this->get_elem(fq);
	uint64_t entry = (fr << 3) & ~7;

	/* Special-case filling canonical slots to simplify insert_into(). */
	if (is_empty_element(T_fq)) {
		this->set_elem(fq, set_occupied(entry));
		++this->values.entries;
		return true;
	}

	if (!is_occupied(T_fq)) {
		this->set_elem(fq, set_occupied(T_fq));
	}

	uint64_t start = this->find_run_index(fq);
	uint64_t s = start;

	if (is_occupied(T_fq)) {
		/* Move the cursor to the insert position in the fq run. */
		uint64_t steps = 0;
		do {
			QuotientFilterScanStep(steps, this->values.max_size);
			uint64_t rem = get_remainder(this->get_elem(s));
			if (rem == fr) {
				return true;
			} else if (rem > fr) {
				break;
			}
			s = this->incr(s);
		} while (is_continuation(this->get_elem(s)));

		if (s == start) {
			/* The old start-of-run becomes a continuation. */
			uint64_t old_head = this->get_elem(start);
			this->set_elem(start, set_continuation(old_head));
		} else {
			/* The new element becomes a continuation. */
			entry = set_continuation(entry);
		}
	}

	/* Set the shifted bit if we can't use the canonical slot. */
	if (s != fq) {
		entry = set_shifted(entry);
	}

	this->insert_into(s, entry);
	++this->values.entries;
	return true;
}

bool QuotientFilter::may_contain(uint64_t hash) const {
	uint64_t fq = this->hash_to_quotient(hash);
	uint64_t fr = this->hash_to_remainder(hash);
	uint64_t T_fq = this->get_elem(fq);

	/* If this quotient has no run, give up. */
	if (!is_occupied(T_fq)) {
		return false;
	}

	/* Scan the sorted run for the target remainder. */
	uint64_t steps = 0;
	uint64_t s = this->find_run_index(fq);
	do {
		QuotientFilterScanStep(steps, this->values.max_size);
		uint64_t rem = get_remainder(this->get_elem(s));
		if (rem == fr) {
			return true;
		} else if (rem > fr) {
			return false;
		}
		s = this->incr(s);
	} while (is_continuation(this->get_elem(s)));
	return false;
}

/* Remove the entry in QF[s] and slide the rest of the cluster forward. */
void QuotientFilter::delete_entry(uint64_t s, uint64_t quot) {
	uint64_t next;
	uint64_t curr = this->get_elem(s);
	uint64_t sp = this->incr(s);
	uint64_t orig = s;

	/*
	 * FIXME(vsk): This loop looks ugly. Rewrite.
	 */
	while (true) {
		next = this->get_elem(sp);
		bool curr_occupied = is_occupied(curr);

		if (is_empty_element(next) || is_cluster_start(next) || sp == orig) {
			this->set_elem(s, 0);
			return;
		} else {
			/* Fix entries which slide into canonical slots. */
			uint64_t updated_next = next;
			if (is_run_start(next)) {
				do {
					quot = this->incr(quot);
				} while (!is_occupied(this->get_elem(quot)));

				if (curr_occupied && quot == s) {
					updated_next = clr_shifted(next);
				}
			}

			this->set_elem(s, curr_occupied ? set_occupied(updated_next) : clr_occupied(updated_next));
			s = sp;
			sp = this->incr(sp);
			curr = next;
		}
	}
}

bool QuotientFilter::remove(uint64_t hash) {
	uint64_t highbits = hash >> (this->values.qbits + this->values.rbits);
	if (highbits) {
		return false;
	}

	uint64_t fq = this->hash_to_quotient(hash);
	uint64_t fr = this->hash_to_remainder(hash);
	uint64_t T_fq = this->get_elem(fq);

	if (!is_occupied(T_fq) || this->values.entries == 0) {
		return true;
	}

	uint64_t start = this->find_run_index(fq);
	uint64_t s = start;
	uint64_t rem;

	/* Find the offending table index (or give up). */
	do {
		rem = get_remainder(this->get_elem(s));
		if (rem == fr) {
			break;
		} else if (rem > fr) {
			return true;
		}
		s = this->incr(s);
	} while (is_continuation(this->get_elem(s)));
	if (rem != fr) {
		return true;
	}

	uint64_t kill = (s == fq) ? T_fq : this->get_elem(s);
	bool replace_run_start = is_run_start(kill);

	/* If we're deleting the last entry in a run, clear `is_occupied'. */
	if (is_run_start(kill)) {
		uint64_t next = this->get_elem(this->incr(s));
		if (!is_continuation(next)) {
			T_fq = clr_occupied(T_fq);
			this->set_elem(fq, T_fq);
		}
	}

	this->delete_entry(s, fq);

	if (replace_run_start) {
		uint64_t next = this->get_elem(s);
		uint64_t updated_next = next;
		if (is_continuation(next)) {
			/* The new start-of-run is no longer a continuation. */
			updated_next = clr_continuation(next);
		}
		if (s == fq && is_run_start(updated_next)) {
			/* The new start-of-run is in the canonical slot. */
			updated_next = clr_shifted(updated_next);
		}
		if (updated_next != next) {
			this->set_elem(s, updated_next);
		}
	}

	--this->values.entries;
	return true;
}

QuotientFilter QuotientFilter::merge(const QuotientFilter &other) const {
	// I don't think we should increase q here, since we are merging two filters
	// with the same q. If we increase q, we might end up with a filter

	//	uint32_t q = 1 + MAX(this->qbits, other.qbits);
	uint32_t q = MAX(this->values.qbits, other.values.qbits);
	uint32_t r = MAX(this->values.rbits, other.values.rbits);
	QuotientFilter out(q, r);

	QuotientFilterIterator qfi(*this);
	while (!qfi.done()) {
		out.insert(qfi.next());
	}
	QuotientFilterIterator qfi2(other);
	while (!qfi2.done()) {
		out.insert(qfi2.next());
	}
	return out;
}

void QuotientFilter::clear() {
	this->values.entries = 0;
	std::fill(this->table.begin(), this->table.end(), 0);
}

size_t QuotientFilter::table_size(const QuotientFilterValues &values) {
	return QuotientFilter::table_size(values.qbits, values.rbits);
}

size_t QuotientFilter::table_size(uint32_t q, uint32_t r) {
	// 1ULL, not 1: q can be up to 63, and shifting an int that far is undefined
	// behaviour (it produced garbage table sizes, and crashes, for q >= 31).
	uint64_t bits = (1ULL << q) * (uint64_t)(r + 3);
	uint64_t bytes = bits / 8;
	return (size_t)((bits % 8) ? (bytes + 1) : bytes);
}

size_t QuotientFilter::table_elements(const QuotientFilterValues &values) {
	return QuotientFilter::table_elements(values.qbits, values.rbits);
}

size_t QuotientFilter::table_elements(uint32_t q, uint32_t r) {
	uint64_t bits = (1ULL << q) * (uint64_t)(r + 3);
	return (size_t)((bits + 63) / 64);
}

void QuotientFilter::validate_serialized(const char *data, size_t length) {
	if (length < sizeof(QuotientFilterValues)) {
		throw duckdb::InvalidInputException(
		    "quotient filter BLOB is %llu bytes, which is smaller than the %llu byte header", (uint64_t)length,
		    (uint64_t)sizeof(QuotientFilterValues));
	}

	QuotientFilterValues header;
	memcpy(&header, data, sizeof(QuotientFilterValues));

	// q + r <= 64 with both non-zero, exactly as the constructor requires.
	if (header.qbits == 0 || header.rbits == 0 || (uint32_t)header.qbits + (uint32_t)header.rbits > 64) {
		throw duckdb::InvalidInputException(
		    "quotient filter BLOB has invalid parameters (q = %u, r = %u); q and r must be non-zero and q + r <= 64",
		    (uint32_t)header.qbits, (uint32_t)header.rbits);
	}
	// The derived fields are redundant with q and r, so they must agree. They
	// are what may_contain() masks its table indices with.
	if (header.elem_bits != header.rbits + 3 || header.index_mask != LOW_MASK(header.qbits) ||
	    header.rmask != LOW_MASK(header.rbits) || header.elem_mask != LOW_MASK(header.elem_bits) ||
	    header.max_size != (1ULL << header.qbits)) {
		throw duckdb::InvalidInputException("quotient filter BLOB has a header inconsistent with q = %u and r = %u",
		                                    (uint32_t)header.qbits, (uint32_t)header.rbits);
	}
	if ((uint64_t)header.entries > header.max_size) {
		throw duckdb::InvalidInputException(
		    "quotient filter BLOB claims %llu entries, which exceeds its capacity of %llu", (uint64_t)header.entries,
		    header.max_size);
	}

	// (1ULL << qbits) * (rbits + 3) must not wrap; a filter that large cannot
	// fit in a BLOB anyway, but the multiplication has to stay well defined.
	uint64_t slots = 1ULL << header.qbits;
	uint64_t bits_per_slot = (uint64_t)header.rbits + 3;
	if (slots > UINT64_MAX / bits_per_slot) {
		throw duckdb::InvalidInputException("quotient filter BLOB describes a table too large to address (q = %u)",
		                                    (uint32_t)header.qbits);
	}
	uint64_t table_bytes = (slots * bits_per_slot + 7) / 8;
	if (length - sizeof(QuotientFilterValues) < table_bytes) {
		throw duckdb::InvalidInputException(
		    "quotient filter BLOB describes a table of %llu bytes (q = %u, r = %u) but the BLOB only holds %llu bytes "
		    "after its %llu byte header",
		    table_bytes, (uint32_t)header.qbits, (uint32_t)header.rbits,
		    (uint64_t)(length - sizeof(QuotientFilterValues)), (uint64_t)sizeof(QuotientFilterValues));
	}
}

QuotientFilterIterator::QuotientFilterIterator(const QuotientFilter &qf) : qf(qf) {
	visited = qf.values.entries;

	if (qf.values.entries == 0) {
		return;
	}

	/* Find the start of a cluster. */
	uint64_t start;
	for (start = 0; start < qf.values.max_size; ++start) {
		if (is_cluster_start(qf.get_elem(start))) {
			break;
		}
	}

	visited = 0;
	index = start;
}

bool QuotientFilterIterator::done() const {
	return qf.values.entries == visited;
}

uint64_t QuotientFilterIterator::next() {
	uint64_t steps = 0;
	while (!done()) {
		QuotientFilterScanStep(steps, qf.values.max_size);
		uint64_t elt = qf.get_elem(index);

		/* Keep track of the current run. */
		if (is_cluster_start(elt)) {
			quotient = index;
		} else {
			if (is_run_start(elt)) {
				uint64_t quot = quotient;
				uint64_t run_steps = 0;
				do {
					QuotientFilterScanStep(run_steps, qf.values.max_size);
					quot = qf.incr(quot);
				} while (!is_occupied(qf.get_elem(quot)));
				quotient = quot;
			}
		}

		index = qf.incr(index);

		if (!is_empty_element(elt)) {
			uint64_t quot = quotient;
			uint64_t rem = get_remainder(elt);
			uint64_t hash = (quot << qf.values.rbits) | rem;
			++visited;
			return hash;
		}
	}

	// Reached when the filter claims more entries than its table holds, which
	// only a corrupt BLOB can do. Aborting here would take the process down.
	throw duckdb::InvalidInputException(
	    "quotient filter BLOB is corrupt: it claims more entries than its table contains");
}
