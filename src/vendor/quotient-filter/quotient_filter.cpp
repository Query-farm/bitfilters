/*
 * qf.c
 *
 * Copyright (c) 2014 Vedant Kumar <vsk@berkeley.edu>
 */

#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "quotient_filter.hpp"

#define MAX(a, b)   ((a) > (b) ? (a) : (b))
#define LOW_MASK(n) ((1ULL << (n)) - 1ULL)

QuotientFilter::QuotientFilter(const QuotientFilter &other) : values(other.values), table(other.table) {
}

QuotientFilterValues::QuotientFilterValues(uint32_t q, uint32_t r)
    : qbits(q), rbits(r), elem_bits(r + 3), entries(0), index_mask(LOW_MASK(q)), rmask(LOW_MASK(r)),
      elem_mask(LOW_MASK(elem_bits)), max_size(1ULL << q) {
	assert(q > 0);
	assert(r > 0);
	assert(q + r <= 64);
}

QuotientFilter::QuotientFilter(uint32_t q, uint32_t r) : values(q, r), table(QuotientFilter::table_size(q, r)) {
	std::fill(table.begin(), table.end(), 0);
}

QuotientFilter::QuotientFilter(const std::string &data) {
	// Deserialize the QuotientFilter from a string
	memcpy(&values, data.data(), sizeof(QuotientFilterValues));
	if (data.size() < sizeof(QuotientFilterValues)) {
		throw std::invalid_argument("Data size is too small to contain QuotientFilterValues");
	}
	size_t table_size = QuotientFilter::table_size(values);
	if (data.size() < sizeof(QuotientFilterValues) + table_size) {
		throw std::invalid_argument("Data size is too small to contain QuotientFilter table");
	}
	table.resize(table_size / sizeof(uint64_t));
	memcpy(table.data(), data.data() + sizeof(QuotientFilterValues), table.size() * sizeof(uint64_t));
}

QuotientFilter::QuotientFilter(const char *data, size_t length) {
	memcpy(&values, data, sizeof(QuotientFilterValues));
	if (length < sizeof(QuotientFilterValues)) {
		throw std::invalid_argument("Data size is too small to contain QuotientFilterValues");
	}
	size_t table_size = QuotientFilter::table_size(values);
	if (length < sizeof(QuotientFilterValues) + table_size) {
		throw std::invalid_argument("Data size is too small to contain QuotientFilter table");
	}
	table.resize(table_size / sizeof(uint64_t));
	memcpy(table.data(), data + sizeof(QuotientFilterValues), table.size() * sizeof(uint64_t));
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
inline uint64_t QuotientFilter::find_run_index(uint64_t fq) const {
	/* Find the start of the cluster. */
	uint64_t b = fq;
	while (is_shifted(this->get_elem(b))) {
		b = decr(b);
	}

	/* Find the start of the run for fq. */
	uint64_t s = b;
	while (b != fq) {
		do {
			s = incr(s);
		} while (is_continuation(this->get_elem(s)));

		do {
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
		do {
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
	uint64_t s = this->find_run_index(fq);
	do {
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
	size_t bits = (1 << values.qbits) * (values.rbits + 3);
	size_t bytes = bits / 8;
	return (bits % 8) ? (bytes + 1) : bytes;
}

size_t QuotientFilter::table_size(uint32_t q, uint32_t r) {
	size_t bits = (1 << q) * (r + 3);
	size_t bytes = bits / 8;
	return (bits % 8) ? (bytes + 1) : bytes;
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
	while (!done()) {
		uint64_t elt = qf.get_elem(index);

		/* Keep track of the current run. */
		if (is_cluster_start(elt)) {
			quotient = index;
		} else {
			if (is_run_start(elt)) {
				uint64_t quot = quotient;
				do {
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

	abort();
}
