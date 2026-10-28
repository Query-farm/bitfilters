#pragma once

#include "duckdb.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/function/aggregate_state.hpp"
#include "duckdb/storage/arena_allocator.hpp"

namespace duckdb {

// Memory accounting
// =================
//
// Every aggregate in this extension gets its memory from the arena DuckDB
// hands the aggregate (AggregateInputData::allocator). In both the grouped and
// the ungrouped aggregate operators that arena sits on top of the buffer
// manager's allocator, so whatever we allocate from it is charged against
// memory_limit and fails with an Out of Memory error instead of growing the
// process past the limit.
//
// The arena is a bump allocator: nothing allocated from it is released until
// the operator finishes. Memory that must be released earlier -- scratch space
// while a filter is built -- comes from the arena's *underlying* allocator
// instead: the same buffer-managed allocator, with the same accounting, but
// individually freeable.
inline Allocator &BitfiltersStateAllocator(AggregateInputData &input) {
	return input.allocator.GetUnderlyingAllocator();
}

// Routing the vendored fastfilter allocations through DuckDB
// ==========================================================
//
// Building an xor or binary fuse filter allocates scratch space several times
// the size of the input (the peeling graph, the reverse order stack, ...) with
// malloc. The vendored headers call FASTFILTER_MALLOC / FASTFILTER_CALLOC /
// FASTFILTER_FREE, which bitfilters points at the functions below.
//
// While a BitfiltersFastFilterAllocationScope is alive on the current thread
// those allocations come from the scope's allocator, so they count against
// memory_limit. The vendored code checks every allocation for NULL and backs
// out cleanly, so a failed allocation returns NULL rather than throwing through
// C code that would leak whatever it had already allocated. The scope keeps
// the first error, and ThrowIfFailed() rethrows it once the vendored call has
// returned.
class BitfiltersFastFilterAllocationScope {
public:
	explicit BitfiltersFastFilterAllocationScope(Allocator &allocator);
	~BitfiltersFastFilterAllocationScope();

	BitfiltersFastFilterAllocationScope(const BitfiltersFastFilterAllocationScope &) = delete;
	BitfiltersFastFilterAllocationScope &operator=(const BitfiltersFastFilterAllocationScope &) = delete;

	Allocator &GetAllocator() {
		return allocator;
	}

	//! Rethrows the first allocation failure seen while the scope was active.
	void ThrowIfFailed();

	//! Records an allocation failure; only the first one is kept.
	void SetError(ErrorData error_p);

private:
	Allocator &allocator;
	BitfiltersFastFilterAllocationScope *previous;
	ErrorData error;
};

void *BitfiltersFastFilterMalloc(size_t size);
void *BitfiltersFastFilterCalloc(size_t count, size_t size);
void BitfiltersFastFilterFree(void *pointer);

} // namespace duckdb

#define FASTFILTER_MALLOC(size)        ::duckdb::BitfiltersFastFilterMalloc(size)
#define FASTFILTER_CALLOC(count, size) ::duckdb::BitfiltersFastFilterCalloc(count, size)
#define FASTFILTER_FREE(pointer)       ::duckdb::BitfiltersFastFilterFree(pointer)
