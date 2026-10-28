#include "bitfilters_memory.hpp"

namespace duckdb {

namespace {

// The innermost allocation scope on this thread, or nullptr.
thread_local BitfiltersFastFilterAllocationScope *current_scope = nullptr;

// Each allocation is preceded by a header recording which allocator it came
// from and how large it is, because free() is only handed the pointer. The
// header is 16 bytes so the returned pointer keeps malloc's alignment.
struct FastFilterAllocationHeader {
	Allocator *allocator;
	uint64_t size;
};
static_assert(sizeof(FastFilterAllocationHeader) == 16, "header must preserve 16 byte alignment");

void *FastFilterAllocate(size_t size, bool zero) {
	if (size > NumericLimits<uint64_t>::Maximum() - sizeof(FastFilterAllocationHeader)) {
		return nullptr;
	}
	auto scope = current_scope;
	auto &allocator = scope ? scope->GetAllocator() : Allocator::DefaultAllocator();
	const auto total = sizeof(FastFilterAllocationHeader) + size;
	data_ptr_t base;
	try {
		base = allocator.AllocateData(total);
	} catch (std::exception &ex) {
		if (scope) {
			scope->SetError(ErrorData(ex));
		}
		return nullptr;
	}
	if (!base) {
		return nullptr;
	}
	auto header = reinterpret_cast<FastFilterAllocationHeader *>(base);
	header->allocator = &allocator;
	header->size = total;
	auto result = base + sizeof(FastFilterAllocationHeader);
	if (zero) {
		memset(result, 0, size);
	}
	return result;
}

} // namespace

BitfiltersFastFilterAllocationScope::BitfiltersFastFilterAllocationScope(Allocator &allocator_p)
    : allocator(allocator_p), previous(current_scope) {
	current_scope = this;
}

BitfiltersFastFilterAllocationScope::~BitfiltersFastFilterAllocationScope() {
	current_scope = previous;
}

void BitfiltersFastFilterAllocationScope::SetError(ErrorData error_p) {
	if (!error.HasError()) {
		error = std::move(error_p);
	}
}

void BitfiltersFastFilterAllocationScope::ThrowIfFailed() {
	if (error.HasError()) {
		error.Throw();
	}
}

void *BitfiltersFastFilterMalloc(size_t size) {
	return FastFilterAllocate(size, false);
}

void *BitfiltersFastFilterCalloc(size_t count, size_t size) {
	if (size != 0 && count > NumericLimits<size_t>::Maximum() / size) {
		return nullptr;
	}
	return FastFilterAllocate(count * size, true);
}

void BitfiltersFastFilterFree(void *pointer) {
	if (!pointer) {
		return;
	}
	auto base = reinterpret_cast<data_ptr_t>(pointer) - sizeof(FastFilterAllocationHeader);
	auto header = reinterpret_cast<FastFilterAllocationHeader *>(base);
	header->allocator->FreeData(base, header->size);
}

} // namespace duckdb
