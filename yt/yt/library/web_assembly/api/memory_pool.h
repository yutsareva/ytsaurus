#pragma once

#include "compartment.h"

#include <vector>

#include <util/generic/noncopyable.h>

namespace NYT::NWebAssembly {

////////////////////////////////////////////////////////////////////////////////

// We intentionally define a separate class to work with memory pools,
// because this class handles the difference between
// memory addresses inside the VM and inside the host process.

class TWebAssemblyMemoryPool
    : public TNonCopyable
{
public:
    TWebAssemblyMemoryPool();
    explicit TWebAssemblyMemoryPool(IWebAssemblyCompartment* compartment);

    ~TWebAssemblyMemoryPool();
    TWebAssemblyMemoryPool(TWebAssemblyMemoryPool&& other);
    TWebAssemblyMemoryPool& operator=(TWebAssemblyMemoryPool&& other);

    char* AllocateUnaligned(size_t size);
    char* AllocateAligned(size_t size, int align = 8);

    void Clear();

    size_t GetSize() const;
    size_t GetCapacity() const;

private:
    IWebAssemblyCompartment* const Compartment_ = nullptr;
    size_t Size_ = 0;
    std::vector<uintptr_t> Allocations_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NWebAssembly
