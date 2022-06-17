// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <memory_resource>

extern "C" {
#include "examples/redis_dict/sds.h"
}

namespace dfly {

// StringSet is a nice but over-optimized data-structure. Probably is not worth it in the first
// place but sometimes the OCD kicks in and one can not resist.
// The advantage of it over redis-dict is smaller meta-data waste.
// dictEntry is 24 bytes, i.e it uses at least 32N bytes where N is the expected length.
// dict requires to allocate dictEntry per each addition in addition to the supplied key.
// It also wastes space in case of a set because it stores a value pointer inside dictEntry.
// To summarize:
// 100% utilized dict uses N*24 + N*8 = 32N bytes not including the key space.
// for 75% utilization (1/0.75 buckets): N*1.33*8 + N*24 = 35N
//
// This class uses 8 bytes per bucket (similarly to dictEntry*) but it used it for both
// links and keys. For most cases, we remove the need for another redirection layer
// and just store the key, so no "dictEntry" allocations occur.
// For those cells that require chaining, the bucket is
// changed in run-time to represent a linked chain.
// Additional feature - in order to to reduce collisions, we insert items into
// neighbour cells but only if they are empty (not chains). This way we reduce the number of
// empty (unused) spaces at full utilization from 36% to ~21%.
// 100% utilized table requires: N*8 + 0.2N*16 = 11.2N bytes or ~20 bytes savings.
// 75% utilization: N*1.33*8 + 0.12N*16 = 13N or ~22 bytes savings per record.
// TODO: to separate hash/compare functions from table logic and make it generic
// with potential replacements of hset/zset data structures.
// static_assert(sizeof(dictEntry) == 24);

class StringSet {
  struct LinkKey;
  // we can assume that high 12 bits of user address space
  // can be used for tagging. At most 52 bits of address are reserved for
  // some configurations, and usually it's 48 bits.
  // https://www.kernel.org/doc/html/latest/arm64/memory.html
  static constexpr size_t kLinkBit = 1ULL << 52;
  static constexpr size_t kDisplaceBit = 1ULL << 53;
  static constexpr size_t kTagMask = 4095ULL << 51; // we reserve 12 high bits.

  struct SuperPtr {
    void* ptr = nullptr;  //

    explicit SuperPtr(void* p = nullptr) : ptr(p) {
    }

    bool IsSds() const {
      return (uintptr_t(ptr) & kLinkBit) == 0;
    }

    bool IsLink() const {
      return (uintptr_t(ptr) & kLinkBit) == kLinkBit;
    }

    bool IsEmpty() const {
      return ptr == nullptr;
    }

    void* get() const {
      return (void*)(uintptr_t(ptr) & ~kTagMask);
    }

    bool IsDisplaced() const {
      return (uintptr_t(ptr) & kDisplaceBit) == kDisplaceBit;
    }

    // returns usable size.
    size_t SetString(std::string_view str);

    void SetLink(LinkKey* lk) {
      ptr = (void*)(uintptr_t(lk) | kLinkBit);
    }

    bool Compare(std::string_view str) const;

    void SetDisplaced() {
      ptr = (void*)(uintptr_t(ptr) | kDisplaceBit);
    }

    void ClearDisplaced() {
      ptr = (void*)(uintptr_t(ptr) & ~kDisplaceBit);
    }

    void Reset() {
      ptr = nullptr;
    }

    sds GetSds() const {
      if (IsSds())
        return (sds)get();
      LinkKey* lk = (LinkKey*)get();
      return (sds)lk->get();
    }
  };

  struct LinkKey : public SuperPtr {
    SuperPtr next;  // could be LinkKey* or sds.
  };

  static_assert(sizeof(SuperPtr) == 8);

 public:
  class iterator;
  class const_iterator;
  // using ItemCb = std::function<void(const CompactObj& co)>;

  StringSet(const StringSet&) = delete;

  explicit StringSet(std::pmr::memory_resource* mr = std::pmr::get_default_resource());
  ~StringSet();

  StringSet& operator=(StringSet&) = delete;

  void Reserve(size_t sz);

  bool Add(std::string_view str);

  bool Remove(std::string_view str);

  void Erase(iterator it);

  size_t size() const {
    return size_;
  }

  bool empty() const {
    return size_ == 0;
  }

  size_t bucket_count() const {
    return entries_.size();
  }

  // those that are chained to the entries stored inline in the bucket array.
  size_t num_chain_entries() const {
    return num_chain_entries_;
  }

  size_t num_used_buckets() const {
    return num_used_buckets_;
  }

  bool Contains(std::string_view val) const;

  bool Erase(std::string_view val);

  iterator begin() {
    return iterator{this, 0};
  }

  iterator end() {
    return iterator{};
  }

  size_t obj_malloc_used() const {
    return obj_malloc_used_;
  }

  size_t set_malloc_used() const {
    return (num_chain_entries_ + entries_.capacity()) * sizeof(SuperPtr);
  }

  /// stable scanning api. has the same guarantees as redis scan command.
  /// we avoid doing bit-reverse by using a different function to derive a bucket id
  /// from hash values. By using msb part of hash we make it "stable" with respect to
  /// rehashes. For example, with table log size 4 (size 16), entries in bucket id
  /// 1110 come from hashes 1110XXXXX.... When a table grows to log size 5,
  /// these entries can move either to 11100 or 11101. So if we traversed with our cursor
  /// range [0000-1110], it's guaranteed that in grown table we do not need to cover again
  /// [00000-11100]. Similarly with shrinkage, if a table is shrinked to log size 3,
  /// keys from 1110 and 1111 will move to bucket 111. Again, it's guaranteed that we
  /// covered the range [000-111] (all keys in that case).
  /// Returns: next cursor or 0 if reached the end of scan.
  /// cursor = 0 - initiates a new scan.
  // uint32_t Scan(uint32_t cursor, const ItemCb& cb) const;

  unsigned BucketDepth(uint32_t bid) const;

  // void IterateOverBucket(uint32_t bid, const ItemCb& cb);

  class iterator {
    friend class StringSet;

   public:
    iterator() : owner_(nullptr), entry_(nullptr), bucket_id_(0) {
    }

    iterator& operator++();

    bool operator==(const iterator& o) const {
      return entry_ == o.entry_;
    }

    bool operator!=(const iterator& o) const {
      return !(*this == o);
    }

   private:
    iterator(StringSet* owner, uint32_t bid) : owner_(owner), bucket_id_(bid) {
      SeekNonEmpty();
    }

    void SeekNonEmpty();

    StringSet* owner_ = nullptr;
    SuperPtr* entry_ = nullptr;
    uint32_t bucket_id_ = 0;
  };

  class const_iterator {
    friend class StringSet;

   public:
    const_iterator() : owner_(nullptr), entry_(nullptr), bucket_id_(0) {
    }

    const_iterator& operator++();

    const_iterator& operator=(iterator& it) {
      owner_ = it.owner_;
      entry_ = it.entry_;
      bucket_id_ = it.bucket_id_;

      return *this;
    }

    bool operator==(const const_iterator& o) const {
      return entry_ == o.entry_;
    }

    bool operator!=(const const_iterator& o) const {
      return !(*this == o);
    }

   private:
    const_iterator(const StringSet* owner, uint32_t bid) : owner_(owner), bucket_id_(bid) {
      SeekNonEmpty();
    }

    void SeekNonEmpty();

    const StringSet* owner_ = nullptr;
    const SuperPtr* entry_ = nullptr;
    uint32_t bucket_id_ = 0;
  };

 private:
  friend class iterator;

  using LinkAllocator = std::pmr::polymorphic_allocator<LinkKey>;

  std::pmr::memory_resource* mr() {
    return entries_.get_allocator().resource();
  }

  uint32_t BucketId(uint64_t hash) const {
    return hash >> (64 - capacity_log_);
  }

  uint32_t BucketId(sds ptr) const;

  // Returns: 2 if no empty spaces found around the bucket. 0, -1, 1 - offset towards
  // an empty bucket.
  int FindEmptyAround(uint32_t bid) const;

  // returns 2 if no object was found in the vicinity.
  // Returns relative offset to bid: 0, -1, 1 if found.
  int FindAround(std::string_view str, uint32_t bid) const;

  void Grow();

  void Link(SuperPtr ptr, uint32_t bid);
  /*void MoveEntry(Entry* e, uint32_t bid);

  void ShiftLeftIfNeeded(Entry* root) {
    if (root->next) {
      root->value = std::move(root->next->value);
      Entry* tmp = root->next;
      root->next = root->next->next;
      Free(tmp);
    }
  }
  */
  void Free(LinkKey* lk) {
    mr()->deallocate(lk, sizeof(LinkKey), alignof(LinkKey));
    --num_chain_entries_;
  }

  LinkKey* NewLink(std::string_view str, SuperPtr ptr);

  // The rule is - entries can be moved to vicinity as long as they are stored
  // "flat", i.e. not into the linked list. The linked list
  std::pmr::vector<SuperPtr> entries_;
  size_t obj_malloc_used_ = 0;
  uint32_t size_ = 0;
  uint32_t num_chain_entries_ = 0;
  uint32_t num_used_buckets_ = 0;
  unsigned capacity_log_ = 0;
};

#if 0
inline StringSet::iterator& StringSet::iterator::operator++() {
  if (entry_->next) {
    entry_ = entry_->next;
  } else {
    ++bucket_id_;
    SeekNonEmpty();
  }

  return *this;
}

inline StringSet::const_iterator& StringSet::const_iterator::operator++() {
  if (entry_->next) {
    entry_ = entry_->next;
  } else {
    ++bucket_id_;
    SeekNonEmpty();
  }

  return *this;
}
#endif

}  // namespace dfly
