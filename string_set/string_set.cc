// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "string_set/string_set.h"

#include <absl/numeric/bits.h>
#include <absl/strings/escaping.h>
#include <absl/hash/hash.h>

#include "base/logging.h"

namespace dfly {
using namespace std;

constexpr size_t kMinSizeShift = 2;
constexpr size_t kMinSize = 1 << kMinSizeShift;
constexpr size_t kAllowDisplacements = true;

inline bool CanSetFlat(int offs) {
  if (kAllowDisplacements)
    return offs < 2;
  return offs == 0;
}

inline uint64_t HashCode(string_view str) {
  return absl::Hash<string_view>{}(str);
}

StringSet::StringSet(pmr::memory_resource* mr) : entries_(mr) {
}

StringSet::~StringSet() {
  for (auto& entry : entries_) {
    if (entry.IsLink()) {
      LinkKey* lk = (LinkKey*)entry.get();
      while (lk) {
        sdsfree((sds)lk->ptr);
        SuperPtr next = lk->next;
        Free(lk);
        if (next.IsSds()) {
          sdsfree((sds)next.get());
          lk = nullptr;
        } else {
          DCHECK(next.IsLink());
          lk = (LinkKey*)next.get();
        }
      }
    } else if (!entry.IsEmpty()) {
      sdsfree((sds)entry.get());
    }
  }
  DCHECK_EQ(0u, num_chain_entries_);
}

void StringSet::Reserve(size_t sz) {
  sz = std::min<size_t>(sz, kMinSize);

  sz = absl::bit_ceil(sz);
  capacity_log_ = absl::bit_width(sz);
  entries_.reserve(sz);
}

size_t StringSet::SuperPtr::SetString(std::string_view str) {
  sds sdsptr = sdsnewlen(str.data(), str.size());
  ptr = sdsptr;
  return sdsAllocSize(sdsptr);
}

bool StringSet::SuperPtr::Compare(std::string_view str) const {
  if (IsEmpty())
    return false;

  sds sp = GetSds();
  return str == string_view{sp, sdslen(sp)};
}

bool StringSet::Add(std::string_view str) {
  DVLOG(1) << "Add " << absl::CHexEscape(str);

  uint64_t hc = HashCode(str);

  if (entries_.empty()) {
    capacity_log_ = kMinSizeShift;
    entries_.resize(kMinSize);
    auto& e = entries_[BucketId(hc)];
    obj_malloc_used_ += e.SetString(str);
    ++size_;
    ++num_used_buckets_;

    return true;
  }

  uint32_t bucket_id = BucketId(hc);
  if (FindAround(str, bucket_id) < 2)
    return false;

  DCHECK_LT(bucket_id, entries_.size());
  ++size_;

  // Try insert into flat surface first. Also handle the grow case
  // if utilization is too high.
  for (unsigned j = 0; j < 2; ++j) {
    int offs = FindEmptyAround(bucket_id);
    if (CanSetFlat(offs)) {
      auto& entry = entries_[bucket_id + offs];
      obj_malloc_used_ += entry.SetString(str);
      if (offs != 0) {
        entry.SetDisplaced();
      }
      ++num_used_buckets_;
      return true;
    }

    if (size_ < entries_.size())
      break;

    Grow();
    bucket_id = BucketId(hc);
  }

  auto& dest = entries_[bucket_id];
  DCHECK(!dest.IsEmpty());
  if (dest.IsDisplaced()) {
    sds sptr = dest.GetSds();
    uint32_t nbid = BucketId(sptr);
    Link(SuperPtr{sptr}, nbid);

    if (dest.IsSds()) {
      obj_malloc_used_ += dest.SetString(str);
    } else {
      LinkKey* lk = (LinkKey*)dest.get();
      obj_malloc_used_ += lk->SetString(str);
      dest.ClearDisplaced();
    }
  } else {
    LinkKey* lk = NewLink(str, dest);
    dest.SetLink(lk);
  }
  DCHECK(!dest.IsDisplaced());
  return true;
}

unsigned StringSet::BucketDepth(uint32_t bid) const {
  SuperPtr ptr = entries_[bid];
  if (ptr.IsEmpty()) {
    return 0;
  }

  unsigned res = 1;
  while (ptr.IsLink()) {
    LinkKey* lk = (LinkKey*)ptr.get();
    ++res;
    ptr = lk->next;
    DCHECK(!ptr.IsEmpty());
  }

  return res;
}

auto StringSet::NewLink(std::string_view str, SuperPtr ptr) -> LinkKey* {
  LinkAllocator ea(mr());
  LinkKey* lk = ea.allocate(1);
  ea.construct(lk);
  obj_malloc_used_ += lk->SetString(str);
  lk->next = ptr;
  ++num_chain_entries_;

  return lk;
}

#if 0
void StringSet::IterateOverBucket(uint32_t bid, const ItemCb& cb) {
  const Entry& e = entries_[bid];
  if (e.IsEmpty()) {
    DCHECK(!e.next);
    return;
  }
  cb(e.value);

  const Entry* next = e.next;
  while (next) {
    cb(next->value);
    next = next->next;
  }
}
#endif

inline bool cmpsds(sds sp, string_view str) {
  if (sdslen(sp) != str.size())
    return false;
  return str.empty() || memcmp(sp, str.data(), str.size()) == 0;
}

int StringSet::FindAround(string_view str, uint32_t bid) const {
  SuperPtr ptr = entries_[bid];

  while (ptr.IsLink()) {
    LinkKey* lk = (LinkKey*)ptr.get();
    sds sp = (sds)lk->get();
    if (cmpsds(sp, str))
      return 0;
    ptr = lk->next;
    DCHECK(!ptr.IsEmpty());
  }

  if (!ptr.IsEmpty()) {
    DCHECK(ptr.IsSds());
    sds sp = (sds)ptr.get();
    if (cmpsds(sp, str))
      return 0;
  }

  if (bid && entries_[bid - 1].Compare(str)) {
    return -1;
  }

  if (bid + 1 < entries_.size() && entries_[bid + 1].Compare(str)) {
    return 1;
  }

  return 2;
}

void StringSet::Grow() {
  size_t prev_sz = entries_.size();
  entries_.resize(prev_sz * 2);
  ++capacity_log_;

  for (int i = prev_sz - 1; i >= 0; --i) {
    SuperPtr* current = &entries_[i];
    if (current->IsEmpty()) {
      continue;
    }

    SuperPtr* prev = nullptr;
    while (true) {
      SuperPtr next;
      LinkKey* lk = nullptr;
      sds sp;

      if (current->IsLink()) {
        lk = (LinkKey*)current->get();
        sp = (sds)lk->get();
        next = lk->next;
      } else {
        sp = (sds)current->get();
      }

      uint32_t bid = BucketId(sp);
      if (bid != uint32_t(i)) {
        int offs = FindEmptyAround(bid);
        if (CanSetFlat(offs)) {
          auto& dest = entries_[bid + offs];
          DCHECK(!dest.IsLink());

          dest.ptr = sp;
          if (offs != 0)
            dest.SetDisplaced();
          if (lk) {
            Free(lk);
          }
          ++num_used_buckets_;
        } else {
          Link(*current, bid);
        }
        *current = next;
      } else {
        current->ClearDisplaced();
        if (lk) {
          prev = current;
          current = &lk->next;
        }
      }
      if (next.IsEmpty())
        break;
    }

    if (prev) {
      DCHECK(prev->IsLink());
      LinkKey* lk = (LinkKey*)prev->get();
      if (lk->next.IsEmpty()) {
        bool is_displaced = prev->IsDisplaced();
        prev->ptr = lk->get();
        if (is_displaced) {
          prev->SetDisplaced();
        }
        Free(lk);
      }
    }

    if (entries_[i].IsEmpty()) {
      --num_used_buckets_;
    }
  }

#if 0
  unsigned cnt = 0;
  for (auto ptr : entries_) {
    cnt += (!ptr.IsEmpty());
  }
  DCHECK_EQ(num_used_buckets_, cnt);
#endif
}

void StringSet::Link(SuperPtr ptr, uint32_t bid) {
  SuperPtr& root = entries_[bid];
  DCHECK(!root.IsEmpty());

  bool is_root_displaced = root.IsDisplaced();

  if (is_root_displaced) {
    DCHECK_NE(bid, BucketId(root.GetSds()));
  }
  LinkKey* head;
  void* val;

  if (ptr.IsSds()) {
    if (is_root_displaced) {
      // in that case it's better to put ptr into root and move root data into its correct place.
      sds val;
      if (root.IsSds()) {
        val = (sds)root.get();
        root.ptr = ptr.get();
      } else {
        LinkKey* lk = (LinkKey*)root.get();
        val = (sds)lk->get();
        lk->ptr = ptr.get();
        root.ClearDisplaced();
      }
      uint32_t nbid = BucketId(val);
      DCHECK_NE(nbid, bid);

      Link(SuperPtr{val}, nbid);  // Potentially unbounded wave of updates.
      return;
    }

    LinkAllocator ea(mr());
    head = ea.allocate(1);
    ea.construct(head);
    val = ptr.get();
    ++num_chain_entries_;
  } else {
    head = (LinkKey*)ptr.get();
    val = head->get();
  }

  if (root.IsSds()) {
    head->ptr = root.get();
    head->next = SuperPtr{val};
    root.SetLink(head);
    if (is_root_displaced) {
      DCHECK_NE(bid, BucketId((sds)head->ptr));
      root.SetDisplaced();
    }
  } else {
    DCHECK(root.IsLink());
    LinkKey* chain = (LinkKey*)root.get();
    head->next = chain->next;
    head->ptr = val;
    chain->next.SetLink(head);
  }
}

#if 0
void StringSet::MoveEntry(Entry* e, uint32_t bid) {
  auto& dest = entries_[bid];
  if (IsEmpty(dest)) {
    dest.value = std::move(e->value);
    Free(e);
    return;
  }
  e->next = dest.next;
  dest.next = e;
}
#endif

int StringSet::FindEmptyAround(uint32_t bid) const {
  if (entries_[bid].IsEmpty())
    return 0;

  if (bid + 1 < entries_.size() && entries_[bid + 1].IsEmpty())
    return 1;

  if (bid && entries_[bid - 1].IsEmpty())
    return -1;

  return 2;
}

uint32_t StringSet::BucketId(sds ptr) const {
  string_view sv{ptr, sdslen(ptr)};
  return BucketId(HashCode(sv));
}

#if 0
uint32_t StringSet::Scan(uint32_t cursor, const ItemCb& cb) const {
  if (capacity_log_ == 0)
    return 0;

  uint32_t bucket_id = cursor >> (32 - capacity_log_);
  const_iterator it(this, bucket_id);

  if (it.entry_ == nullptr)
    return 0;

  bucket_id = it.bucket_id_;  // non-empty bucket
  do {
    cb(*it);
    ++it;
  } while (it.bucket_id_ == bucket_id);

  if (it.entry_ == nullptr)
    return 0;

  if (it.bucket_id_ == bucket_id + 1) {  // cover displacement case
    // TODO: we could avoid checking computing HC if we explicitly mark displacement.
    // we have plenty-metadata to do so.
    uint32_t bid = BucketId((*it).HashCode());
    if (bid == it.bucket_id_) {
      cb(*it);
      ++it;
    }
  }

  return it.entry_ ? it.bucket_id_ << (32 - capacity_log_) : 0;
}

bool StringSet::Erase(std::string_view val) {
  uint64_t hc = CompactObj::HashCode(val);
  uint32_t bid = BucketId(hc);

  Entry* current = &entries_[bid];

  if (!current->IsEmpty()) {
    if (current->value == val) {
      current->Reset();
      ShiftLeftIfNeeded(current);
      --size_;
      return true;
    }

    Entry* prev = current;
    current = current->next;
    while (current) {
      if (current->value == val) {
        current->Reset();
        prev->next = current->next;
        Free(current);
        --size_;
        return true;
      }
      prev = current;
      current = current->next;
    }
  }

  auto& prev = entries_[bid - 1];
  // TODO: to mark displacement.
  if (bid && !prev.IsEmpty()) {
    if (prev.value == val) {
      obj_malloc_used_ -= prev.value.MallocUsed();

      prev.Reset();
      ShiftLeftIfNeeded(&prev);
      --size_;
      return true;
    }
  }

  auto& next = entries_[bid + 1];
  if (bid + 1 < entries_.size()) {
    if (next.value == val) {
      obj_malloc_used_ -= next.value.MallocUsed();
      next.Reset();
      ShiftLeftIfNeeded(&next);
      --size_;
      return true;
    }
  }

  return false;
}

#endif

void StringSet::iterator::SeekNonEmpty() {
  while (bucket_id_ < owner_->entries_.size()) {
    if (!owner_->entries_[bucket_id_].IsEmpty()) {
      entry_ = &owner_->entries_[bucket_id_];
      return;
    }
    ++bucket_id_;
  }
  entry_ = nullptr;
}

void StringSet::const_iterator::SeekNonEmpty() {
  while (bucket_id_ < owner_->entries_.size()) {
    if (!owner_->entries_[bucket_id_].IsEmpty()) {
      entry_ = &owner_->entries_[bucket_id_];
      return;
    }
    ++bucket_id_;
  }
  entry_ = nullptr;
}

}  // namespace dfly
