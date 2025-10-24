// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/engine_shard_set.h"

#include <absl/strings/str_cat.h>
#include "base/logging.h"

// #include "util/fiber_sched_algo.h"
#include "util/varz.h"

namespace dfly {

using namespace std;
using namespace util;

thread_local EngineShard* EngineShard::shard_ = nullptr;
constexpr size_t kQueueLen = 64;

EngineShard::EngineShard(ShardId index)
    : db_slice(index, this), queue_(kQueueLen) {
  fiber_q_ = fb2::Fiber(absl::StrCat("shard_queue", index), [this] {
    queue_.Run();
  });
}

EngineShard::~EngineShard() {
  queue_.Shutdown();
  fiber_q_.Join();
}

void EngineShard::InitThreadLocal(ShardId index) {
  CHECK(shard_ == nullptr) << index;
  shard_ = new EngineShard(index);
}

void EngineShard::DestroyThreadLocal() {
  if (!shard_)
    return;

  uint32_t index = shard_->db_slice.shard_id();
  delete shard_;
  shard_ = nullptr;

  VLOG(1) << "Shard reset " << index;
}

void EngineShardSet::Init(uint32_t sz) {
  CHECK_EQ(0u, size());

  shard_queue_.resize(sz);
}

void EngineShardSet::InitThreadLocal(ShardId index) {
  EngineShard::InitThreadLocal(index);
  shard_queue_[index] = EngineShard::tlocal()->GetQueue();
}

}  // namespace dfly
