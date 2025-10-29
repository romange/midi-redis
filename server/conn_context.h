// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/functional/function_ref.h>

#include "server/common_types.h"
#include "server/reply_builder.h"

namespace dfly {

class Connection;
class EngineShardSet;
class CommandId;

class ConnectionContext {
 public:
  struct ErrorString : public std::string {};
  struct SimpleString : public std::string {};
  struct BulkString : public std::string {};
  using Null = std::nullptr_t;
  using Response = std::variant<std::monostate, ErrorString, SimpleString, BulkString, Null>;

  struct LockFreeRecord {
    std::atomic<LockFreeRecord*> next{nullptr};
    std::atomic_bool ready{false};
  };

  struct Request : public LockFreeRecord {
    absl::FixedArray<MutableStrSpan> args;
    absl::FixedArray<char> storage;

    Response resp;

    Request(size_t nargs, size_t capacity) : args(nargs), storage(capacity) {
    }
    Request(const Request&) = delete;
  };

  ConnectionContext(::util::FiberSocketBase* stream, Connection* owner);

  // TODO: to introduce proper accessors.
  const CommandId* cid = nullptr;
  EngineShardSet* shard_set = nullptr;

  Connection* owner() {
    return owner_;
  }

  Protocol protocol() const;

  ConnectionState conn_state;

  void EnqueueRequest(Request* req) {
    request_queue_.Enqueue(req);
  }

  // Thread-safe and keeping pipeline semantics.
  void SendError(std::string_view str, Request* req = nullptr);

  // Stubs
  void SendMCClientError(std::string_view str) {
  }
  void EndMultilineReply() {
  }

  void SendSimpleRespString(std::string_view str, Request* req = nullptr);

  void SendRespBlob(std::string_view str) {
  }

  void SendGetReply(std::string_view key, uint32_t flags, std::string_view value, Request* req);
  void SendGetNotFound(Request* re);

  void SendSimpleStrArr(const std::string_view* arr, uint32_t count) {
  }

  void SendOk(Request* req = nullptr);

  void SendStored(Request* req = nullptr);

  std::error_code ec() const {
    return reply_builder_.ec();
  }

  bool HasPendingRequests() const {
    return !request_queue_.IsEmpty();
  }

  Request* current_request = nullptr;

 private:
  class RequestQueue {
   public:
    // bool - whether more items are pending.
    using ProcessCb = absl::FunctionRef<void(LockFreeRecord*, bool)>;

    void Enqueue(LockFreeRecord* req);

    void TryDrain(ProcessCb cb);

    bool IsEmpty() const {
      return dispatch_q_head_.load(std::memory_order_acquire) == nullptr;
    }

   private:
    std::atomic<LockFreeRecord*> dispatch_q_head_{nullptr};
    char buf[64];
    std::atomic<LockFreeRecord*> dispatch_q_tail_{nullptr};
    std::atomic_uint32_t pending_drain_calls_{0};
  };

  void TryDrain();

  ReplyBuilder reply_builder_;
  Connection* owner_;
  RequestQueue request_queue_;
};

}  // namespace dfly
