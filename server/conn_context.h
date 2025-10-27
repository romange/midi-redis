// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

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

  using Response = std::variant<std::monostate, ErrorString, SimpleString>;

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

  // Async, can be called from other threads.
  void ForeignSendStored();

  void EnqueueRequest(Request* req) {
    request_queue_.Enqueue(req);
  }

  // Thread-safe and keeping pipeline semantics.
  void SendError(Request* req, std::string_view str);

  // Stubs
  void SendMCClientError(std::string_view str) {
  }
  void EndMultilineReply() {
  }

  void SendSimpleRespString(std::string_view str) {
  }

  void SendRespBlob(std::string_view str) {
  }

  void SendGetReply(std::string_view key, uint32_t flags, std::string_view value) {
  }
  void SendGetNotFound() {
  }
  void SendError(std::string_view str) {
  }
  void SendSimpleStrArr(const std::string_view* arr, uint32_t count) {
  }
  void SendOk() {
  }

  std::error_code ec() const {
    return reply_builder_.ec();
  }

 private:
  class RequestQueue {
   public:
    void Enqueue(LockFreeRecord* req);

    void TryDrain(ReplyBuilder* builder);

   private:
    std::atomic<LockFreeRecord*> dispatch_q_head_{nullptr};
    char buf[64];
    std::atomic<LockFreeRecord*> dispatch_q_tail_{nullptr};
    std::atomic_uint32_t pending_drain_calls_{0};
  };

  ReplyBuilder reply_builder_;
  Connection* owner_;
  RequestQueue request_queue_;
};

}  // namespace dfly
