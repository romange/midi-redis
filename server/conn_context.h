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

  using Response = std::variant<ErrorString, SimpleString>;

  struct Request {
    absl::FixedArray<MutableStrSpan> args;
    absl::FixedArray<char> storage;
    std::atomic<Request*> next{nullptr};
    Response resp;
    std::atomic_bool send_ready{false};

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

  std::atomic<Request*> dispatch_q_head{nullptr}, dispatch_q_tail{nullptr};

  // Async, can be called from other threads.
  void ForeignSendStored();

  void EnqueueRequest(Request* req);

  // Thread-safe and keeping pipeline semantics.
  void SendError(Request* req, std::string_view str);

 private:

  void DeleteHeadChain();

  ReplyBuilder reply_builder_;
  Connection* owner_;
};

}  // namespace dfly
