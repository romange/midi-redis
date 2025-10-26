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

class ConnectionContext : public ReplyBuilder {
 public:
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

 private:
  Connection* owner_;
};

}  // namespace dfly
