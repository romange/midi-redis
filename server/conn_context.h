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

struct ParsedCommand {
  sds* tokens;
  unsigned argc;
};

class ConnectionContext : public ReplyBuilder {
 public:
  ConnectionContext(::io::Sink* stream, Connection* owner);

  // TODO: to introduce proper accessors.
  const CommandId* cid = nullptr;
  EngineShardSet* shard_set = nullptr;

  Connection* owner() {
    return owner_;
  }

  Protocol protocol() const;

  ConnectionState conn_state;

  std::vector<ParsedCommand> parsed_commands;
  int current_cmd_idx = -1;

 private:
  Connection* owner_;
};

}  // namespace dfly
