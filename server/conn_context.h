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
  sds* tokens = nullptr;
  unsigned argc = 0;
  ParsedCommand* next = nullptr;

  std::atomic_uint32_t state{0};
};

class ConnectionContext {
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

  ParsedCommand* parsed_list = nullptr;
  ParsedCommand* current
  // std::vector<ParsedCommand> parsed_commands;
  int current_cmd_idx = -1;

  // Reply methods that delegate to reply_builder_
  void SendError(std::string_view str) {
    reply_builder_.SendError(str);
  }

  void SendError(OpStatus status) {
    reply_builder_.SendError(status);
  }

  void SendOk() {
    reply_builder_.SendOk();
  }

  void SendStored() {
    reply_builder_.SendStored();
  }

  void SendMCClientError(std::string_view str) {
    reply_builder_.SendMCClientError(str);
  }

  void EndMultilineReply() {
    reply_builder_.EndMultilineReply();
  }

  void SendSimpleRespString(std::string_view str) {
    reply_builder_.SendSimpleRespString(str);
  }

  void SendRespBlob(std::string_view str) {
    reply_builder_.SendRespBlob(str);
  }

  void SendGetReply(std::string_view key, uint32_t flags, std::string_view value) {
    reply_builder_.SendGetReply(key, flags, value);
  }

  void SendGetNotFound() {
    reply_builder_.SendGetNotFound();
  }

  void SendSimpleStrArr(const std::string_view* arr, uint32_t count) {
    reply_builder_.SendSimpleStrArr(arr, count);
  }

  void SetBatchMode(bool mode) {
    reply_builder_.SetBatchMode(mode);
  }

  std::error_code ec() const {
    return reply_builder_.ec();
  }

 private:
  Connection* owner_;
  ReplyBuilder reply_builder_;
};

}  // namespace dfly
