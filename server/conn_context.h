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
  ParsedCommand() : flags(0) {
  }

  struct ErrorString : public std::string {};
  struct SimpleString : public std::string {};
  struct BulkString : public std::string {};
  using Null = std::nullptr_t;
  using Response = std::variant<std::monostate, ErrorString, SimpleString, BulkString, Null>;

  sds* tokens = nullptr;
  unsigned argc = 0;
  ParsedCommand* next = nullptr;

  union {
    struct {
      uint8_t parse_complete : 1;
      uint8_t dispatched : 1;
      uint8_t execute_async : 1;

      uint8_t reserved : 6;
    };
    uint8_t flags;
  };

  Response resp;

  enum StateBits : uint32_t {
    EXECUTE_DONE = 1 << 0,
    HEAD_REPLY = 1 << 1,
  };
  std::atomic_uint8_t state{0};
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

  void AddParsedCommand(sds* tokens, unsigned argc, bool fully_parsed);

  //
  // The structure is:
  // head -> ... -> to_send -> .. -> to_execute -> ... -> tail
  //
  ParsedCommand* parsed_head = nullptr;
  ParsedCommand* parsed_tail = nullptr;
  ParsedCommand* to_execute = nullptr;

  // std::vector<ParsedCommand> parsed_commands;
  // int current_cmd_idx = -1;

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

  void SendSimpleRespString(std::string_view str, ParsedCommand* cmd);

  void SendRespBlob(std::string_view str) {
    reply_builder_.SendRespBlob(str);
  }

  void SendGetReply(std::string_view key, uint32_t flags, std::string_view value,
                    ParsedCommand* cmd);

  void SendGetNotFound() {
    reply_builder_.SendGetNotFound();
  }

  void SendGetNotFound(ParsedCommand* cmd);

  void SendSimpleStrArr(const std::string_view* arr, uint32_t count) {
    reply_builder_.SendSimpleStrArr(arr, count);
  }

  void SetBatchMode(bool mode) {
    reply_builder_.SetBatchMode(mode);
  }

  std::error_code ec() const {
    return reply_builder_.ec();
  }

  void ReplyReadyCommands();

  bool CheckIfCanReply(ParsedCommand* head, bool peek_only = false);

 private:
  Connection* owner_;
  ReplyBuilder reply_builder_;
};

}  // namespace dfly
