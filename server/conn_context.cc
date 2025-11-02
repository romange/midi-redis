// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/conn_context.h"

extern "C" {
#include "examples/redis_dict/sds.h"
}

#include "server/dragonfly_connection.h"

namespace dfly {

ConnectionContext::ConnectionContext(::io::Sink* stream, Connection* owner)
    : owner_(owner), reply_builder_(owner->protocol(), stream) {
}

Protocol ConnectionContext::protocol() const {
  return owner_->protocol();
}

void ConnectionContext::AddParsedCommand(sds* tokens, unsigned argc, bool fully_parsed) {
  ParsedCommand* cmd = new ParsedCommand;
  cmd->tokens = tokens;
  cmd->argc = argc;
  cmd->next = nullptr;
  cmd->parse_complete = fully_parsed ? 1 : 0;

  if (parsed_head == nullptr) {
    parsed_head = cmd;
    parsed_tail = cmd;
    to_execute = cmd;
  } else {
    parsed_tail->next = cmd;
    parsed_tail = cmd;
  }
}

void ConnectionContext::ReplyReadyCommands() {
  while (parsed_head != to_execute) {
    auto* cmd = parsed_head;
    if (!CheckIfCanReply(cmd)) {
      break;
    }

    sdsfreesplitres(cmd->tokens, cmd->argc);
    auto* next = cmd->next;
    delete cmd;
    parsed_head = next;
  }
}

bool ConnectionContext::CheckIfCanReply(ParsedCommand* head) {
  if (!head->execute_async)
    return true;

  uint8_t state = head->state.load(std::memory_order_relaxed);

  while ((state & ParsedCommand::EXECUTE_DONE) == 0) {
    if (state & ParsedCommand::HEAD_REPLY) {
      return false;
    }
    if (head->state.compare_exchange_weak(state, state | ParsedCommand::HEAD_REPLY,
                                          std::memory_order_acq_rel)) {
      return false;
    }
  }
  return true;
}

}  // namespace dfly
