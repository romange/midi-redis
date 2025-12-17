// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/conn_context.h"

extern "C" {
#include "examples/redis_dict/sds.h"
}

#include "base/logging.h"
#include "server/dragonfly_connection.h"

using namespace std;
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

    if (to_execute == nullptr) {
      // we executed all the parsed commands so far.
      to_execute = cmd;
    }
  }
}

void ConnectionContext::SendSimpleRespString(string_view str, ParsedCommand* cmd) {
  DCHECK(cmd);

  // Enqueue a success response into the req.
  cmd->resp = ParsedCommand::SimpleString{string{str}};
  uint8_t prev = cmd->state.fetch_or(ParsedCommand::EXECUTE_DONE, std::memory_order_acq_rel);
  if (prev & ParsedCommand::HEAD_REPLY) {
    owner_->Notify();
  }
}

void ConnectionContext::SendGetReply(std::string_view key, uint32_t flags, std::string_view value,
                                     ParsedCommand* cmd) {
  DCHECK(cmd);

  // Enqueue a bulk response into the req.
  cmd->resp = ParsedCommand::BulkString{string{value}};
  uint8_t prev = cmd->state.fetch_or(ParsedCommand::EXECUTE_DONE, std::memory_order_acq_rel);
  if (prev & ParsedCommand::HEAD_REPLY) {
    owner_->Notify();
  }
}

void ConnectionContext::SendGetNotFound(ParsedCommand* cmd) {
  DCHECK(cmd);
  cmd->resp = ParsedCommand::Null{};
  uint8_t prev = cmd->state.fetch_or(ParsedCommand::EXECUTE_DONE, std::memory_order_acq_rel);
  if (prev & ParsedCommand::HEAD_REPLY) {
    VLOG(1) << "Notify " << cmd;
    owner_->Notify();
  }
}

void ConnectionContext::ReplyReadyCommands() {
  while (parsed_head != to_execute) {
    auto* cmd = parsed_head;
    // peek_only = false since we want to claim ownership of the cmd to send its reply immediately.
    if (!CheckIfCanReply(cmd)) {
      break;
    }
    VLOG(1) << "Replying command " << cmd;
    auto resp = std::move(cmd->resp);

    sdsfreesplitres(cmd->tokens, cmd->argc);

    auto* next = cmd->next;
    delete cmd;
    parsed_head = next;
    // Look-ahead check: peek_only=true. We just want to know if we should batch,
    // we do NOT want to claim ownership of the next reply yet.
    bool batch_mode = (next && next->dispatched && CheckIfCanReply(next, true));
    SetBatchMode(batch_mode);

    if (std::holds_alternative<ParsedCommand::ErrorString>(resp)) {
      reply_builder_.SendError(std::get<ParsedCommand::ErrorString>(resp));
    } else if (std::holds_alternative<ParsedCommand::SimpleString>(resp)) {
      reply_builder_.SendSimpleRespString(std::get<ParsedCommand::SimpleString>(resp));
    } else if (std::holds_alternative<ParsedCommand::BulkString>(resp)) {
      reply_builder_.SendBulk(std::get<ParsedCommand::BulkString>(resp));
    } else if (std::holds_alternative<ParsedCommand::Null>(resp)) {
      reply_builder_.SendGetNotFound();
    } else {
      // Skip monostate.
    }
  }
}

bool ConnectionContext::CheckIfCanReply(ParsedCommand* head, bool peek_only) {
  DCHECK(head);
  if (head->parse_complete == 0)
    return false;
  if (!head->execute_async)
    return true;

  uint8_t state = head->state.load(std::memory_order_relaxed);

  // If the command is already done, we can always reply (peek or not).
  if (state & ParsedCommand::EXECUTE_DONE) {
    return true;
  }

  // If we are only peeking and it's not done, return false, do not set HEAD_REPLY (avoiding the
  // side effect).
  if (peek_only) {
    return false;
  }

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
