// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/fixed_array.h>

#include <deque>

#include "base/io_buf.h"
#include "server/common_types.h"
#include "server/dfly_protocol.h"
#include "server/resp_expr.h"
#include "util/connection.h"
#include "util/fibers/synchronization.h"
#include "server/conn_context.h"

typedef struct ssl_ctx_st SSL_CTX;

namespace dfly {

class ConnectionContext;
class RedisParser;
class Service;
class MemcacheParser;

class Connection : public util::Connection {
 public:
  Connection(Protocol protocol, Service* service, SSL_CTX* ctx);
  ~Connection();

  using error_code = std::error_code;

  Protocol protocol() const {
    return protocol_;
  }

 protected:
  void OnShutdown() override;

 private:
  enum ParserStatus { OK, NEED_MORE, ERROR };

  void HandleRequests() final;

  void InputLoop(util::FiberSocketBase* peer);
  void DispatchFiber(util::FiberSocketBase* peer);

  ParserStatus ParseRedis(base::IoBuf* buf);
  ParserStatus ParseMemcache(base::IoBuf* buf);

  std::unique_ptr<RedisParser> redis_parser_;
  std::unique_ptr<MemcacheParser> memcache_parser_;
  Service* service_;
  SSL_CTX* ctx_;
  std::unique_ptr<ConnectionContext> cc_;

  static ConnectionContext::Request* FromArgs(RespVec args);



  // std::deque<Request*> dispatch_q_;  // coordinated via evc_.
  // util::fb2::EventCount evc_;
  unsigned parser_error_ = 0;
  Protocol protocol_;
};

}  // namespace dfly
