// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "util/listener_interface.h"
#include "server/dfly_protocol.h"

typedef struct ssl_ctx_st SSL_CTX;

namespace dfly {

class Service;

class Listener : public util::ListenerInterface {
 public:
  Listener(Protocol protocol, Service*);
  ~Listener();

 private:
  util::Connection* NewConnection(util::fb2::ProactorBase* proactor) final;
  util::fb2::ProactorBase* PickConnectionProactor(util::FiberSocketBase* sock) final;

  void PreShutdown();

  void PostShutdown();

  Service* engine_;

  std::atomic_uint32_t next_id_{0};
  Protocol protocol_;
  SSL_CTX* ctx_ = nullptr;
};

}  // namespace dfly
