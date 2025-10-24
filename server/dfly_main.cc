// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "base/init.h"
#include "server/main_service.h"
#include "server/dragonfly_listener.h"
#include "util/accept_server.h"
#include "util/fibers/pool.h"
#include "util/varz.h"

ABSL_FLAG(int32_t, http_port, 8080, "Http port.");
ABSL_DECLARE_FLAG(uint32_t, port);
ABSL_DECLARE_FLAG(uint32_t, memcache_port);

using namespace util;
using namespace std;
using absl::GetFlag;

namespace dfly {

void RunEngine(ProactorPool* pool, AcceptServer* acceptor, HttpListener<>* http) {
  Service service(pool);
  service.Init(acceptor);

  if (http) {
    service.RegisterHttp(http);
  }

  acceptor->AddListener(GetFlag(FLAGS_port), new Listener{Protocol::REDIS, &service});
  auto mc_port = GetFlag(FLAGS_memcache_port);
  if (mc_port > 0) {
    acceptor->AddListener(mc_port, new Listener{Protocol::MEMCACHE, &service});
  }

  acceptor->Run();
  acceptor->Wait();

  service.Shutdown();
}

}  // namespace dfly


int main(int argc, char* argv[]) {
  MainInitGuard guard(&argc, &argv);

  CHECK_GT(GetFlag(FLAGS_port), 0u);

  unique_ptr<ProactorPool> pp;
  pp.reset(fb2::Pool::IOUring(256));

  pp->Run();

  AcceptServer acceptor(pp.get());
  HttpListener<>* http_listener = nullptr;

  if (GetFlag(FLAGS_http_port) >= 0) {
    http_listener = new HttpListener<>;
    http_listener->enable_metrics();

    // Ownership over http_listener is moved to the acceptor.
    uint16_t port = acceptor.AddListener(GetFlag(FLAGS_http_port), http_listener);

    LOG(INFO) << "Started http service on port " << port;
  }

  dfly::RunEngine(pp.get(), &acceptor, http_listener);

  pp->Stop();

  return 0;
}
