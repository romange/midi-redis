// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <mimalloc-new-delete.h>

#include "base/init.h"
#include "base/proc_util.h"  // for GetKernelVersion
extern "C" {
 #include "examples/redis_dict/alloc.h"
}

#include "server/dragonfly_listener.h"
#include "server/main_service.h"
#include "util/accept_server.h"
#include "util/fibers/pool.h"

#ifdef __linux__
#include "util/fibers/uring_proactor.h"
#endif

ABSL_FLAG(int32_t, http_port, 8080, "Http port.");
ABSL_FLAG(bool, force_epoll, false, "Force using epoll proactor on linux.");
ABSL_DECLARE_FLAG(uint32_t, port);
ABSL_DECLARE_FLAG(uint32_t, memcache_port);

ABSL_FLAG(uint16_t, uring_recv_buffer_cnt, 0,
          "How many socket recv buffers of size 256 to allocate per thread."
          "Relevant only for modern kernels with io_uring enabled");

using namespace util;
using namespace std;
using absl::GetFlag;

namespace dfly {

// version 5.11 maps to 511 etc.
// set upon server start.
unsigned kernel_version = 0;

void RegisterBufRings(ProactorPool* pool) {
#ifdef __linux__
  auto bufcnt = absl::GetFlag(FLAGS_uring_recv_buffer_cnt);
  if (bufcnt == 0) {
    return;
  }

  if (dfly::kernel_version < 602 || pool->at(0)->GetKind() != ProactorBase::IOURING) {
    LOG(WARNING) << "uring_recv_buffer_cnt is only supported on kernels >= 6.2 and with "
                    "io_uring proactor";
    return;
  }

  // We need a power of 2 length.
  bufcnt = absl::bit_ceil(bufcnt);
  pool->AwaitBrief([&](unsigned, ProactorBase* pb) {
    auto up = static_cast<fb2::UringProactor*>(pb);
    int res = up->RegisterBufferRing(kRecvSockGid, bufcnt, kRecvBufSize);
    if (res != 0) {
      LOG(ERROR) << "Failed to register buf ring for proactor "
                 << util::detail::SafeErrorMessage(res);
      exit(1);
    }
  });
  LOG(INFO) << "Registered a bufring with " << bufcnt << " buffers of size " << kRecvBufSize
            << " per thread ";
#endif
}

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
  hiredisAllocFuncs funcs;
  funcs.mallocFn = mi_malloc;
  funcs.callocFn = mi_calloc;
  funcs.reallocFn = mi_realloc;
  funcs.freeFn = mi_free;
  funcs.strdupFn = [](const char* str) { return static_cast<char*>(mi_strdup(str)); };
  hiredisSetAllocators(&funcs);

  unique_ptr<ProactorPool> pp;

#ifdef __linux__
  base::sys::KernelVersion kver;
  base::sys::GetKernelVersion(&kver);

  CHECK_LT(kver.major, 99u);
  dfly::kernel_version = kver.kernel * 100 + kver.major;

  bool use_epoll = GetFlag(FLAGS_force_epoll);

  if (use_epoll) {
    pp.reset(fb2::Pool::Epoll(0));
  } else {
    pp.reset(fb2::Pool::IOUring(1024, 0));  // 1024 - iouring queue size.
  }
#else
  pp.reset(fb2::Pool::Epoll(max_available_threads));
#endif

  pp->Run();

  dfly::RegisterBufRings(pp.get());

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
