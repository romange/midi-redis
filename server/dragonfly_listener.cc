// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/dragonfly_listener.h"

#include <openssl/ssl.h>

#include "base/flags.h"
#include "base/logging.h"
#include "server/dragonfly_connection.h"
#include "util/proactor_pool.h"

using namespace std;

ABSL_FLAG(uint32_t, conn_threads, 0, "Number of threads used for handing server connections");
ABSL_FLAG(bool, tls, false, "");
ABSL_FLAG(bool, conn_use_incoming_cpu, false,
            "If true uses incoming cpu of a socket in order to distribute"
            " incoming connections");

ABSL_FLAG(string, tls_client_cert_file, "", "");
ABSL_FLAG(string, tls_client_key_file, "", "");

using absl::GetFlag;

enum TlsClientAuth {
  CL_AUTH_NO = 0,
  CL_AUTH_YES = 1,
  CL_AUTH_OPTIONAL = 2,
};


namespace dfly {

using namespace util;

// To connect: openssl s_client  -cipher "ADH:@SECLEVEL=0" -state -crlf  -connect 127.0.0.1:6380
static SSL_CTX* CreateSslCntx() {
  SSL_CTX* ctx = SSL_CTX_new(TLS_server_method());

  if (GetFlag(FLAGS_tls_client_key_file).empty()) {
    // To connect - use openssl s_client -cipher with either:
    // "AECDH:@SECLEVEL=0" or "ADH:@SECLEVEL=0" setting.
    CHECK_EQ(1, SSL_CTX_set_cipher_list(ctx, "aNULL"));

    // To allow anonymous ciphers.
    SSL_CTX_set_security_level(ctx, 0);

    // you can still connect with redis-cli with :
    // redis-cli --tls --insecure --tls-ciphers "ADH:@SECLEVEL=0"
    LOG(WARNING)
        << "tls-client-key-file not set, no keys are loaded and anonymous ciphers are enabled. "
        << "Do not use in production!";
  } else {  // tls_client_key_file is set.
    auto key_file = GetFlag(FLAGS_tls_client_key_file);
    CHECK_EQ(1,
             SSL_CTX_use_PrivateKey_file(ctx, key_file.c_str(), SSL_FILETYPE_PEM));

    auto cert_file = GetFlag(FLAGS_tls_client_cert_file);

    if (!cert_file.empty()) {
      // TO connect with redis-cli you need both tls-client-key-file and tls-client-cert-file
      // loaded. Use `redis-cli --tls -p 6380 --insecure  PING` to test

      CHECK_EQ(1, SSL_CTX_use_certificate_chain_file(ctx, cert_file.c_str()));
    }
    CHECK_EQ(1, SSL_CTX_set_cipher_list(ctx, "DEFAULT"));
  }
  SSL_CTX_set_min_proto_version(ctx, TLS1_2_VERSION);

  SSL_CTX_set_options(ctx, SSL_OP_DONT_INSERT_EMPTY_FRAGMENTS);

  unsigned mask = SSL_VERIFY_NONE;

  // if (tls_auth_clients_opt)
  // mask |= SSL_VERIFY_FAIL_IF_NO_PEER_CERT;
  SSL_CTX_set_verify(ctx, mask, NULL);

  CHECK_EQ(1, SSL_CTX_set_dh_auto(ctx, 1));

  return ctx;
}

Listener::Listener(Protocol protocol, Service* e) : engine_(e), protocol_(protocol) {
  if (GetFlag(FLAGS_tls)) {
    OPENSSL_init_ssl(OPENSSL_INIT_SSL_DEFAULT, NULL);
    ctx_ = CreateSslCntx();
  }
}

Listener::~Listener() {
  SSL_CTX_free(ctx_);
}

util::Connection* Listener::NewConnection(ProactorBase* proactor) {
  return new Connection{protocol_, engine_, ctx_};
}

void Listener::PreShutdown() {
}

void Listener::PostShutdown() {
}

// We can limit number of threads handling dragonfly connections.
ProactorBase* Listener::PickConnectionProactor(FiberSocketBase* sock) {
  util::ProactorPool* pp = pool();
  uint32_t total = GetFlag(FLAGS_conn_threads);
  uint32_t id = kuint32max;

  if (total == 0 || total > pp->size()) {
    total = pp->size();
  }

  if (GetFlag(FLAGS_conn_use_incoming_cpu)) {
    int fd = sock->native_handle();

    int cpu, napi_id;
    socklen_t len = sizeof(cpu);

    CHECK_EQ(0, getsockopt(fd, SOL_SOCKET, SO_INCOMING_CPU, &cpu, &len));
    CHECK_EQ(0, getsockopt(fd, SOL_SOCKET, SO_INCOMING_NAPI_ID, &napi_id, &len));
    VLOG(1) << "CPU/NAPI for connection " << fd << " is " << cpu << "/" << napi_id;

    vector<unsigned> ids = pool()->MapCpuToThreads(cpu);
    if (!ids.empty()) {
      id = ids.front();
    }
  }

  if (id == kuint32max) {
    id = next_id_.fetch_add(1, std::memory_order_relaxed);
  }

  return pp->at(id % total);
}

}  // namespace dfly
