// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/dragonfly_connection.h"

#include <absl/container/flat_hash_map.h>

#include <boost/fiber/operations.hpp>

#include "base/io_buf.h"
#include "base/logging.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/main_service.h"
#include "server/memcache_parser.h"
#include "server/redis_parser.h"
#include "util/fibers/fibers.h"
#include "util/tls/tls_socket.h"

using namespace util;
using namespace std;

namespace dfly {
namespace {

void SendProtocolError(RedisParser::Result pres, FiberSocketBase* peer) {
  string res("-ERR Protocol error: ");
  if (pres == RedisParser::BAD_BULKLEN) {
    res.append("invalid bulk length\r\n");
  } else {
    CHECK_EQ(RedisParser::BAD_ARRAYLEN, pres);
    res.append("invalid multibulk length\r\n");
  }

  error_code size_res = peer->Write(::io::Buffer(res));
  if (!size_res) {
    LOG(WARNING) << "Error " << size_res;
  }
}

void RespToArgList(const RespVec& src, CmdArgVec* dest) {
  dest->resize(src.size());
  for (size_t i = 0; i < src.size(); ++i) {
    (*dest)[i] = ToMSS(src[i].GetBuf());
  }
}

constexpr size_t kMinReadSize = 256;

}  // namespace

Connection::Connection(Protocol protocol, Service* service, SSL_CTX* ctx)
    : service_(service), ctx_(ctx) {
  protocol_ = protocol;

  switch (protocol) {
    case Protocol::REDIS:
      redis_parser_.reset(new RedisParser);
      break;
    case Protocol::MEMCACHE:
      memcache_parser_.reset(new MemcacheParser);
      break;
  }
}

Connection::~Connection() {
}

void Connection::OnShutdown() {
  VLOG(1) << "Connection::OnShutdown";
}

void Connection::HandleRequests() {
  util::ThisFiber::SetName("DflyConnection");

  LinuxSocketBase* lsb = static_cast<LinuxSocketBase*>(socket_.get());
  int val = 1;
  CHECK_EQ(0, setsockopt(socket_->native_handle(), SOL_TCP, TCP_NODELAY, &val, sizeof(val)));

  auto ep = lsb->RemoteEndpoint();

  std::unique_ptr<tls::TlsSocket> tls_sock;
  if (ctx_) {
    tls_sock.reset(new tls::TlsSocket(socket_.get()));
    tls_sock->InitSSL(ctx_);

    FiberSocketBase::AcceptResult aresult = tls_sock->Accept();
    if (!aresult) {
      LOG(WARNING) << "Error handshaking " << aresult.error().message();
      return;
    }
    VLOG(1) << "TLS handshake succeeded";
  }
  FiberSocketBase* peer = tls_sock ? (FiberSocketBase*)tls_sock.get() : socket_.get();
  cc_.reset(new ConnectionContext(peer, this));
  cc_->shard_set = &service_->shard_set();

  InputLoop(peer);

  while (cc_->conn_state.pending_requests.load(std::memory_order_relaxed) > 0) {
    ThisFiber::Yield();
  }

  VLOG(1) << "Closed connection for peer " << ep;
}

void Connection::InputLoop(FiberSocketBase* peer) {
  base::IoBuf io_buf{kMinReadSize};

  // auto dispatch_fb = fb2::Fiber(fb2::Launch::dispatch, [&] { DispatchFiber(peer); });
  ParserStatus status = OK;
  std::error_code ec;

  do {
    auto buf = io_buf.AppendBuffer();
    ::io::Result<size_t> recv_sz = peer->Recv(buf);

    if (!recv_sz) {
      ec = recv_sz.error();
      status = OK;
      break;
    }

    io_buf.CommitWrite(*recv_sz);

    if (redis_parser_)
      status = ParseRedis(&io_buf);
    else {
      DCHECK(memcache_parser_);
      status = ParseMemcache(&io_buf);
    }

    if (status == NEED_MORE) {
      status = OK;
    } else if (status != OK) {
      break;
    }
  } while (peer->IsOpen() && !cc_->ec());

  cc_->conn_state.mask |= ConnectionState::CONN_CLOSING;  // Signal dispatch to close.
  evc_.notify();
  // dispatch_fb.Join();

  if (cc_->ec()) {
    ec = cc_->ec();
  } else {
    if (status == ERROR) {
      VLOG(1) << "Error stats " << status;
      if (redis_parser_) {
        SendProtocolError(RedisParser::Result(parser_error_), peer);
      } else {
        string_view sv{"CLIENT_ERROR bad command line format\r\n"};
        std::error_code size_res = peer->Write(::io::Buffer(sv));
        if (!size_res) {
          LOG(WARNING) << "Error " << size_res;
          ec = size_res;
        }
      }
    }
  }

  if (ec && !FiberSocketBase::IsConnClosed(ec)) {
    LOG(WARNING) << "Socket error " << ec;
  }
}

auto Connection::ParseRedis(base::IoBuf* io_buf) -> ParserStatus {
  RespVec args;
  CmdArgVec arg_vec;
  uint32_t consumed = 0;

  RedisParser::Result result = RedisParser::OK;

  do {
    result = redis_parser_->Parse(io_buf->InputBuffer(), &consumed, &args);

    if (result == RedisParser::OK && !args.empty()) {
      RespExpr& first = args.front();
      if (first.type == RespExpr::STRING) {
        DVLOG(2) << "Got Args with first token " << ToSV(first.GetBuf());
      } else {
        result = RedisParser::BAD_STRING;
        break;
      }

      // TODO: Implement pipelining properly.
      // 1. Allocate commands arguments so that they won't rely on io_buf (not a blocker) - see SET
      //    just allocating its args inside for now.
      // 2. Make commands fully asynchronous - done in SET for example.
      // 3. Add ability to send into sockets from other threads.
      //    I hacked this inside FiberSocketBase::AsyncWrite2 - midi-redis works
      //    for non-pipelined commands.
      // 4. Order the replies properly. The idea is to maintain an intrusive message queue but
      //    only if cc_->conn_state.pending_requests > 0. Otherwise, we can dispatch directly.
      //    The message queue comes into play if we submitted a command asynchronously
      //    and then another command was read and parsed. If pending_requests is still > 0,
      //    we need to enqueue the new command to the queue. For simplicity, for now
      //    we can always enqueue. pending_requests is incremented in the coordinator, and
      //    decremented after the reply is sent.
      // c. Once the coordinator creates an intrusive reply item, it is appended to the queue,
      //    and the associated command keeps pointer to the item.  When it needs to reply,
      //    it moves the reply structure from the shard thread to the item
      //    but not necessarily sends it. It's thread safe as nobody reads from that item yet.
      //
      // d. For simplicity, I assume only single shard, single hop commands for now (GET/SET).
      //    There are two options: the item is at the head of the queue - or not.
      //    If the item is at the head of the queue, the shard callback
      //    can send the reply directly. Moreover, it can send multiple subsequent replies
      //    from the queue, as long as they are finalized. So, say, message requests contain A, B;
      //    and B callback was finished earlier than A, then its item will contain the reply,
      //    but it won't be sent until A is also ready. When A is ready,
      //    it can look at all the items at head that are ready, and combine them into a single
      //    vectorized send. At any time only a single "owner" can send from the queue and it's
      //    not necessarily the connection fiber.
      // e. Multi-shard command could in theory also send partial replies (MGET) asynchronously,
      //    but we could just have the last shard sending everything for simplicity.
      // f. Multi-hop commands: they can be fully asynchronous, or just make the last hop
      //    asynchronous for simplicity. Yes, the pipeline will stall on multi-hop commands,
      //    but that's acceptable for the POC.
      // g. FiberSocketBase::AsyncWrite2 assumes that everything we write is sent immediately and
      //    check-fails on it. It should be fixed. Currently, epoll notification mechanism is
      //    local to the proactor, so we can use DispatchBrief to schedule socket async writes
      //    in the coordinator thread.
      // h. Valkey uses a single epoll object in the process that is polled by different threads
      //    and contains all fds. Not sure if it's a good idea to do the same here
      //    but one thing I noticed - we call epoll_wait more frequently and each call brings
      //    back only 1-2 events in helio, while in valkey each epoll_wait brings back
      //    more events. I do not have any evidence it's even a problem as we call epoll_wait
      //    during idle times, but it's something to think about.
      //
      ConnectionContext::Request* req = FromArgs(std::move(args));
      cc_->EnqueueRequest(req);
      service_->DispatchCommand(CmdArgList{req->args.data(), req->args.size()}, cc_.get());
#if 0
      bool is_sync_dispatch = !cc_->conn_state.IsRunViaDispatch();
      if (dispatch_q_.empty() && is_sync_dispatch && consumed >= io_buf->InputLen()) {
        RespToArgList(args, &arg_vec);
        service_->DispatchCommand(CmdArgList{arg_vec.data(), arg_vec.size()}, cc_.get());
      } else {
        // Dispatch via queue to speedup input reading,
        Request* req = FromArgs(std::move(args));
        dispatch_q_.emplace_back(req);
        if (dispatch_q_.size() == 1) {
          evc_.notify();
        } else if (dispatch_q_.size() > 10) {
          ThisFiber::Yield();
        }
      }
#endif
    }
    io_buf->ConsumeInput(consumed);
  } while (RedisParser::OK == result && !cc_->ec());

  parser_error_ = result;
  if (result == RedisParser::OK)
    return OK;

  if (result == RedisParser::INPUT_PENDING)
    return NEED_MORE;

  return ERROR;
}


auto Connection::ParseMemcache(base::IoBuf* io_buf) -> ParserStatus {
  MemcacheParser::Result result = MemcacheParser::OK;
  uint32_t consumed = 0;
  MemcacheParser::Command cmd;
  string_view value;
  do {
    string_view str = ToSV(io_buf->InputBuffer());
    result = memcache_parser_->Parse(str, &consumed, &cmd);

    if (result != MemcacheParser::OK) {
      io_buf->ConsumeInput(consumed);
      break;
    }

    size_t total_len = consumed;
    if (MemcacheParser::IsStoreCmd(cmd.type)) {
      total_len += cmd.bytes_len + 2;
      if (io_buf->InputLen() >= total_len) {
        value = str.substr(consumed, cmd.bytes_len);
        // TODO: dispatch.
      } else {
        return NEED_MORE;
      }
    }

    // An optimization to skip dispatch_q_ if no pipelining is identified.
    // We use ASYNC_DISPATCH as a lock to avoid out-of-order replies when the
    // dispatch fiber pulls the last record but is still processing the command and then this
    // fiber enters the condition below and executes out of order.
    bool is_sync_dispatch = (cc_->conn_state.mask & ConnectionState::ASYNC_DISPATCH) == 0;
    if (dispatch_q_.empty() && is_sync_dispatch && consumed >= io_buf->InputLen()) {
      service_->DispatchMC(cmd, value, cc_.get());
    }
    io_buf->ConsumeInput(consumed);
  } while (!cc_->ec());

  parser_error_ = result;

  if (result == MemcacheParser::OK)
    return OK;

  if (result == MemcacheParser::INPUT_PENDING)
    return NEED_MORE;

  return ERROR;
}

// DispatchFiber handles commands coming from the InputLoop.
// Thus, InputLoop can quickly read data from the input buffer, parse it and push
// into the dispatch queue and DispatchFiber will run those commands asynchronously with InputLoop.
// Note: in some cases, InputLoop may decide to dispatch directly and bypass the DispatchFiber.
void Connection::DispatchFiber(util::FiberSocketBase* peer) {
  ThisFiber::SetName("DispatchFiber");

  while (!cc_->ec()) {
    evc_.await([this] { return cc_->conn_state.IsClosing() || !dispatch_q_.empty(); });
    if (cc_->conn_state.IsClosing())
      break;

    std::unique_ptr<Request> req{dispatch_q_.front()};
    dispatch_q_.pop_front();

    cc_->SetBatchMode(!dispatch_q_.empty());
    cc_->conn_state.mask |= ConnectionState::ASYNC_DISPATCH;
    service_->DispatchCommand(CmdArgList{req->args.data(), req->args.size()}, cc_.get());
    cc_->conn_state.mask &= ~ConnectionState::ASYNC_DISPATCH;
  }

  cc_->conn_state.mask |= ConnectionState::CONN_CLOSING;
}

auto Connection::FromArgs(RespVec args) -> Request* {
  DCHECK(!args.empty());
  size_t backed_sz = 0;
  for (const auto& arg : args) {
    CHECK_EQ(RespExpr::STRING, arg.type);
    backed_sz += arg.GetBuf().size();
  }
  DCHECK(backed_sz);

  Request* req = new Request{args.size(), backed_sz};

  auto* next = req->storage.data();
  for (size_t i = 0; i < args.size(); ++i) {
    auto buf = args[i].GetBuf();
    size_t s = buf.size();
    memcpy(next, buf.data(), s);
    req->args[i] = MutableStrSpan(next, s);
    next += s;
  }

  return req;
}

}  // namespace dfly
