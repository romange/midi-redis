// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/dragonfly_connection.h"

extern "C" {
#include "examples/redis_dict/sds.h"
}

#include <absl/container/flat_hash_map.h>
#include <absl/strings/numbers.h>

#include <boost/fiber/operations.hpp>

#include "base/flags.h"
#include "base/io_buf.h"
#include "base/logging.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/main_service.h"
#include "server/memcache_parser.h"
#include "server/redis_parser.h"
#include "util/fibers/fibers.h"
#include "util/tls/tls_socket.h"

#ifdef __linux__
#include "util/fibers/uring_socket.h"
#endif

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

#if 0
void RespToArgList(const RespVec& src, CmdArgVec* dest) {
  dest->resize(src.size());
  for (size_t i = 0; i < src.size(); ++i) {
    (*dest)[i] = ToMSS(src[i].GetBuf());
  }
}
#endif
constexpr size_t kMinReadSize = 256;
// 64KB safety limit for the input buffer to prevent memory exhaustion
// while matching standard TCP window sizes.
constexpr size_t kMaxReadBufferSize = 65536;

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

  if (ctx_) {
    std::unique_ptr<tls::TlsSocket> tls_sock = std::make_unique<tls::TlsSocket>(std::move(socket_));

    tls_sock->InitSSL(ctx_);

    FiberSocketBase::AcceptResult aresult = tls_sock->Accept();
    if (!aresult) {
      LOG(WARNING) << "Error handshaking " << aresult.error().message();
      tls_sock->Close();
      return;
    }
    VLOG(1) << "TLS handshake succeeded";
    socket_ = std::move(tls_sock);
  }
  FiberSocketBase* peer{socket_.get()};
  cc_.reset(new ConnectionContext(peer, this));
  cc_->shard_set = &service_->shard_set();

  InputLoop(peer);

  VLOG(1) << "Closed connection for peer " << ep;
}

bool Connection::DoRead(util::FiberSocketBase* peer,
                        const util::FiberSocketBase::RecvNotification& rn, base::IoBuf* io_buf) {
  bool read_more{false};
  DVLOG(1) << "DoRead called for fd " << peer->native_handle();
  if (std::holds_alternative<util::FiberSocketBase::RecvNotification::RecvCompletion>(
          rn.read_result)) {
    auto buf = io_buf->AppendBuffer();
    if (buf.empty()) {
      return true;
    }
    auto res = peer->TryRecv(buf);
    if (res && (*res > 0)) {
      size_t bytes_received = *res;
      DVLOG(1) << "Read " << bytes_received << " bytes from fd " << peer->native_handle();
      io_buf->CommitWrite(bytes_received);
      read_more = (bytes_received == buf.size());
    } else if (res && (*res == 0)) {
      // EOF / Connection Closed cleanly
      ec_ = make_error_code(std::errc::connection_reset);
    } else {
      std::error_code ec = res.error();
      if (ec == std::errc::resource_unavailable_try_again ||
          ec == std::errc::operation_would_block) {
        // The socket is temporarily busy (e.g. TLS lock or Empty). We act as if there is "more"
        // work to do so the loop doesn't abort or treat this as a fatal error.
        read_more = true;
      } else {
        // Genuine Fatal Error
        ec_ = ec;
      }
    }

    // epoll notification.
    evc_.notify();
    return read_more;
  }

  if (std::holds_alternative<io::MutableBytes>(rn.read_result)) {
    auto& buf = std::get<io::MutableBytes>(rn.read_result);
    // TODO: parse directly from buf without copy.
    // Can be done if we ensure that parsing fully consumes the input buffer.
    io_buf->WriteAndCommit(buf.data(), buf.size());
  } else if (auto err = std::get_if<std::error_code>(&rn.read_result)) {
    if (*err == std::errc::resource_unavailable_try_again ||
        *err == std::errc::operation_would_block) {
      read_more = true;
    } else {
      ec_ = *err;
    }
  }

  evc_.notify();
  return read_more;
}

void Connection::InputLoop(FiberSocketBase* peer) {
  base::IoBuf io_buf{kMinReadSize};
  bool sock_might_have_data = true;

#ifdef __linux__
  if (socket_->proactor()->GetKind() == ProactorBase::IOURING) {
    // breaks with tls.
    auto* up = static_cast<fb2::UringProactor*>(socket_->proactor());
    bool enable_multishot = up->BufRingEntrySize(kRecvSockGid) > 0;
    if (enable_multishot) {
      fb2::UringSocket* usock = static_cast<fb2::UringSocket*>(peer);
      usock->set_bufring_id(kRecvSockGid);
      usock->EnableRecvMultishot();
    }
  }
#endif
  peer->RegisterOnRecv([&, peer](const FiberSocketBase::RecvNotification& rn) {
    sock_might_have_data = DoRead(peer, rn, &io_buf);
  });

  // auto dispatch_fb = fb2::Fiber(fb2::Launch::dispatch, [&] { DispatchFiber(peer); });
  ParserStatus status = OK;
  unsigned min_parse_threshold = 0;
  do {
    // If we have very little space left to append, grow the buffer. This handles the case where we
    // have partial data but need to read more to satisfy the parser.
    if ((io_buf.AppendLen() < kMinReadSize) && (io_buf.Capacity() < kMaxReadBufferSize)) {
      io_buf.EnsureCapacity(io_buf.Capacity() * 2);
    }

    if (sock_might_have_data && io_buf.InputLen() <= min_parse_threshold) {
      sock_might_have_data = DoRead(peer, FiberSocketBase::RecvNotification{}, &io_buf);
    }

    VLOG(1) << "before await, append len=" << io_buf.AppendLen()
            << " input len=" << io_buf.InputLen() << " " << cc_->parsed_head;
    evc_.await([&] {
      return io_buf.InputLen() > min_parse_threshold || ec_ ||
             (cc_->parsed_head && cc_->CheckIfCanReply(cc_->parsed_head));
    });

    if (ec_) {
      status = OK;
      break;
    }

    // io_buf.CommitWrite(*recv_sz);
    if (io_buf.InputLen() > 0) {
      VLOG(1) << "after await, input len=" << io_buf.InputLen();
      if (redis_parser_) {
        status = ParseRedis(&io_buf);
        // TODO: ParseRedis should fully deplete io_buf to simplify the I/O logic here,
        // to suppose io_uring provided buffers, and to allow a thread-local buffer shared among
        // multiple connections during the read/parse phase.
        // Also the way `evc_.await` is currently used assumes that after parsing
        // we have no data left in io_buf, and it enters
        // a busy loop otherwise:` echo -n "set foo" | nc localhost 6379` to reproduce.
        DCHECK(io_buf.InputLen() == 0 || status != OK);
      } else {
        DCHECK(memcache_parser_);
        status = ParseMemcache(&io_buf);
      }

      if (status == ERROR) {
        break;
      }
      VLOG(1) << "after parse, io_buf.InputLen=" << io_buf.InputLen()
              << " append len=" << io_buf.AppendLen();
    }

    // important: we record the watermark now before we preempt and possibly
    // increase io_buf further.
    min_parse_threshold = io_buf.InputLen();

    while (cc_->to_execute) {
      auto* cmd = cc_->to_execute;
      if (cmd->parse_complete == 0) {
        break;
      }
      // Enable batch mode if the next command is ready, so replies are buffered for pipelining.
      bool batch_mode =
          cmd->next && cmd->next->parse_complete && cc_->CheckIfCanReply(cmd->next, true);
      cc_->SetBatchMode(batch_mode);
      service_->DispatchCommand(CmdArgList{}, cc_.get());
      cc_->to_execute = cmd->next;
    }
    cc_->ReplyReadyCommands();
  } while (peer->IsOpen() && !cc_->ec());

  cc_->conn_state.mask |= ConnectionState::CONN_CLOSING;  // Signal dispatch to close.
  evc_.notify();
  // dispatch_fb.Join();

  if (cc_->ec()) {
    ec_ = cc_->ec();
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
          ec_ = size_res;
        }
      }
    }
  }
  peer->ResetOnRecvHook();

  if (ec_ && !FiberSocketBase::IsConnClosed(ec_)) {
    LOG(WARNING) << "Socket error " << ec_;
  }
}

#define PROTO_INLINE_MAX_SIZE (1024 * 64) /* Max size of inline reads */

auto Connection::ParseRedis(base::IoBuf* io_buf) -> ParserStatus {
  // TODO: the invariant should not be that we fully deplete io_buf
  // the real use-case is more complicated, as we may want to pause when too many commands
  // being in flight. For now we just loop.
  while (io_buf->InputLen() > 0) {
    auto input = io::View(io_buf->InputBuffer());
    if (state_ == INIT) {
      if (input[0] == '*') {
        state_ = PARSE_MULTIBULK;
      } else {
        state_ = PARSE_INLINE;
      }
    }

    if (state_ == PARSE_INLINE) {
      int linefeed_chars = 1;

      // int is_primary = c->read_flags & READ_FLAGS_PRIMARY;

      /* Search for end of line */
      size_t pos = input.find('\n', 0);

      /* Nothing to do without a \r\n */
      if (pos == string_view::npos) {
        if (input.size() > PROTO_INLINE_MAX_SIZE) {
          return ERROR;
        }
        parse_stash_.append(input.data(), input.size());
        io_buf->ConsumeInput(input.size());
        return NEED_MORE;
      }

      /* Handle the \r\n case. */
      if (pos && input[pos - 1] == '\r')
        pos--, linefeed_chars++;

      /* Split the input buffer up to the \r\n */
      size_t querylen = pos;
      parse_stash_.append(input.data(), querylen);
      parse_stash_.push_back('\0');  // null terminate for sdssplitargs
      int argc = 0;
      sds* argv = sdssplitargs(parse_stash_.c_str(), &argc);
      if (argv == NULL) {
        return ERROR;
      }
      CHECK(argc > 0);  // tbd.
      io_buf->ConsumeInput(querylen + linefeed_chars);
      parse_stash_.clear();
      /* Setup argv array on client structure */
      if (argc) {
        cc_->AddParsedCommand(argv, argc, true);
      }
    } else {
      auto result = ParseMultiBulk(io_buf);
      if (result != OK) {
        return result;
      }
    }
    CHECK(multibulk_len_ == 0);
    CHECK(bulk_len_ == -1);
    state_ = INIT;
  }
  return OK;

#if 0
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
      }

      // An optimization to skip dispatch_q_ if no pipelining is identified.
      // We use ASYNC_DISPATCH as a lock to avoid out-of-order replies when the
      // dispatch fiber pulls the last record but is still processing the command and then this
      // fiber enters the condition below and executes out of order.
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
    }
    io_buf->ConsumeInput(consumed);
  } while (RedisParser::OK == result && !cc_->ec());

  parser_error_ = result;
  if (result == RedisParser::OK)
    return OK;

  if (result == RedisParser::INPUT_PENDING)
    return NEED_MORE;

  return ERROR;
#endif
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

auto Connection::ParseMultiBulk(base::IoBuf* io_buf) -> ParserStatus {
  string_view input = ToSV(io_buf->InputBuffer());
  DCHECK(!input.empty());
  if (multibulk_len_ == 0) {
    DCHECK(input[0] == '*' || !parse_stash_.empty());

    /* Multi bulk length cannot be read without a \r\n */
    size_t pos = input.find('\r', 0);
    if (pos == string_view::npos || pos + 1 == input.size()) {
      return input.size() > PROTO_INLINE_MAX_SIZE ? ERROR : NEED_MORE;
    }

    size_t multibulk_len_slen = pos - 1;  // due to '*'
    long long ll = 0;

    if (!absl::SimpleAtoi(input.substr(1, multibulk_len_slen), &ll))
      return ERROR;

    if (ll > INT_MAX || ll <= 0) {
      return ERROR;
    }

    io_buf->ConsumeInput(pos + 2);
    input = ToSV(io_buf->InputBuffer());

    multibulk_len_ = ll;

    // TODO: we leak memory in case of errors, fine for now.
    sds* tokens = (sds*)sds_malloc(sizeof(sds) * multibulk_len_);
    memset(tokens, 0, sizeof(sds) * multibulk_len_);
    cc_->AddParsedCommand(tokens, multibulk_len_, false);
  }

  DCHECK_GT(multibulk_len_, 0u);
  DCHECK(cc_->parsed_tail);

  ParsedCommand& cur_cmd = *cc_->parsed_tail;
  while (multibulk_len_) {
    /* Read bulk length if unknown */
    if (bulk_len_ == -1) {
      size_t pos = input.find('\r', 0);
      if (pos == string_view::npos) {
        return input.size() > PROTO_INLINE_MAX_SIZE ? ERROR : NEED_MORE;
      }
      if (pos + 1 == input.size()) {
        return NEED_MORE;
      }
      if (input[0] != '$' || input[pos + 1] != '\n') {
        return ERROR;
      }

      size_t bulklen_slen = pos - 1;  // due to '$'
      long long ll = 0;
      if (!absl::SimpleAtoi(input.substr(1, bulklen_slen), &ll) || ll < 0) {
        return ERROR;
      }

      io_buf->ConsumeInput(pos + 2);
      input = ToSV(io_buf->InputBuffer());

      bulk_len_ = ll;
    }

    /* Read bulk argument */
    if (input.size() < size_t(bulk_len_) + 2) {
      return NEED_MORE;
    }
    unsigned cur_argc = cur_cmd.argc - multibulk_len_;
    DCHECK(cur_cmd.tokens[cur_argc] == nullptr);

    cur_cmd.tokens[cur_argc] = sdsnewlen(input.data(), bulk_len_);
    io_buf->ConsumeInput(bulk_len_ + 2);
    input = ToSV(io_buf->InputBuffer());
    bulk_len_ = -1;
    multibulk_len_--;
  }  // while (multibulk_len_)

  for (size_t i = 0; i < cur_cmd.argc; i++) {
    DCHECK(cur_cmd.tokens[i] != nullptr);
  }
  cur_cmd.parse_complete = 1;
  return OK;
}

// DispatchFiber handles commands coming from the InputLoop.
// Thus, InputLoop can quickly read data from the input buffer, parse it and push
// into the dispatch queue and DispatchFiber will run those commands asynchronously with InputLoop.
// Note: in some cases, InputLoop may decide to dispatch directly and bypass the DispatchFiber.
#if 0
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
#endif

}  // namespace dfly
