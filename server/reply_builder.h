// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#include <string_view>

#include "io/io.h"
#include "server/dfly_protocol.h"
#include "server/op_status.h"
#include "util/fiber_socket_base.h"

namespace dfly {

class BaseSerializer {
 public:
  explicit BaseSerializer(util::FiberSocketBase* sink);

  std::error_code ec() const {
    return ec_;
  }

  void CloseConnection() {
    if (!ec_)
      ec_ = std::make_error_code(std::errc::connection_aborted);
  }

  // In order to reduce interrupt rate we allow coalescing responses together using
  // Batch mode. It is controlled by Connection state machine because it makes sense only
  // when pipelined requests are arriving.
  void SetBatchMode(bool batch) {
    should_batch_ = batch;
  }

  //! Sends a string as is without any formatting. raw should be encoded according to the protocol.
  void SendDirect(std::string_view str);

  void Send(const iovec* v, uint32_t len);

 private:
  util::FiberSocketBase* sink_;
  std::error_code ec_;
  std::string batch_;

  bool should_batch_ = false;
};

class RespSerializer : public BaseSerializer {
 public:
  RespSerializer(util::FiberSocketBase* sink) : BaseSerializer(sink) {
  }

  //! See https://redis.io/topics/protocol
  void SendSimpleString(std::string_view str);
  void SendNull();

  /// aka "$6\r\nfoobar\r\n"
  void SendBulkString(std::string_view str);
};

class MemcacheSerializer : public BaseSerializer {
 public:
  explicit MemcacheSerializer(util::FiberSocketBase* sink) : BaseSerializer(sink) {
  }

  void SendStored();
  void SendError();
};

class ReplyBuilder {
 public:
  ReplyBuilder(Protocol protocol, util::FiberSocketBase* stream);

  void SendStored();

  void SendError(std::string_view str);
  void SendError(OpStatus status);

  void SendOk() {
    as_resp()->SendSimpleString("OK");
  }

  std::error_code ec() const {
    return serializer_->ec();
  }

  void SendMCClientError(std::string_view str);
  void EndMultilineReply();

  void SendSimpleRespString(std::string_view str) {
    as_resp()->SendSimpleString(str);
  }

  void SendRespBlob(std::string_view str) {
    as_resp()->SendDirect(str);
  }

  void SendGetReply(std::string_view key, uint32_t flags, std::string_view value);
  void SendGetNotFound();

  void SetBatchMode(bool mode) {
    serializer_->SetBatchMode(mode);
  }

  // Resp specific.
  // This one is prefixed with + and with clrf added automatically to each item..
  void SendSimpleStrArr(const std::string_view* arr, uint32_t count);

 private:
  RespSerializer* as_resp() {
    return static_cast<RespSerializer*>(serializer_.get());
  }
  MemcacheSerializer* as_mc() {
    return static_cast<MemcacheSerializer*>(serializer_.get());
  }

  std::unique_ptr<BaseSerializer> serializer_;
  Protocol protocol_;
};

}  // namespace dfly
