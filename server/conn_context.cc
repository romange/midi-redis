// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/conn_context.h"

#include "server/dragonfly_connection.h"

namespace dfly {

ConnectionContext::ConnectionContext(::io::Sink* stream, Connection* owner)
    : owner_(owner), reply_builder_(owner->protocol(), stream) {
}

Protocol ConnectionContext::protocol() const {
  return owner_->protocol();
}

}  // namespace dfly
