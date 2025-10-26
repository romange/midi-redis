// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/conn_context.h"

#include "server/dragonfly_connection.h"
#include "base/logging.h"

namespace dfly {

ConnectionContext::ConnectionContext(util::FiberSocketBase* stream, Connection* owner)
    : ReplyBuilder(owner->protocol(), stream), owner_(owner) {
}

Protocol ConnectionContext::protocol() const {
  return owner_->protocol();
}

void ConnectionContext::ForeignSendStored() {
  SendStored();
  unsigned prev = conn_state.pending_requests.fetch_sub(1, std::memory_order_acq_rel);
  DCHECK(prev > 0);
}

}  // namespace dfly
