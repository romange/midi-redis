// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/conn_context.h"

#include "base/logging.h"
#include "server/dragonfly_connection.h"

namespace dfly {

using namespace std;

ConnectionContext::ConnectionContext(util::FiberSocketBase* stream, Connection* owner)
    : reply_builder_(owner->protocol(), stream), owner_(owner) {
}

Protocol ConnectionContext::protocol() const {
  return owner_->protocol();
}

void ConnectionContext::ForeignSendStored() {
  reply_builder_.SendStored();
  unsigned prev = conn_state.pending_requests.fetch_sub(1, std::memory_order_acq_rel);
  DCHECK(prev > 0);
}

void ConnectionContext::EnqueueRequest(Request* req) {
  // updates tail only atomically, if queue is empty (therefore no consumers),
  // updates its head as well.
  DCHECK(!req->send_ready);

  req->next.store(nullptr, std::memory_order_relaxed);
  Request* prev_tail = dispatch_q_tail.exchange(req, std::memory_order_acq_rel);
  if (prev_tail) {
    // We are acessing an object potentially consumed and deleted by another thread.
    // See how in DeleteHeadChain we first ensure dispatch_q_tail is not updated before
    // deleting the object.
    prev_tail->next.store(req, std::memory_order_release);
  } else {
    DCHECK(!dispatch_q_head.load(std::memory_order_acquire));
    // Queue was empty.
    dispatch_q_head.store(req, std::memory_order_release);
  }
}

void ConnectionContext::SendError(Request* req, std::string_view str) {
  auto* head = dispatch_q_head.load(std::memory_order_acquire);
  if (head == req) {
    // Send directly head and all the other queued requests.
    reply_builder_.SendError(str);
    DeleteHeadChain();
  } else {
    // Enqueue an error response into the req.
    req->resp = ErrorString{string{str}};
    req->send_ready.store(true, std::memory_order_release);
  }
};

// Prerequisite: the queue is not empty and the caller consumer is allowed to pop head.
void ConnectionContext::DeleteHeadChain() {
  // Avoid ABA problem by resetting the head during the head update. It could be that
  // any Request deleted here, will be readded by coordinator thread,
  // and then another thread will check against dispatch_q_head to mistakenly think they
  // are first in the queue.
  // After this point no other thread can pull from the queue.
  auto* head = dispatch_q_head.exchange(nullptr, std::memory_order_acq_rel);
  DCHECK(head);

  Request* current = head;
  do {
    do {
      // Send all the ready requests in the queue.
      if (std::holds_alternative<ErrorString>(current->resp)) {
        reply_builder_.SendError(std::get<ErrorString>(current->resp));
      } else {
        reply_builder_.SendStored();
      }
      Request* next = current->next.load(std::memory_order_relaxed);

      if (next == nullptr) {
        // current == dispatch_q_tail, we can not delete it before ensuring producer is not
        // chaining it (see prev_tail->next.store in EnqueueRequest).
        Request* expected = current;

        if (!dispatch_q_tail.compare_exchange_strong(expected, nullptr,
                                                     std::memory_order_acq_rel)) {
          // tail was updated, so "tail->next" is in process of being updated as well.
          // There is a critical section between dispatch_q_tail and prev_tail->next update.
          // which warrants this blocking point - we processed current so we must delete it,
          // but if it's in process of being enqueued by the producer we can not delete,
          // until its next is updated.
          do {
            next = current->next.load(std::memory_order_relaxed);
          } while (next == nullptr);
        }
      }
      delete current;
      current = next;
    } while (current && current->send_ready.load(std::memory_order_relaxed));

    if (!current)  // Reached the end of the queue, dispatch_q_head will stay nullptr.
      break;

    // current is not null, therefore dispatch_q_tail was never updated to nullptr,
    // therefore we must fix dispatch_q_head to the current head.

    // There are more requests in the queue but they are not ready to send last time we checked.
    // Update head but then double-check send_ready.
    dispatch_q_head.store(current, std::memory_order_release);
  } while (current->send_ready.load(std::memory_order_acquire));
}

}  // namespace dfly
