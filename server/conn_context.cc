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

void ConnectionContext::SendSimpleRespString(std::string_view str, Request* req) {
  if (!req)
    req = current_request;

  CHECK(req);
  if (req) {
    // Enqueue a success response into the req.
    req->resp = SimpleString{string{str}};
    req->ready.store(true, memory_order_release);

    request_queue_.TryDrain(&reply_builder_);
  } else {
    reply_builder_.SendSimpleRespString(str);
  }
}

void ConnectionContext::SendStored(Request* req) {
  // Not valid for memcache protocol.
  SendOk(req);
}

void ConnectionContext::SendOk(Request* req) {
  SendSimpleRespString("OK", req);
}

void ConnectionContext::SendError(std::string_view str, Request* req) {
  if (!req)
    req = current_request;
  CHECK(req);
  if (req) {
    // Enqueue an error response into the req.
    req->resp = ErrorString{string{str}};
    req->ready.store(true, memory_order_release);

    request_queue_.TryDrain(&reply_builder_);
  } else {
    reply_builder_.SendError(str);
  }
};

void ConnectionContext::RequestQueue::Enqueue(LockFreeRecord* req) {
  // updates tail only atomically, if queue is empty (therefore no consumers),
  // updates its head as well.
  DCHECK(!req->ready);

  pending_drain_calls_.fetch_add(1, memory_order_relaxed);
  req->next.store(nullptr, memory_order_relaxed);
  LockFreeRecord* prev_tail = dispatch_q_tail_.exchange(req, memory_order_acq_rel);
  if (prev_tail) {
    // We are acessing an object potentially consumed and deleted by another thread.
    // See how in TryDrainQueue we first ensure dispatch_q_tail is not updated before
    // deleting the object.
    prev_tail->next.store(req, memory_order_release);
  } else {
    DCHECK(!dispatch_q_head_.load(memory_order_acquire));
    // Queue was empty.
    dispatch_q_head_.store(req, memory_order_release);
  }
}

void ConnectionContext::RequestQueue::TryDrain(ReplyBuilder* builder) {
  pending_drain_calls_.fetch_sub(1, memory_order_relaxed);

  try_drain_start:
  // 2. Fetch the head of the queue (sync point with EnqueueRequest).
  LockFreeRecord* head = dispatch_q_head_.exchange(nullptr, memory_order_acq_rel);
  if (head == nullptr) {
    return;
  }

  // --- We have the baton. ---
  while (head->ready.load(memory_order_acquire)) {
    // --- Head is ready. Dequeue and send it. ---
    LockFreeRecord* next = head->next.load(memory_order_relaxed);

    // 4. Handle tail-race with producer.
    if (next == nullptr) {
      LockFreeRecord* expected = head;
      if (!dispatch_q_tail_.compare_exchange_strong(expected, nullptr, memory_order_acq_rel)) {

        // Spin-wait for producer to link.
        while (!(next = head->next.load(memory_order_acquire))) ;
      }
    }

    // Process and delete the old head.
    Request* req = static_cast<Request*>(head);
    if (std::holds_alternative<ErrorString>(req->resp)) {
      builder->SendError(std::get<ErrorString>(req->resp));
    } else if (std::holds_alternative<SimpleString>(req->resp)) {
      builder->SendSimpleRespString(std::get<SimpleString>(req->resp));
    }
    delete head;

    if (next == nullptr) {
      // Queue is empty and we are done.
      return;
    }
    head = next;
  }  // while head is ready


  dispatch_q_head_.store(head, memory_order_release); // head can not be accessed from this point.

  // if no more items, but we just restored head, it means some TryDrain calls skipped,
  // due to dispatch_q_head_ being held. There is a risk of starvation here,
  // therefore, lets try again.
  if (pending_drain_calls_.load(memory_order_relaxed)  == 0)
    goto try_drain_start;
}

}  // namespace dfly
