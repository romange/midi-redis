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

void ConnectionContext::SendSimpleRespString(string_view str, Request* req) {
  if (!req)
    req = current_request;

  CHECK(req);

  // Enqueue a success response into the req.
  req->resp = SimpleString{string{str}};
  req->ready.store(true, memory_order_release);
  TryDrain();
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
  // Enqueue an error response into the req.
  req->resp = ErrorString{string{str}};
  req->ready.store(true, memory_order_release);

  TryDrain();
};

void ConnectionContext::SendGetReply(std::string_view key, uint32_t flags, std::string_view value,
                                     Request* req) {
  DCHECK(req);

  // Enqueue an error response into the req.
  req->resp = BulkString{string{value}};
  req->ready.store(true, memory_order_release);

  TryDrain();
}

void ConnectionContext::SendGetNotFound(Request* req) {
  DCHECK(req);

  // Enqueue an error response into the req.
  req->resp = Null{};
  req->ready.store(true, memory_order_release);

  TryDrain();
}

void ConnectionContext::TryDrain() {
  request_queue_.TryDrain([this](LockFreeRecord* rec, bool has_more) {
    Request* req = static_cast<Request*>(rec);
    if (std::holds_alternative<ErrorString>(req->resp)) {
      const auto& err = std::get<ErrorString>(req->resp);
      reply_builder_.SendError(err);
    } else if (std::holds_alternative<SimpleString>(req->resp)) {
      const auto& ok = std::get<SimpleString>(req->resp);
      reply_builder_.SendSimpleRespString(ok);
    } else if (std::holds_alternative<BulkString>(req->resp)) {
      const auto& bulk = std::get<BulkString>(req->resp);
      reply_builder_.SendBulk(bulk);
    } else if (std::holds_alternative<Null>(req->resp)) {
      reply_builder_.SendGetNotFound();
    } else {
      LOG(ERROR) << "Unknown response variant";
    }
  });
}

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

void ConnectionContext::RequestQueue::TryDrain(ProcessCb cb) {
  pending_drain_calls_.fetch_sub(1, memory_order_relaxed);

try_drain_start:

  // Fetch the head of the queue (sync point with Enqueue).
  // Note, that even though we grab the head,
  LockFreeRecord* head = dispatch_q_head_.exchange(nullptr, memory_order_acq_rel);
  if (head == nullptr) {
    return;
  }

  // --- We have the baton. ---
  while (head->ready.load(memory_order_acquire)) {
    // The order here is important. If we update dispatch_q_tail_ before processing head
    // it is possible that a producer enqueues a new item and updates the tail and
    // another consumer comes in and sees the new tail and processes it out of order.

    LockFreeRecord* next = head->next.load(memory_order_relaxed);

    cb(head, next != nullptr);

    // 4. Handle tail-race with producer.
    if (next == nullptr) {
      LockFreeRecord* expected = head;
      if (!dispatch_q_tail_.compare_exchange_strong(expected, nullptr, memory_order_acq_rel)) {
        // Spin-wait for producer to link.
        while (!(next = head->next.load(memory_order_acquire))) {
          sched_yield();
        }
      }
    }

    delete head;

    if (next == nullptr) {
      // Queue is empty and we are done.
      return;
    }
    head = next;
  }  // while head is ready

  // head can not be accessed from this point.
  dispatch_q_head_.store(head, memory_order_release);

  // we use a fence to make sure that the load below is not reordered before
  // the store above.
  atomic_thread_fence(memory_order_seq_cst);

  // if no more items, but we just restored head, it means some TryDrain calls skipped,
  // due to dispatch_q_head_ being held. There is a risk of starvation here,
  // therefore, lets try again.
  unsigned pending = pending_drain_calls_.load(memory_order_relaxed);
  DVLOG(1) << "Pending drain calls: " << pending;
  if (pending == 0)
    goto try_drain_start;
}

}  // namespace dfly
