// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/strings/ascii.h>
#include <absl/types/span.h>

#include <cstdint>
#include <variant>
#include <vector>

namespace dfly {

class RespExpr {
 public:
  using Buffer = absl::Span<uint8_t>;

  enum Type : uint8_t { STRING, ARRAY, INT64, NIL, NIL_ARRAY, ERROR };

  using Vec = std::vector<RespExpr>;
  Type type;
  bool has_support;  // whether pointers in this item are supported by external storage.

  std::variant<int64_t, Buffer, Vec*> u;

  RespExpr(Type t = NIL) : type(t), has_support(false) {
  }

  static Buffer buffer(std::string* s) {
    return Buffer{reinterpret_cast<uint8_t*>(s->data()), s->size()};
  }

  Buffer GetBuf() const {
    return std::get<Buffer>(u);
  }

  static const char* TypeName(Type t);
};

using RespVec = RespExpr::Vec;
using RespSpan = absl::Span<const RespExpr>;

inline std::string_view ToSV(const absl::Span<uint8_t>& s) {
  return std::string_view{reinterpret_cast<char*>(s.data()), s.size()};
}

}  // namespace dfly

namespace std {

ostream& operator<<(ostream& os, const dfly::RespExpr& e);
ostream& operator<<(ostream& os, dfly::RespSpan rspan);

}  // namespace std