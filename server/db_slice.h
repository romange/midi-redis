// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "server/common_types.h"
#include "server/op_status.h"


namespace dfly {

class DbSlice {
  struct InternalDbStats {
    // Object memory usage besides hash-table capacity.
    size_t obj_memory_usage = 0;
  };

 public:
  DbSlice(uint32_t index, EngineShard* owner);
  ~DbSlice();


  // Activates `db_ind` database if it does not exist (see ActivateDb below).
  void Reserve(DbIndex db_ind, size_t key_size);

  OpResult<MainIterator> Find(DbIndex db_index, std::string_view key) const;

  // Return .second=true if insertion ocurred, false if we return the existing key.
  std::pair<MainIterator, bool> AddOrFind(DbIndex db_ind, std::string_view key);

  // Adds a new entry. Requires: key does not exist in this slice.
  void AddNew(DbIndex db_ind, std::string_view key, MainValue obj, uint64_t expire_at_ms);

  // Adds a new entry if a key does not exists. Returns true if insertion took place,
  // false otherwise. expire_at_ms equal to 0 - means no expiry.
  bool AddIfNotExist(DbIndex db_ind, std::string_view key, MainValue obj, uint64_t expire_at_ms);

  // Creates a database with index `db_ind`. If such database exists does nothing.
  void ActivateDb(DbIndex db_ind);

  size_t db_array_size() const {
    return db_arr_.size();
  }

  bool IsDbValid(DbIndex id) const {
    return bool(db_arr_[id].main_table);
  }

  // Returns existing keys count in the db.
  size_t DbSize(DbIndex db_ind) const;

  ShardId shard_id() const { return shard_id_;}

 private:

  void CreateDbRedis(unsigned index);

  ShardId shard_id_;

  EngineShard* owner_;

  struct DbRedis {
    std::unique_ptr<MainTable> main_table;
    mutable InternalDbStats stats;
  };

  std::vector<DbRedis> db_arr_;
};

}  // namespace dfly
