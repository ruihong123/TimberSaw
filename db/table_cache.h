// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Thread-safe (provides internal synchronization)

#ifndef STORAGE_LEVELDB_DB_TABLE_CACHE_H_
#define STORAGE_LEVELDB_DB_TABLE_CACHE_H_

#include <cstdint>
#include <string>

#include "db/dbformat.h"
#include "leveldb/cache.h"
#include "leveldb/table.h"
#include "db/version_edit.h"
#include "port/port.h"

namespace leveldb {

class Env;

class TableCache {
 public:
  TableCache(const std::string& dbname, const Options& options, int entries);
  ~TableCache();
#ifdef GETANALYSIS
  static std::atomic<uint64_t> GetTimeElapseSum;
  static std::atomic<uint64_t> GetNum;
  static std::atomic<uint64_t> not_filtered;
  static std::atomic<uint64_t> DataBinarySearchTimeElapseSum;
  static std::atomic<uint64_t> IndexBinarySearchTimeElapseSum;
  static std::atomic<uint64_t> DataBlockFetchBeforeCacheElapseSum;
  static std::atomic<uint64_t> filtered;
  static std::atomic<uint64_t> foundNum;

  static std::atomic<uint64_t> cache_hit_look_up_time;
  static std::atomic<uint64_t> cache_miss_block_fetch_time;
  static std::atomic<uint64_t> cache_hit;
  static std::atomic<uint64_t> cache_miss;

#endif
  // Return an iterator for the specified file number (the corresponding
  // file length must be exactly "file_size" bytes).  If "tableptr" is
  // non-null, also sets "*tableptr" to point to the Table object
  // underlying the returned iterator, or to nullptr if no Table object
  // underlies the returned iterator.  The returned "*tableptr" object is owned
  // by the cache and should not be deleted, and is valid for as long as the
  // returned iterator is live.
  Iterator* NewIterator(const ReadOptions& options,
                        std::shared_ptr<RemoteMemTableMetaData> remote_table,
                        Table** tableptr = nullptr);
  static void CleanAll(){
    GetTimeElapseSum = 0;
    GetNum = 0;
    filtered = 0;
    not_filtered = 0;
    DataBinarySearchTimeElapseSum = 0;
    IndexBinarySearchTimeElapseSum = 0;
    DataBlockFetchBeforeCacheElapseSum = 0;
    foundNum = 0;

    cache_hit_look_up_time = 0;
    cache_miss_block_fetch_time = 0;
    cache_hit = 0;
    cache_miss = 0;
  }
  // If a seek to internal key "k" in specified file finds an entry,
  // call (*handle_result)(arg, found_key, found_value).
  Status Get(const ReadOptions& options,
             std::shared_ptr<RemoteMemTableMetaData> f, const Slice& k,
             void* arg,
             void (*handle_result)(void*, const Slice&, const Slice&));

  // Evict any entry for the specified file number
  void Evict(uint64_t file_number);

 private:
  Status FindTable(std::shared_ptr<RemoteMemTableMetaData> Remote_memtable_meta,
                   Cache::Handle** handle);

  Env* const env_;
  const std::string dbname_;
  const Options& options_;
  Cache* cache_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_TABLE_CACHE_H_
