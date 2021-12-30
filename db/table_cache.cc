// Copyright (c) 2011 The TimberSaw Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"

#include "db/filename.h"
#include "table/table_memoryside.h"
#include <utility>

#include "TimberSaw/env.h"
#include "TimberSaw/table.h"

#include "util/coding.h"

namespace TimberSaw {
#ifdef PROCESSANALYSIS
std::atomic<uint64_t> TableCache::GetTimeElapseSum = 0;
std::atomic<uint64_t> TableCache::GetNum = 0;
std::atomic<uint64_t> TableCache::filtered = 0;
std::atomic<uint64_t> TableCache::not_filtered = 0;
std::atomic<uint64_t> TableCache::DataBinarySearchTimeElapseSum = 0;
std::atomic<uint64_t> TableCache::IndexBinarySearchTimeElapseSum = 0;
std::atomic<uint64_t> TableCache::DataBlockFetchBeforeCacheElapseSum = 0;
std::atomic<uint64_t> TableCache::foundNum = 0;

std::atomic<uint64_t> TableCache::cache_hit_look_up_time = 0;
std::atomic<uint64_t> TableCache::cache_miss_block_fetch_time = 0;
std::atomic<uint64_t> TableCache::cache_hit = 0;
std::atomic<uint64_t> TableCache::cache_miss = 0;
#endif
union SSTable {
//  RandomAccessFile* file;
//  std::weak_ptr<RemoteMemTableMetaData> remote_table;
  Table* table_compute;
  Table_Memory_Side* table_memory;
};

static void DeleteEntry_Compute(const Slice& key, void* value) {
  SSTable* tf = reinterpret_cast<SSTable*>(value);
  delete tf->table_compute;
//  delete tf->file;
  delete tf;
}
static void DeleteEntry_Memory(const Slice& key, void* value) {
  SSTable* tf = reinterpret_cast<SSTable*>(value);
  delete tf->table_memory;
  //  delete tf->file;
  delete tf;
}
static void UnrefEntry(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(h);
}

TableCache::TableCache(const std::string& dbname, const Options& options,
                       int entries)
    : env_(options.env),
      dbname_(dbname),
      options_(options),
      cache_(NewLRUCache(entries)) {}

TableCache::~TableCache() {
#ifdef PROCESSANALYSIS
  if (TableCache::GetNum.load() >0)
    printf("Cache Get time statics is %zu, %zu, %zu, need binary search: "
           "%zu, filtered %zu, foundNum is %zu\n",
           TableCache::GetTimeElapseSum.load(), TableCache::GetNum.load(),
           TableCache::GetTimeElapseSum.load()/TableCache::GetNum.load(),
           TableCache::not_filtered.load(), TableCache::filtered.load(),
           TableCache::foundNum.load());
  if (TableCache::not_filtered.load() > 0){
    printf("Average time elapse for Data binary search is %zu, "
           "Average time elapse for Index binary search is %zu,"
           " Average time elapse for data block fetch before cache is %zu\n",
           TableCache::DataBinarySearchTimeElapseSum.load()/TableCache::not_filtered.load(),
           TableCache::IndexBinarySearchTimeElapseSum.load()/TableCache::not_filtered.load(),
           TableCache::DataBlockFetchBeforeCacheElapseSum.load()/TableCache::not_filtered.load());
  }
  if (TableCache::cache_miss>0&&TableCache::cache_hit>0){
    printf("Cache hit Num %zu, average look up time %zu, Cache miss %zu, average Block Fetch time %zu\n",
           TableCache::cache_hit.load(),TableCache::cache_hit_look_up_time.load()/TableCache::cache_hit.load(),
           TableCache::cache_miss.load(),TableCache::cache_miss_block_fetch_time.load()/TableCache::cache_miss.load());
  }
#endif
  delete cache_;
}

Status TableCache::FindTable(
    std::shared_ptr<RemoteMemTableMetaData> Remote_memtable_meta,
    Cache::Handle** handle) {
  Status s;
  char buf[sizeof(Remote_memtable_meta->number) + sizeof(Remote_memtable_meta->creator_node_id)];
  EncodeFixed64(buf, Remote_memtable_meta->number);
  memcpy(buf + sizeof(Remote_memtable_meta->number), &Remote_memtable_meta->creator_node_id,
         sizeof(Remote_memtable_meta->creator_node_id));
  Slice key(buf, sizeof(buf));
  *handle = cache_->Lookup(key);
  if (*handle == nullptr) {
    Table* table = nullptr;

    if (s.ok()) {
      s = Table::Open(options_, &table, Remote_memtable_meta);
    }
    //TODO(ruihong): add remotememtablemeta and Table to the cache entry.
    if (!s.ok()) {
      assert(table == nullptr);
//      delete file;
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
    } else {
      SSTable* tf = new SSTable;
//      tf->file = file;
//      tf->remote_table = Remote_memtable_meta;
      tf->table_compute = table;
      assert(table->rep_ != nullptr);
      *handle = cache_->Insert(key, tf, 1, &DeleteEntry_Compute);
    }
  }
  return s;
}
Status TableCache::FindTable_MemorySide(std::shared_ptr<RemoteMemTableMetaData> Remote_memtable_meta, Cache::Handle** handle){
{
  Status s;
  char buf[sizeof(Remote_memtable_meta->number) + sizeof(Remote_memtable_meta->creator_node_id)];
  EncodeFixed64(buf, Remote_memtable_meta->number);
  memcpy(buf + sizeof(Remote_memtable_meta->number), &Remote_memtable_meta->creator_node_id,
         sizeof(Remote_memtable_meta->creator_node_id));
  Slice key(buf, sizeof(buf));
  *handle = cache_->Lookup(key);
  if (*handle == nullptr) {
    Table_Memory_Side* table = nullptr;
    DEBUG("FindTable_MemorySide\n");
    if (s.ok()) {
      s = Table_Memory_Side::Open(options_, &table, Remote_memtable_meta);
      DEBUG_arg("file number inserted to the cache is %lu", Remote_memtable_meta.get()->number);
      DEBUG_arg("Remote_memtable_meta pointer is %p", Remote_memtable_meta.get());
    }
    //TODO(ruihong): add remotememtablemeta and Table to the cache entry.
    if (!s.ok()) {
      assert(table == nullptr);
      //      delete file;
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
    } else {
      SSTable* tf = new SSTable;
      //      tf->file = file;
      //      tf->remote_table = Remote_memtable_meta;
      tf->table_memory = table;
      assert(table->rep_ != nullptr);
      assert(static_cast<RemoteMemTableMetaData*>(table->Get_rdma())->number != 0);
      *handle = cache_->Insert(key, tf, 1, &DeleteEntry_Memory);
    }
  }
  return s;
}
}
Iterator* TableCache::NewIterator(
    const ReadOptions& options,
    std::shared_ptr<RemoteMemTableMetaData> remote_table, Table** tableptr) {
  if (tableptr != nullptr) {
    *tableptr = nullptr;
  }

  Cache::Handle* handle = nullptr;
  Status s = FindTable(std::move(remote_table), &handle);
  if (!s.ok()) {
    return NewErrorIterator(s);
  }

  Table* table = reinterpret_cast<SSTable*>(cache_->Value(handle))->table_compute;
  Iterator* result = table->NewIterator(options);
  result->RegisterCleanup(&UnrefEntry, cache_, handle);
  if (tableptr != nullptr) {
    *tableptr = table;
  }
  return result;
}
Iterator* TableCache::NewIterator_MemorySide(
    const ReadOptions& options,
    std::shared_ptr<RemoteMemTableMetaData> remote_table,
    Table_Memory_Side** tableptr) {
  if (tableptr != nullptr) {
    *tableptr = nullptr;
  }

  Cache::Handle* handle = nullptr;
#ifndef NDEBUG
  void* p = remote_table.get();

  if (reinterpret_cast<long>(p) == 0x7fff94151070)
    printf("check for NewIterator_MemorySide\n");
#endif
  Status s = FindTable_MemorySide(std::move(remote_table), &handle);
  if (!s.ok()) {
    return NewErrorIterator(s);
  }

  Table_Memory_Side* table = reinterpret_cast<SSTable*>(cache_->Value(handle))->table_memory;
  assert(p == table->Get_rdma());
  Iterator* result = table->NewIterator(options);
  result->RegisterCleanup(&UnrefEntry, cache_, handle);
  if (tableptr != nullptr) {
    *tableptr = table;
  }
  return result;
}
Status TableCache::Get(const ReadOptions& options,
                       std::shared_ptr<RemoteMemTableMetaData> f,
                       const Slice& k, void* arg,
                       void (*handle_result)(void*, const Slice&,
                                             const Slice&)) {
#ifdef PROCESSANALYSIS
  auto start = std::chrono::high_resolution_clock::now();
#endif
  Cache::Handle* handle = nullptr;
  Status s = FindTable(f, &handle);
  if (s.ok()) {
    Table* t = reinterpret_cast<SSTable*>(cache_->Value(handle))->table_compute;
    s = t->InternalGet(options, k, arg, handle_result);
    cache_->Release(handle);
  }
#ifdef PROCESSANALYSIS
  auto stop = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//    std::printf("Get from SSTables (not found) time elapse is %zu\n",  duration.count());
  TableCache::GetTimeElapseSum.fetch_add(duration.count());
  TableCache::GetNum.fetch_add(1);
#endif
  return s;
}

void TableCache::Evict(uint64_t file_number) {
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  cache_->Erase(Slice(buf, sizeof(buf)));
}

}  // namespace TimberSaw
