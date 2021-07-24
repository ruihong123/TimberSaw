// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table.h"

#include "leveldb/cache.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block.h"
#include "db/table_cache.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"

namespace leveldb {

struct Table::Rep {
  ~Rep() {
    delete filter;
//    delete[] filter_data;
    delete index_block;
  }

  Options options;
  Status status;
  // weak_ptr because if there is cached value in the table cache then the obsoleted SST
  // will never be garbage collected.
  std::weak_ptr<RemoteMemTableMetaData> remote_table;
  uint64_t cache_id;
  FullFilterBlockReader* filter;
//  const char* filter_data;

  BlockHandle metaindex_handle;  // Handle to metaindex_block: saved from footer
  Block* index_block;
};

Status Table::Open(const Options& options, Table** table,
                   const std::shared_ptr<RemoteMemTableMetaData>& Remote_table_meta) {
  *table = nullptr;


  // Read the index block
  Status s = Status::OK();
  BlockContents index_block_contents;
  ReadOptions opt;
  if (options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  s = ReadDataIndexBlock(Remote_table_meta->remote_dataindex_mrs.begin()->second,
                         opt, &index_block_contents);

  if (s.ok()) {
    // We've successfully read the footer and the index block: we're
    // ready to serve requests.
    Block* index_block = new Block(index_block_contents, IndexBlock);
    Rep* rep = new Table::Rep;
    rep->options = options;
//    rep->file = file;
    rep->remote_table = Remote_table_meta;
//    rep->metaindex_handle = footer.metaindex_handle();
    rep->index_block = index_block;
    assert(rep->index_block->size() > 0);
    rep->cache_id = (options.block_cache ? options.block_cache->NewId() : 0);
//    rep->filter_data = nullptr;
    rep->filter = nullptr;
    *table = new Table(rep);
    (*table)->ReadFilter();
//    (*table)->ReadMeta(footer);
  }else{
    assert(false);
  }

  return s;
}

void Table::ReadFilter() {
  if (rep_->options.filter_policy == nullptr) {
    return;  // Do not need any metadata
  }
  // We might want to unify with ReadDataBlock() if we start
  // requiring checksum verification in Table::Open.
  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  BlockContents block;
  if (!ReadFilterBlock(rep_->remote_table.lock()->remote_filter_mrs.begin()->second, opt, &block).ok()) {
    return;
  }
//  if (block.heap_allocated) {
//    rep_->filter_data = block.data.data();  // Will need to delete later
//  }
  rep_->filter =
      new FullFilterBlockReader(block.data, rep_->remote_table.lock()->rdma_mg);
}

Table::~Table() { delete rep_; }

static void DeleteBlock(void* arg, void* ignored) {
  delete reinterpret_cast<Block*>(arg);
}

static void DeleteCachedBlock(const Slice& key, void* value) {
  Block* block = reinterpret_cast<Block*>(value);
  delete block;
}

static void ReleaseBlock(void* arg, void* h) {
  Cache* cache = reinterpret_cast<Cache*>(arg);
  Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
  cache->Release(handle);
}

// Convert an index iterator value (i.e., an encoded BlockHandle)
// into an iterator over the contents of the corresponding block.
Iterator* Table::BlockReader(void* arg, const ReadOptions& options,
                             const Slice& index_value) {
  Table* table = reinterpret_cast<Table*>(arg);
  Cache* block_cache = table->rep_->options.block_cache;
  Block* block = nullptr;
  Cache::Handle* cache_handle = nullptr;

  BlockHandle handle;
  Slice input = index_value;
  Status s = handle.DecodeFrom(&input);
  // We intentionally allow extra stuff in index_value so that we
  // can add more features in the future.

  if (s.ok()) {
    BlockContents contents;
    if (block_cache != nullptr) {
      char cache_key_buffer[16];
      EncodeFixed64(cache_key_buffer, table->rep_->cache_id);
      EncodeFixed64(cache_key_buffer + 8, handle.offset());
      Slice key(cache_key_buffer, sizeof(cache_key_buffer));
#ifdef GETANALYSIS
      auto start = std::chrono::high_resolution_clock::now();
#endif
      cache_handle = block_cache->Lookup(key);
#ifdef GETANALYSIS
      auto stop = std::chrono::high_resolution_clock::now();
      auto lookup_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
#endif
      if (cache_handle != nullptr) {
        block = reinterpret_cast<Block*>(block_cache->Value(cache_handle));
//        DEBUG("Cache hit\n");
#ifdef GETANALYSIS
        TableCache::cache_hit.fetch_add(1);
        TableCache::cache_hit_look_up_time.fetch_add(lookup_duration.count());
#endif
      } else {
#ifdef GETANALYSIS
        start = std::chrono::high_resolution_clock::now();
        TableCache::cache_miss.fetch_add(1);
#endif
        s = ReadDataBlock(&table->rep_->remote_table.lock()->remote_data_mrs, options, handle, &contents);
#ifdef GETANALYSIS
        stop = std::chrono::high_resolution_clock::now();
        auto blockfetch_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
        TableCache::cache_miss_block_fetch_time.fetch_add(blockfetch_duration.count());
#endif

        if (s.ok()) {
          block = new Block(contents, DataBlock);
          if (options.fill_cache) {
            cache_handle = block_cache->Insert(key, block, block->size(),
                                               &DeleteCachedBlock);
          }
        }
      }
    } else {
      s = ReadDataBlock(&table->rep_->remote_table.lock()->remote_data_mrs, options, handle, &contents);
      if (s.ok()) {
        block = new Block(contents, DataBlock);
      }
    }
  }

  Iterator* iter;
  if (block != nullptr) {
    iter = block->NewIterator(table->rep_->options.comparator);
    if (cache_handle == nullptr) {
      iter->RegisterCleanup(&DeleteBlock, block, nullptr);
    } else {
      iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
    }
  } else {
    iter = NewErrorIterator(s);
  }
  iter->SeekToFirst();
//  DEBUG_arg("First key after the block create %s", iter->key().ToString().c_str());
  return iter;
}

Iterator* Table::NewIterator(const ReadOptions& options) const {
  return NewTwoLevelIterator(
      rep_->index_block->NewIterator(rep_->options.comparator),
      &Table::BlockReader, const_cast<Table*>(this), options);
}

Status Table::InternalGet(const ReadOptions& options, const Slice& k, void* arg,
                          void (*handle_result)(void*, const Slice&,
                                                const Slice&)) {
  Status s;
  FullFilterBlockReader* filter = rep_->filter;
  if (filter != nullptr && !filter->KeyMayMatch(k)) {
    // Not found
#ifdef GETANALYSIS
    TableCache::filtered.fetch_add(1);
#endif
#ifndef BLOOMANALYSIS
    //assert that bloom filter is correct
    Iterator* iiter = rep_->index_block->NewIterator(rep_->options.comparator);

    iiter->Seek(k);//binary search for block index
    if (iiter->Valid()) {
      Slice handle_value = iiter->value();
      BlockHandle handle;

      Iterator* block_iter = BlockReader(this, options, iiter->value());
      block_iter->Seek(k);
      if (block_iter->Valid()) {
        (*handle_result)(arg, block_iter->key(), block_iter->value());
      }
      Saver* saver = reinterpret_cast<Saver*>(arg);
//      assert(saver->state == kNotFound);
      if(saver->state == kNotFound){
//        printf("filtered key not found\n");
        int dummy = 0;
      }else{
//        printf("filtered key found\n");
        int dummy = 0;
      }
      delete block_iter;
    }
#endif
  } else {
#ifdef GETANALYSIS
    TableCache::not_filtered.fetch_add(1);
    auto start = std::chrono::high_resolution_clock::now();
#endif
    Iterator* iiter = rep_->index_block->NewIterator(rep_->options.comparator);

    iiter->Seek(k);//binary search for block index

    if (iiter->Valid()) {

      Slice handle_value = iiter->value();

      BlockHandle handle;

      Iterator* block_iter = BlockReader(this, options, iiter->value());

      block_iter->Seek(k);

      if (block_iter->Valid()) {
        (*handle_result)(arg, block_iter->key(), block_iter->value());
      }
      s = block_iter->status();
      delete block_iter;
#ifdef GETANALYSIS
      auto stop = std::chrono::high_resolution_clock::now();
      auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//    std::printf("Block Reader time elapse is %zu\n",  duration.count());
      TableCache::BinarySearchTimeElapseSum.fetch_add(duration.count());
#endif
#ifdef GETANALYSIS
      Saver* saver = reinterpret_cast<Saver*>(arg);
      if(saver->state == kFound){
        TableCache::foundNum.fetch_add(1);
      }
#endif
    }else{
      printf("block iterator invalid\n");
      exit(1);
    }
    delete iiter;

  }

  return s;
}

uint64_t Table::ApproximateOffsetOf(const Slice& key) const {
  Iterator* index_iter =
      rep_->index_block->NewIterator(rep_->options.comparator);
  index_iter->Seek(key);
  uint64_t result;
  if (index_iter->Valid()) {
    BlockHandle handle;
    Slice input = index_iter->value();
    Status s = handle.DecodeFrom(&input);
    if (s.ok()) {
      result = handle.offset();
    } else {
      // Strange: we can't decode the block handle in the index block.
      // We'll just return the offset of the metaindex block, which is
      // close to the whole file size for this case.
      result = rep_->metaindex_handle.offset();
    }
  } else {
    // key is past the last key in the file.  Approximate the offset
    // by returning the offset of the metaindex block (which is
    // right near the end of the file).
    result = rep_->metaindex_handle.offset();
  }
  delete index_iter;
  return result;
}

}  // namespace leveldb
