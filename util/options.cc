// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/options.h"

#include "leveldb/comparator.h"
#include "leveldb/env.h"

namespace leveldb {

Options::Options() : comparator(BytewiseComparator()), env(Env::Default()) {
  size_t RDMA_write_block = 1024*1024;
  env->rdma_mg->Mempool_initialize(std::string("DataBlock"), block_size);
  env->rdma_mg->Mempool_initialize(std::string("DataIndexBlock"), RDMA_write_block);
  env->rdma_mg->Mempool_initialize(std::string("FilterBlock"), RDMA_write_block);
  env->rdma_mg->Mempool_initialize(std::string("FlushBuffer"), RDMA_write_block);
  env->SetBackgroundThreads(max_background_flushes,ThreadPoolType::FlushThreadPool);
  env->SetBackgroundThreads(max_background_compactions,ThreadPoolType::CompactionThreadPool);
}

}  // namespace leveldb
