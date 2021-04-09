// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/options.h"

#include "leveldb/comparator.h"
#include "leveldb/env.h"

namespace leveldb {

Options::Options() : comparator(BytewiseComparator()), env(Env::Default()) {
  env->rdma_mg->Mempool_initialize(std::string("DataBlock"), block_size);
  env->rdma_mg->Mempool_initialize(std::string("DataIndexBlock"), write_buffer_size);
  env->rdma_mg->Mempool_initialize(std::string("FilterBlock"), write_buffer_size);
  env->rdma_mg->Mempool_initialize(std::string("FlushBuffer"), write_buffer_size);

}

}  // namespace leveldb
