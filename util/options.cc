// Copyright (c) 2011 The TimberSaw Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "TimberSaw/options.h"

#include "TimberSaw/comparator.h"
#include "TimberSaw/env.h"

namespace TimberSaw {

Options::Options() : comparator(BytewiseComparator()), env(Env::Default()) {
  if (!env->initialized){
    env->rdma_mg->Mempool_initialize(IndexChunk, INDEX_BLOCK, 0);
    env->rdma_mg->Mempool_initialize(FilterChunk, FILTER_BLOCK, 0);
    env->rdma_mg->Mempool_initialize(FlushBuffer, RDMA_WRITE_BLOCK, 0);
//    env->rdma_mg->Mempool_initialize(std::string("Prefetch"),
//                                     RDMA_WRITE_BLOCK);
    env->rdma_mg->Mempool_initialize(DataChunk, block_size, 0);
    ibv_mr* mr;
    char* buff;
    env->rdma_mg->Local_Memory_Register(&buff, &mr, 1024*1024*1024, IndexChunk);
    env->rdma_mg->Local_Memory_Register(&buff, &mr, 1024*1024*1024, FilterChunk);
    env->rdma_mg->Local_Memory_Register(&buff, &mr, 1024*1024*1024, FlushBuffer);
    env->rdma_mg->Local_Memory_Register(&buff, &mr, 1024*1024*1024, DataChunk);
  }

  env->initialized = true;
}
Options::Options(bool is_memory_side) : comparator(BytewiseComparator()), env(is_memory_side? nullptr : Env::Default()){

}

}  // namespace TimberSaw
