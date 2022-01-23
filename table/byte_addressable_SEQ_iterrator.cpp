//
// Created by ruihong on 1/21/22.
//
#define PREFETCH_GRANULARITY  (1024*1024)
#include "byte_addressable_SEQ_iterrator.h"
#include "TimberSaw/env.h"
#include "port/likely.h"
namespace TimberSaw {
//Note: the memory side KVReader should be passed as block function
ByteAddressableSEQIterator::ByteAddressableSEQIterator(Iterator* index_iter,
                                                     KVFunction kv_function,
                                                     void* arg,
                                                     const ReadOptions& options,
                                                     bool compute_side)
    : compute_side_(compute_side),
//      mr_addr(nullptr),
      kv_function_(kv_function),
      arg_(arg),
      options_(options),
      status_(Status::OK()),
      index_iter_(index_iter),
      valid_(false) {
    prefetched_mr = new ibv_mr{};
    Env::Default()->rdma_mg->Allocate_Local_RDMA_Slot(*prefetched_mr, "Prefetch");
}

ByteAddressableSEQIterator::~ByteAddressableSEQIterator() {
  auto rdma_mg = Env::Default()->rdma_mg;
  if (poll_number != 0){
    ibv_wc* wc = new ibv_wc[poll_number];
    rdma_mg->poll_completion(wc, poll_number, "read_local", true);
  }
  rdma_mg->Deallocate_Local_RDMA_Slot(prefetched_mr->addr, "Prefetch");
  delete prefetched_mr;


  //  DEBUG_arg("TWOLevelIterator destructing, this pointer is %p\n", this);
};

void ByteAddressableSEQIterator::Seek(const Slice& target) {
  index_iter_.Seek(target);
  GetKVInitial();

}

void ByteAddressableSEQIterator::SeekToFirst() {
  index_iter_.SeekToFirst();
  GetKVInitial();

}

void ByteAddressableSEQIterator::SeekToLast() {
  valid_ = false;
  status_ = Status::NotSupported("SeekToLast");
}

void ByteAddressableSEQIterator::Next() {
  GetNextKV();

}

void ByteAddressableSEQIterator::Prev() {

}
void ByteAddressableSEQIterator::GetKVInitial(){
  if(index_iter_.Valid()){
    Slice handle_content = index_iter_.value();
    BlockHandle handle;
    handle.DecodeFrom(&handle_content);
    iter_offset = handle.offset();
    valid_ = Fetch_next_buffer(iter_offset);
    assert(valid_);
    auto rdma_mg = Env::Default()->rdma_mg;
    // Only support forward iterator for sequential access iterator.
    uint32_t key_size, value_size;
    if (iter_offset + 8 >= cur_prefetch_status){
      ibv_wc wc[1];
      rdma_mg->poll_completion(wc,1, "read_local", true);
      cur_prefetch_status += PREFETCH_GRANULARITY;
    }
    Slice Size_buff = Slice(iter_ptr, 8);
    GetFixed32(&Size_buff, &key_size);
    GetFixed32(&Size_buff, &value_size);
    iter_ptr += 8;
    //Check whether the
    if (UNLIKELY(iter_offset + key_size + value_size >= cur_prefetch_status)){
      ibv_wc wc[1];
      rdma_mg->poll_completion(wc,1, "read_local", true);
      cur_prefetch_status += PREFETCH_GRANULARITY;
      poll_number--;
      assert(poll_number >= 0);
    }
    key_.SetKey(Slice(iter_ptr, key_size), false /* copy */);
    iter_ptr += key_size;
    value_ = Slice(iter_ptr, value_size);
    iter_ptr += value_size;
    iter_offset += key_size + value_size + 2*sizeof(uint32_t);
    assert(iter_ptr - (char*)prefetched_mr->addr <= prefetched_mr->length);
  }else{
    valid_ = false;
  }
}


void ByteAddressableSEQIterator::GetNextKV() {

  if (iter_ptr == nullptr || iter_ptr == prefetched_mr->length + (char*)prefetched_mr->addr){
    valid_ = Fetch_next_buffer(iter_offset);
    //TODO: reset all the relevent metadata such as iter_ptr, cur_prefetch_status.
  }
  //TODO: The Get KV need to wait if the data has not been fetched already, need to Poll completion
  // Use the cur_prefetch_status to represent the postion for current prefetching.
//    valid_ = true;
  auto rdma_mg = Env::Default()->rdma_mg;
  // Only support forward iterator for sequential access iterator.
  uint32_t key_size, value_size;
  if (UNLIKELY(iter_offset + 8 >= cur_prefetch_status)){
    ibv_wc wc[1];
    rdma_mg->poll_completion(wc,1, "read_local", true);
    cur_prefetch_status += PREFETCH_GRANULARITY;
  }
  Slice Size_buff = Slice(iter_ptr, 8);
  GetFixed32(&Size_buff, &key_size);
  GetFixed32(&Size_buff, &value_size);
  iter_ptr += 8;
  //Check whether the
  if (UNLIKELY(iter_offset + key_size + value_size >= cur_prefetch_status)){
    ibv_wc wc[1];
    rdma_mg->poll_completion(wc,1, "read_local", true);
    cur_prefetch_status += PREFETCH_GRANULARITY;
  }
  key_.SetKey(Slice(iter_ptr, key_size), false /* copy */);
  iter_ptr += key_size;
  value_ = Slice(iter_ptr, value_size);
  iter_ptr += value_size;
  iter_offset += key_size + value_size + 2*sizeof(uint32_t);
  assert(iter_ptr - (char*)prefetched_mr->addr <= prefetched_mr->length);
}
// The offset has to be the start address of the SSTable chunk, which means
// we need to fetch a whole chunk for this prefetching
bool ByteAddressableSEQIterator::Fetch_next_buffer(size_t offset) {
  Table* table = reinterpret_cast<Table*>(arg_);
  ibv_mr mr = {};
  auto tablemeta = table->rep->remote_table.lock();
  auto rdma_mg = Env::Default()->rdma_mg;
  if(Find_prefetch_MR(&tablemeta->remote_data_mrs, offset, &mr)){
    size_t total_len = mr.length;
    ibv_mr remote_mr = mr;
    remote_mr.length = PREFETCH_GRANULARITY;
    ibv_mr local_mr = *prefetched_mr;
    local_mr.length = PREFETCH_GRANULARITY;
    for (size_t i = 0; i < mr.length/PREFETCH_GRANULARITY + 1; ++i) {
      remote_mr.addr = (void*)((char*)remote_mr.addr + i*PREFETCH_GRANULARITY);
      local_mr.addr = (void*)((char*)local_mr.addr + i*PREFETCH_GRANULARITY);
      if (total_len > PREFETCH_GRANULARITY){

        rdma_mg->RDMA_Read(&remote_mr, &local_mr, PREFETCH_GRANULARITY, "read_local", IBV_SEND_SIGNALED, 0);
        total_len -= PREFETCH_GRANULARITY;
      }else if (total_len > 0){
        remote_mr.addr = (void*)((char*)remote_mr.addr + i*PREFETCH_GRANULARITY);
        local_mr.addr = (void*)((char*)local_mr.addr + i*PREFETCH_GRANULARITY);
        remote_mr.length = total_len;
        local_mr.length = total_len;
        rdma_mg->RDMA_Read(&remote_mr, &local_mr, total_len, "read_local", IBV_SEND_SIGNALED, 0);
        total_len = 0;
      }
    }
    prefetched_mr->length = mr.length;
    iter_ptr = (char*)prefetched_mr->addr;
    poll_number = mr.length/PREFETCH_GRANULARITY;
    assert(total_len == 0);
    return true;
  }else{
    return false;
  }

  //  return mr;
}
}