// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table_builder.h"

#include <cassert>



namespace leveldb {
//TOthink: how to save the remote mr?
//TOFIX : now we suppose the index and filter block will not over the write buffer.
struct TableBuilder::Rep {
  Rep(const Options& opt)
      : options(opt),
        index_block_options(opt),
        offset(0),
        offset_last_flushed(0),

        num_entries(0),
        closed(false),
        pending_index_filter_entry(false) {
    index_block_options.block_restart_interval = 1;
    std::shared_ptr<RDMA_Manager> rdma_mg = options.env->rdma_mg;
    ibv_mr* temp_data_mr;
    ibv_mr* temp_index_mr;
    ibv_mr* temp_filter_mr;
    //first create two buffer for each slot.
    rdma_mg->Allocate_Local_RDMA_Slot(temp_data_mr, "FlushBuffer");
    rdma_mg->Allocate_Local_RDMA_Slot(temp_index_mr, "FlushBuffer");
    rdma_mg->Allocate_Local_RDMA_Slot(temp_filter_mr, "FlushBuffer");
    local_data_mr.push_back(temp_data_mr);
    local_index_mr.push_back(temp_index_mr);
    local_filter_mr.push_back(temp_filter_mr);
    rdma_mg->Allocate_Local_RDMA_Slot(temp_data_mr, "FlushBuffer");
    rdma_mg->Allocate_Local_RDMA_Slot(temp_index_mr, "FlushBuffer");
    rdma_mg->Allocate_Local_RDMA_Slot(temp_filter_mr, "FlushBuffer");
    local_data_mr.push_back(temp_data_mr);
    local_index_mr.push_back(temp_index_mr);
    local_filter_mr.push_back(temp_filter_mr);
    data_block = new BlockBuilder(&index_block_options, local_data_mr[0]);
    index_block = new BlockBuilder(&index_block_options, local_index_mr[0]);
    filter_block = (opt.filter_policy == nullptr
                    ? nullptr
                    : new FilterBlockBuilder(opt.filter_policy, &local_filter_mr,
                                             &remote_filter_mrs,
                                             options.env->rdma_mg));
    status = Status::OK();
  }

  Options options;
  Options index_block_options;
  WritableFile* file;
  std::vector<ibv_mr*> local_data_mr;
  // the start index of the in use buffer
  int data_inuse_start = -1;
  // the end index of the use buffer
  int data_inuse_end = -1;
  // when start larger than end by 1, there could be two scenarios:
  // First, all the buffer are outstanding
  // second, no buffer is outstanding, those two status will both have start - end = 1
  bool data_inuse_empty = true;
  std::vector<ibv_mr*> local_index_mr;
  std::vector<ibv_mr*> local_filter_mr;

  std::map<int, ibv_mr*> remote_data_mrs;
  std::map<int, ibv_mr*> remote_dataindex_mrs;
  std::map<int, ibv_mr*> remote_filter_mrs;
//  std::vector<size_t> remote_mr_real_length;
  uint64_t offset_last_flushed;
  uint64_t offset;
  Status status;
  BlockBuilder* data_block;
  BlockBuilder* index_block;
  std::string last_key;
  int64_t num_entries;
  bool closed;  // Either Finish() or Abandon() has been called.
  FilterBlockBuilder* filter_block;

  // We do not emit the index entry for a block until we have seen the
  // first key for the next data block.  This allows us to use shorter
  // keys in the index block.  For example, consider a block boundary
  // between the keys "the quick brown fox" and "the who".  We can use
  // "the r" as the key for the index block entry since it is >= all
  // entries in the first block and < all entries in subsequent
  // blocks.
  //
  // Invariant: r->pending_index_filter_entry is true only if data_block is empty.
  bool pending_index_filter_entry;
  BlockHandle pending_data_handle;  // Handle to add to index block

  std::string compressed_output;
};
TableBuilder::TableBuilder(const Options& options) : rep_(new Rep(options)) {
  if (rep_->filter_block != nullptr) {
    rep_->filter_block->StartBlock(0);
  }
}

TableBuilder::~TableBuilder() {
  assert(rep_->closed);  // Catch errors where caller forgot to call Finish()
  if (rep_->filter_block != nullptr){
    delete rep_->filter_block;
  }
  std::shared_ptr<RDMA_Manager> rdma_mg = rep_->options.env->rdma_mg;
  for(auto iter : rep_->local_data_mr){
    rdma_mg->Deallocate_Local_RDMA_Slot(iter->addr, "FlushBuffer");
  }
  for(auto iter : rep_->local_index_mr){
    rdma_mg->Deallocate_Local_RDMA_Slot(iter->addr, "FlushBuffer");
  }
  for(auto iter : rep_->local_filter_mr){
    rdma_mg->Deallocate_Local_RDMA_Slot(iter->addr, "FlushBuffer");
  }
  delete rep_->data_block;
  delete rep_->index_block;
  delete rep_;
}

Status TableBuilder::ChangeOptions(const Options& options) {
  // Note: if more fields are added to Options, update
  // this function to catch changes that should not be allowed to
  // change in the middle of building a Table.
  if (options.comparator != rep_->options.comparator) {
    return Status::InvalidArgument("changing comparator while building table");
  }

  // Note that any live BlockBuilders point to rep_->options and therefore
  // will automatically pick up the updated options.
  rep_->options = options;
  rep_->index_block_options = options;
  rep_->index_block_options.block_restart_interval = 1;
  return Status::OK();
}
//TODO: make it create a block every blocksize, flush every 1M. When flushing do not poll completion
// pool the completion at the same time in the end
void TableBuilder::Add(const Slice& key, const Slice& value) {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->num_entries > 0) {
    assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);
  }
  //todo: MAKE IT a remote block size which could be 1M
  // How to predict the datasize before actually serilizae the data, so that there will
  // not be buffer overflow.
  // First, predict whether the block will be full
  // *   if so then finish the old data to a block make it insert to a new block
  // *   Second, if new block finished, check whether the write buffer can hold a new block size.
  // *           if not, the flush temporal buffer content to the remote memory.
  const size_t estimated_block_size = r->data_block->CurrentSizeEstimate();
  if (estimated_block_size + key.size() + value.size() +sizeof (uint32_t) + kBlockTrailerSize >= r->options.block_size) {
    UpdateFunctionBLock();
    if (r->local_data_mr[0]->length - (r->offset - r->offset_last_flushed) < r->options.block_size) {
      FlushData();
    }
  }

  //Create a new index entry but never flush it
  // when write a index entry, the data block offset and data block size will be attached
  if (r->pending_index_filter_entry) {
    assert(r->data_block->empty());
    r->options.comparator->FindShortestSeparator(&r->last_key, key);
    std::string handle_encoding;
    //Note that the handle block size does not contain CRC!
    r->pending_data_handle.EncodeTo(&handle_encoding);
    if (r->index_block->CurrentSizeEstimate()+ r->last_key.size() + handle_encoding.size() +
        sizeof (uint32_t) + kBlockTrailerSize > r->local_index_mr[0]->length){
      BlockHandle dummy_handle;
      size_t msg_size;
      FinishDataIndexBlock(r->index_block, &dummy_handle, r->options.compression, msg_size);
      FlushDataIndex(msg_size);
    }
    r->index_block->Add(r->last_key, Slice(handle_encoding));
    if (r->filter_block != nullptr) {
      if (r->filter_block->CurrentSizeEstimate() + kBlockTrailerSize > r->local_filter_mr[0]->length){
        // Tofix: Finish itself contain Reset and Flush, Modify the filter block make
        // it update the r->offset and add crc and make filter block compatible with Finish block.
        BlockHandle dummy_handle;
        size_t msg_size;
        FinishFilterBlock(r->filter_block, &dummy_handle, kNoCompression, msg_size);
        FlushFilter(msg_size);

      }

      r->filter_block->StartBlock(r->offset);


    }
    r->pending_index_filter_entry = false;
  }

  if (r->filter_block != nullptr) {
    r->filter_block->AddKey(key);
  }

  r->last_key.assign(key.data(), key.size());
  r->num_entries++;
  r->data_block->Add(key, value);




}

void TableBuilder::UpdateFunctionBLock() {

  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->data_block->empty()) return;
  assert(!r->pending_index_filter_entry);
  FinishDataBlock(r->data_block, &r->pending_data_handle, r->options.compression);
  //set data block pointer to next one, clear the block state
//  r->data_block->Reset();
  if (ok()) {
    r->pending_index_filter_entry = true;
//    r->status = r->file->FlushData();
  }

}
//Note: there are three types of finish function for different blocks, the main
//difference is whether update the offset which will record the size of the data block.
//And the filter blocks has a different way to reset the block.
void TableBuilder::FinishDataBlock(BlockBuilder* block, BlockHandle* handle,
                                   CompressionType compressiontype) {
  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    compressiontype: uint8
  //    crc: uint32
  assert(ok());
  Rep* r = rep_;
  block->Finish();

  Slice* raw = &(r->data_block->buffer);
  Slice* block_contents;
//  CompressionType compressiontype = r->options.compression;
  //TOTHINK: temporally disable the compression, because it can increase the latency but it could
  // increase the available bandwidth. THis part depends on whether the in-memory write can catch
  // up with the high RDMA bandwidth.
  switch (compressiontype) {
    case kNoCompression:
      block_contents = raw;
      break;

//    case kSnappyCompression: {
//      std::string* compressed = &r->compressed_output;
//      if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
//          compressed->size() < raw.size() - (raw.size() / 8u)) {
//        block_contents = *compressed;
//      } else {
//        // Snappy not supported, or compressed less than 12.5%, so just
//        // store uncompressed form
//        block_contents = raw;
//        compressiontype = kNoCompression;
//      }
//      break;
//    }
  }
  handle->set_offset(r->offset);
  handle->set_size(block_contents->size());
  if (r->status.ok()) {
    char trailer[kBlockTrailerSize];
    trailer[0] = compressiontype;
    uint32_t crc = crc32c::Value(block_contents->data(), block_contents->size());
    crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block compressiontype
    EncodeFixed32(trailer + 1, crc32c::Mask(crc));
    block_contents->append(trailer, kBlockTrailerSize);
    // block_type == 0 means data block
    if (r->status.ok()) {
      r->offset += block_contents->size();
      assert(r->offset - r->offset_last_flushed <= r->local_data_mr[0]->length);
    }
  }
  r->compressed_output.clear();
  block->Reset();
}
void TableBuilder::FinishDataIndexBlock(BlockBuilder* block,
                                        BlockHandle* handle,
                                        CompressionType compressiontype,
                                        size_t& block_size) {
  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    compressiontype: uint8
  //    crc: uint32
  assert(ok());
  Rep* r = rep_;
  block->Finish();

  Slice* raw = &(r->index_block->buffer);
  if (raw->size() == 19701){
    int i = 1;
  }
  Slice* block_contents;
  switch (compressiontype) {
    case kNoCompression:
      block_contents = raw;
      break;

//    case kSnappyCompression: {
//      std::string* compressed = &r->compressed_output;
//      if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
//          compressed->size() < raw.size() - (raw.size() / 8u)) {
//        block_contents = *compressed;
//      } else {
//        // Snappy not supported, or compressed less than 12.5%, so just
//        // store uncompressed form
//        block_contents = raw;
//        compressiontype = kNoCompression;
//      }
//      break;
//    }
  }
  handle->set_offset(r->offset);
  handle->set_size(block_contents->size());
  if (r->status.ok()) {
    char trailer[kBlockTrailerSize];
    trailer[0] = compressiontype;
    uint32_t crc = crc32c::Value(block_contents->data(), block_contents->size());
    crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block compressiontype
    EncodeFixed32(trailer + 1, crc32c::Mask(crc));
    block_contents->append(trailer, kBlockTrailerSize);
  }
  r->compressed_output.clear();
  block_size = block_contents->size();
  block->Reset();

}
void TableBuilder::FinishFilterBlock(FilterBlockBuilder* block, BlockHandle* handle,
                                     CompressionType compressiontype,
                                     size_t& block_size) {
  assert(ok());
  Rep* r = rep_;
  block->Finish();

  Slice* raw = &(r->index_block->buffer);
  Slice* block_contents;
  switch (compressiontype) {
    case kNoCompression:
      block_contents = raw;
      break;

//    case kSnappyCompression: {
//      std::string* compressed = &r->compressed_output;
//      if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
//          compressed->size() < raw.size() - (raw.size() / 8u)) {
//        block_contents = *compressed;
//      } else {
//        // Snappy not supported, or compressed less than 12.5%, so just
//        // store uncompressed form
//        block_contents = raw;
//        compressiontype = kNoCompression;
//      }
//      break;
//    }
  }
  handle->set_offset(r->offset);
  handle->set_size(block_contents->size());
  if (r->status.ok()) {
    char trailer[kBlockTrailerSize];
    trailer[0] = compressiontype;
    uint32_t crc = crc32c::Value(block_contents->data(), block_contents->size());
    crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block compressiontype
    EncodeFixed32(trailer + 1, crc32c::Mask(crc));
    block_contents->append(trailer, kBlockTrailerSize);
  }
  r->compressed_output.clear();
  block_size = block_contents->size();
//  block->Reset();
}
//TODO make flushing flush the data to the remote memory flushing to remote memory
void TableBuilder::FlushData(){
  Rep* r = rep_;
  size_t msg_size = r->offset - r->offset_last_flushed;
  ibv_mr* remote_mr;
  std::shared_ptr<RDMA_Manager> rdma_mg =  r->options.env->rdma_mg;
  rdma_mg->Allocate_Remote_RDMA_Slot(remote_mr);
  //TOTHINK: check the logic below.
  // I was thinking that the start represent the oldest outgoing buffer, while the
  // end is the latest buffer. When polling a result, the start index will moving forward
  // and the start index will be changed to 0 when meeting the end of this index.
  // If the start index meet the end index, then we need to allocate a new buffer,
  // and insert to the local buffer vector.
  // the end index will move forward because a new buffer will be in used, the start
  // index will also increased by 1 because the insert will cause all the index
  // after that position to be increased by 1.
  if (r->data_inuse_start == -1){
    // first time flush
    assert(r->data_inuse_end == -1 && r->local_data_mr.size() == 2);
    rdma_mg->RDMA_Write(remote_mr, r->local_data_mr[0], msg_size, "",IBV_SEND_SIGNALED, 0);
    r->data_inuse_end = 0;
    r->data_inuse_start = 0;
    r->data_inuse_empty = false;
  }else{
    auto* wc = new ibv_wc[5];
    int poll_num = 0;
    poll_num = rdma_mg->try_poll_this_thread_completions(wc, 5);
    // move the start index
    r->data_inuse_start += poll_num;
    if(r->data_inuse_start >= r->local_data_mr.size()){
      r->data_inuse_start = r->data_inuse_start - r->local_data_mr.size();
    }
//    if(r->data_inuse_start - r->data_inuse_end == 1 ||
//       r->data_inuse_end - r->data_inuse_start == r->local_data_mr.size()-1){
//      if (poll_num > 0){
//        //this means all the buffer is idle.
////       (r->data_inuse_start ==0 && r->data_inuse_end == r->local_data_mr.size()-1) )){
//        r->data_inuse_empty = true;
//      }else{
//        assert(poll_num == 0);
//        //this means all the buffer is in use, need allocate a new buffer
//      }
//    }
    //move forward the end of the outstanding buffer
    r->data_inuse_end = r->data_inuse_end == r->local_data_mr.size()-1 ? 0:r->data_inuse_end+1;
    rdma_mg->RDMA_Write(remote_mr, r->local_data_mr[r->data_inuse_end], msg_size, "",IBV_SEND_SIGNALED, 0);
    if (r->data_inuse_start - r->data_inuse_end == 1 ||
        r->data_inuse_end - r->data_inuse_start == r->local_data_mr.size()-1){
      ibv_mr* new_local_mr;
      rdma_mg->Allocate_Local_RDMA_Slot(new_local_mr,"FlushBuffer");
      // insert new mr at start while increase start by 1.
      r->local_data_mr.insert(r->local_data_mr.begin() + r->data_inuse_start++, new_local_mr);
    }
  }
  remote_mr->length = msg_size;
  if(r->remote_data_mrs.empty()){
    r->remote_data_mrs.insert({0, remote_mr});
  }else{
    r->remote_data_mrs.insert({r->remote_data_mrs.rbegin()->first+1, remote_mr});
  }

  r->offset_last_flushed = r->offset;
  // Move the datablock pointer to the start of the next write buffer, the other state of the data_block
  // has already reseted before
  int next_buffer_index = r->data_inuse_end == r->local_data_mr.size()-1 ? 0:r->data_inuse_end+1;

  assert(next_buffer_index != r->data_inuse_start);
  r->data_block->Move_buffer(const_cast<const char*>(static_cast<char*>(r->local_data_mr[next_buffer_index]->addr)));
  // No need to record the flushing times, because we can check from the remote mr map element number.
}
void TableBuilder::FlushDataIndex(size_t msg_size) {
  Rep* r = rep_;
  ibv_mr* remote_mr;
  std::shared_ptr<RDMA_Manager> rdma_mg =  r->options.env->rdma_mg;
  rdma_mg->Allocate_Remote_RDMA_Slot(remote_mr);
  rdma_mg->RDMA_Write(remote_mr, r->local_index_mr[0], msg_size, "",IBV_SEND_SIGNALED, 0);
  remote_mr->length = msg_size;
  if(r->remote_dataindex_mrs.empty()){
    r->remote_dataindex_mrs.insert({0, remote_mr});
  }else{
    r->remote_dataindex_mrs.insert({r->remote_dataindex_mrs.rbegin()->first+1, remote_mr});
  }
  //TOFIX: the index may overflow and need to create a new index write buffer, otherwise
  // it would be overwrited.
  r->index_block->Move_buffer(static_cast<char*>(r->local_index_mr[0]->addr));

}
void TableBuilder::FlushFilter(size_t& msg_size) {
  Rep* r = rep_;
  ibv_mr* remote_mr;
  std::shared_ptr<RDMA_Manager> rdma_mg =  r->options.env->rdma_mg;
  rdma_mg->Allocate_Remote_RDMA_Slot(remote_mr);
  rdma_mg->RDMA_Write(remote_mr, r->local_filter_mr[0], msg_size, "",IBV_SEND_SIGNALED, 0);
  remote_mr->length = msg_size;
  if(r->remote_filter_mrs.empty()){
    r->remote_filter_mrs.insert({0, remote_mr});
  }else{
    r->remote_filter_mrs.insert({r->remote_filter_mrs.rbegin()->first+1, remote_mr});
  }
  //TOFIX: the index may overflow and need to create a new index write buffer, otherwise
  // it would be overwrited.
  r->filter_block->Move_buffer(static_cast<char*>(r->local_filter_mr[0]->addr));

}
void TableBuilder::WriteRawBlock(const Slice& block_contents,
                                 CompressionType type, BlockHandle* handle) {
  Rep* r = rep_;

}

Status TableBuilder::status() const { return rep_->status; }

Status TableBuilder::Finish() {
  Rep* r = rep_;
  UpdateFunctionBLock();
  FlushData();
  assert(!r->closed);
  r->closed = true;

  BlockHandle filter_block_handle, metaindex_block_handle, index_block_handle;

  // Write filter block
  //TODO:
  // i found that it could have many bloom filters for each data block but there will be only one filter block.
  // the result will append many bloom filters.
  //TOthink why not compress the block here.
  if (ok() && r->filter_block != nullptr) {
//    r->filter_block->Finish();
    assert(r->pending_index_filter_entry);
    r->filter_block->StartBlock(r->offset);
    size_t msg_size;
    FinishFilterBlock(r->filter_block, &filter_block_handle, kNoCompression, msg_size);
    FlushFilter(msg_size);
  }


  // Write metaindex block
//  if (ok()) {
//    BlockBuilder meta_index_block(&r->options, nullptr);
//    if (r->filter_block != nullptr) {
//      // Add mapping from "filter.Name" to location of filter data
//      std::string key = "filter.";
//      key.append(r->options.filter_policy->Name());
//      std::string handle_encoding;
//      filter_block_handle.EncodeTo(&handle_encoding);
//      meta_index_block->Add(key, handle_encoding);
//    }
//
//    // TODO(postrelease): Add stats and other meta blocks
//    FinishDataBlock(&meta_index_block, &metaindex_block_handle);
//  }

  // Write index block
  if (ok()) {
    assert(r->pending_index_filter_entry);
    {
      r->options.comparator->FindShortSuccessor(&r->last_key);
      std::string handle_encoding;
      r->pending_data_handle.EncodeTo(&handle_encoding);
      r->index_block->Add(r->last_key, Slice(handle_encoding));
      r->pending_index_filter_entry = false;
    }
    size_t msg_size;
    FinishDataIndexBlock(r->index_block, &index_block_handle,
                    r->options.compression, msg_size);
    FlushDataIndex(msg_size);
  }
  DEBUG_arg("for a sst the remote data chunks number %zu", r->local_data_mr.size());
  int num_of_poll = r->data_inuse_end - r->data_inuse_start + 1 >= 0 ?
                    r->data_inuse_end - r->data_inuse_start + 1:
                    (int)(r->local_data_mr.size()) - r->data_inuse_start + r->data_inuse_end +1;
  num_of_poll = num_of_poll + r->local_filter_mr.size() + r->local_index_mr.size();
  ibv_wc wc[num_of_poll];
  r->options.env->rdma_mg->poll_completion(wc, num_of_poll, "");
  usleep(10);
  int check_poll_number = r->options.env->rdma_mg->try_poll_this_thread_completions(wc,5);
  assert( check_poll_number == 0);
//  printf("A table finsihed flushing\n");
//  // Write footer
//  if (ok()) {
//    Footer footer;
//    footer.set_metaindex_handle(metaindex_block_handle);
//    footer.set_index_handle(index_block_handle);
//    std::string footer_encoding;
//    footer.EncodeTo(&footer_encoding);
//    r->status = r->file->Append(footer_encoding);
//    if (r->status.ok()) {
//      r->offset += footer_encoding.size();
//    }
//  }
  return r->status;
}

void TableBuilder::Abandon() {
  Rep* r = rep_;
  assert(!r->closed);
  r->closed = true;
}

uint64_t TableBuilder::NumEntries() const { return rep_->num_entries; }

uint64_t TableBuilder::FileSize() const { return rep_->offset; }
void TableBuilder::get_datablocks_map(std::map<int, ibv_mr*>& map) {
  map = rep_->remote_data_mrs;
}
void TableBuilder::get_dataindexblocks_map(std::map<int, ibv_mr*>& map) {
  map = rep_->remote_dataindex_mrs;
}
void TableBuilder::get_filter_map(std::map<int, ibv_mr*>& map) {
  map = rep_->remote_filter_mrs;
}

}  // namespace leveldb
