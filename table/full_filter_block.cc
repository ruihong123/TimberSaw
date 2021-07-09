// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/full_filter_block.h"

#include "leveldb/filter_policy.h"
#include "util/coding.h"

namespace leveldb {

// See doc/table_format.md for an explanation of the filter block format.


FullFilterBlockBuilder::FullFilterBlockBuilder(const FilterPolicy* policy,
                                       std::vector<ibv_mr*>* mrs,
                                       std::map<int, ibv_mr*>* remote_mrs,
                                       std::shared_ptr<RDMA_Manager> rdma_mg,
                                       std::string& type_string)
    : policy_(policy), rdma_mg_(rdma_mg),
      local_mrs(mrs), remote_mrs_(remote_mrs), type_string_(type_string),
      result((char*)(*mrs)[0]->addr, 0) {}

//TOTHINK: One block per bloom filter, then why there is a design for the while loop?
// Is it a bad design?
// Answer: Every bloomfilter corresponding to one block, but the filter offsets
// was set every 2KB. for the starting KV of a data block which lies in the same 2KB
// chunk of the last block, it was wronglly assigned to the last bloom filter
void FullFilterBlockBuilder::RestartBlock(uint64_t block_offset) {
//  uint64_t filter_index = (block_offset / kFilterBase);
    GenerateFilter();
}
size_t FullFilterBlockBuilder::CurrentSizeEstimate() {
  //result plus filter offsets plus array offset plus 1 char for kFilterBaseLg
  return (result.size() + filter_offsets_.size()*4 +5);
}
void FullFilterBlockBuilder::AddKey(const Slice& key) {
  Slice k = key;
  start_.push_back(keys_.size());
  keys_.append(k.data(), k.size());
}

Slice FullFilterBlockBuilder::Finish() {
  if (!start_.empty()) {
    GenerateFilter();
  }

  // Append array of per-filter offsets
//  const uint32_t array_offset = result.size();
//  for (size_t i = 0; i < filter_offsets_.size(); i++) {
//    PutFixed32(&result, filter_offsets_[i]);
//  }
//
//  PutFixed32(&result, array_offset);
//  char length_perfilter = static_cast<char>(kFilterBaseLg);
//  result.append(&length_perfilter,1);  // Save encoding parameter in result
//  Flush();
  //TOFIX: Here could be some other data structures not been cleared.


  return Slice(result);
}
void FullFilterBlockBuilder::Reset() {
  result.Reset(static_cast<char*>((*local_mrs)[0]->addr),0);
}
void FullFilterBlockBuilder::Move_buffer(const char* p){
  result.Reset(p,0);
}
void FullFilterBlockBuilder::Flush() {
  ibv_mr* remote_mr;
  size_t msg_size = result.size();
  rdma_mg_->Allocate_Remote_RDMA_Slot(remote_mr);
  rdma_mg_->RDMA_Write(remote_mr, (*local_mrs)[0], msg_size, type_string_,IBV_SEND_SIGNALED, 0);
  remote_mr->length = msg_size;
  if(remote_mrs_->empty()){
    remote_mrs_->insert({0, remote_mr});
  }else{
    remote_mrs_->insert({remote_mrs_->rbegin()->first+1, remote_mr});
  }
}
void FullFilterBlockBuilder::GenerateFilter() {
  const size_t num_keys = start_.size();
  if (num_keys == 0) {
    // Fast path if there are no keys for this filter
    filter_offsets_.push_back(result.size());
    return;
  }

  // Make list of keys from flattened key structure
  start_.push_back(keys_.size());  // Simplify length computation
  tmp_keys_.resize(num_keys);
  for (size_t i = 0; i < num_keys; i++) {
    char* base = keys_.data() + start_[i];
    size_t length = start_[i + 1] - start_[i];
    tmp_keys_[i] = Slice(base, length);
  }

  // Generate filter for current set of keys and append to result.
  filter_offsets_.push_back(result.size());
  policy_->CreateFilter(&tmp_keys_[0], static_cast<int>(num_keys), &result);
  tmp_keys_.clear();
  keys_.clear();
  start_.clear();
}

FullFilterBlockReader::FullFilterBlockReader(const FilterPolicy* policy,
                                     const Slice& contents,
                                     std::shared_ptr<RDMA_Manager> rdma_mg)
    : policy_(policy), filter_content(contents), rdma_mg_(rdma_mg) {


}
//bool FullFilterBlockReader::KeyMayMatch(uint64_t block_offset, const Slice& key) {
//  uint64_t index = block_offset >> base_lg_;
//  if (index < num_) {
//    uint32_t start = DecodeFixed32(offset_ + index * 4);
//    uint32_t limit = DecodeFixed32(offset_ + index * 4 + 4);
//    if (start <= limit && limit <= static_cast<size_t>(offset_ - data_)) {
//      Slice filter = Slice(data_ + start, limit - start);
//      return policy_->KeyMayMatch(key, filter);
//    } else if (start == limit) {
//      // Empty filters do not match any keys
//      return false;
//    }
//  }
//  return true;  // Errors are treated as potential matches
//}
bool FullFilterBlockReader::KeyMayMatch(const Slice& key) {

  return policy_->KeyMayMatch(key, filter_content);

}
FullFilterBlockReader::~FullFilterBlockReader() {
  if (!rdma_mg_->Deallocate_Local_RDMA_Slot((void*)filter_content.data(), "FilterBlock")){
    printf("Filter Block deregisteration failed\n");
  }else{
    printf("Filter block deregisteration successfully\n");
  }
}

}  // namespace leveldb
