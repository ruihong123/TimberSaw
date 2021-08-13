// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_edit.h"

#include "db/version_set.h"
#include "util/coding.h"
#include "memory_node/memory_node_keeper.h"
#include "leveldb/env.h"
namespace leveldb {
//std::shared_ptr<RDMA_Manager> RemoteMemTableMetaData::rdma_mg = Env::Default()->rdma_mg;
RemoteMemTableMetaData::RemoteMemTableMetaData()  : table_type(0), allowed_seeks(1 << 30) {
  rdma_mg = Env::Default()->rdma_mg;
}
RemoteMemTableMetaData::RemoteMemTableMetaData(int type)
    : table_type(type), allowed_seeks(1 << 30) {
  rdma_mg = Memory_Node_Keeper::rdma_mg;
}
void RemoteMemTableMetaData::EncodeTo(std::string* dst) const {
  PutFixed64(dst, level);
  PutFixed64(dst, number);
  PutFixed64(dst, file_size);
  PutLengthPrefixedSlice(dst, smallest.Encode());
  PutLengthPrefixedSlice(dst, largest.Encode());
  uint64_t remote_data_chunk_num = remote_data_mrs.size();
  uint64_t remote_dataindex_chunk_num = remote_dataindex_mrs.size();
  uint64_t remote_filter_chunk_num = remote_filter_mrs.size();
  PutFixed64(dst, remote_data_chunk_num);
  PutFixed64(dst, remote_dataindex_chunk_num);
  PutFixed64(dst, remote_filter_chunk_num);
  //Here we suppose all the remote memory chuck for the same table have similar infomation  below,
  // the only difference is the addr and lenght
  PutFixed64(dst, (uint64_t)remote_data_mrs.begin()->second->context);
  PutFixed64(dst, (uint64_t)remote_data_mrs.begin()->second->pd);
  PutFixed32(dst, (uint64_t)remote_data_mrs.begin()->second->handle);
  PutFixed32(dst, (uint64_t)remote_data_mrs.begin()->second->lkey);
  PutFixed32(dst, (uint64_t)remote_data_mrs.begin()->second->rkey);
  for(auto iter : remote_data_mrs){
    PutFixed32(dst, iter.first);
    mr_serialization(dst,iter.second);
  }
  for(auto iter : remote_dataindex_mrs){
    PutFixed32(dst, iter.first);
    mr_serialization(dst,iter.second);
  }
  for(auto iter : remote_filter_mrs){
    PutFixed32(dst, iter.first);
    mr_serialization(dst,iter.second);
  }
//  size_t
}
Status RemoteMemTableMetaData::DecodeFrom(Slice& src) {
  Status s = Status::OK();
  rdma_mg = Memory_Node_Keeper::rdma_mg;
  GetFixed64(&src, &level);
  GetFixed64(&src, &number);
  GetFixed64(&src, &file_size);
  Slice temp;
  GetLengthPrefixedSlice(&src, &temp);
  smallest.DecodeFrom(temp);
  GetLengthPrefixedSlice(&src, &temp);
  largest.DecodeFrom(temp);
  uint64_t remote_data_chunk_num;
  uint64_t remote_dataindex_chunk_num;
  uint64_t remote_filter_chunk_num;
  GetFixed64(&src, &remote_data_chunk_num);
  GetFixed64(&src, &remote_dataindex_chunk_num);
  GetFixed64(&src, &remote_filter_chunk_num);
  uint64_t context_temp;
  uint64_t pd_temp;
  uint32_t handle_temp;
  uint32_t lkey_temp;
  uint32_t rkey_temp;
  GetFixed64(&src, &context_temp);
  GetFixed64(&src, &pd_temp);
  GetFixed32(&src, &handle_temp);
  GetFixed32(&src, &lkey_temp);
  GetFixed32(&src, &rkey_temp);
  for(auto i = 0; i< remote_data_chunk_num; i++){
    //Todo: check whether the reinterpret_cast here make the correct value.
    ibv_mr* mr = new ibv_mr{context: reinterpret_cast<ibv_context*>(context_temp),
                            pd: reinterpret_cast<ibv_pd*>(pd_temp), addr: nullptr,
                            length: 0,
                            handle:handle_temp, lkey:lkey_temp, rkey:rkey_temp};
    uint32_t offset = 0;
    GetFixed32(&src, &offset);
    GetFixed64(&src, reinterpret_cast<uint64_t*>(&mr->addr));
    GetFixed64(&src, &mr->length);
    remote_data_mrs.insert({offset, mr});
  }
  for(auto i = 0; i< remote_dataindex_chunk_num; i++){
    //Todo: check whether the reinterpret_cast here make the correct value.
    ibv_mr* mr = new ibv_mr{context: reinterpret_cast<ibv_context*>(context_temp),
                            pd: reinterpret_cast<ibv_pd*>(pd_temp), addr: nullptr,
                            length: 0,
                            handle:handle_temp, lkey:lkey_temp, rkey:rkey_temp};
    uint32_t offset = 0;
    GetFixed32(&src, &offset);
    GetFixed64(&src, reinterpret_cast<uint64_t*>(&mr->addr));
    GetFixed64(&src, &mr->length);
    remote_dataindex_mrs.insert({offset, mr});
  }
  assert(!remote_dataindex_mrs.empty());
  for(auto i = 0; i< remote_filter_chunk_num; i++){
    //Todo: check whether the reinterpret_cast here make the correct value.
    ibv_mr* mr = new ibv_mr{context: reinterpret_cast<ibv_context*>(context_temp),
                            pd: reinterpret_cast<ibv_pd*>(pd_temp), addr: nullptr,
                            length: 0,
                            handle:handle_temp, lkey:lkey_temp, rkey:rkey_temp};
    uint32_t offset = 0;
    GetFixed32(&src, &offset);
    GetFixed64(&src, reinterpret_cast<uint64_t*>(&mr->addr));
    GetFixed64(&src, &mr->length);
    remote_filter_mrs.insert({offset, mr});
  }
  assert(!remote_filter_mrs.empty());
  return s;
}
void RemoteMemTableMetaData::mr_serialization(std::string* dst, ibv_mr* mr) const {
//  PutFixed64(dst, (uint64_t)mr->context);
//  PutFixed64(dst, (uint64_t)mr->pd);
  PutFixed64(dst, (uint64_t)mr->addr);
  PutFixed64(dst, mr->length);
//  PutFixed32(dst, mr->handle);
//  PutFixed32(dst, mr->lkey);
//  PutFixed32(dst, mr->rkey);

}

// Tag numbers for serialized VersionEdit.  These numbers are written to
// disk and should not be changed.
enum Tag {
  kComparator = 1,
  kLogNumber = 2,
  kNextFileNumber = 3,
  kLastSequence = 4,
  kCompactPointer = 5,
  kDeletedFile = 6,
  kNewFile = 7,
  // 8 was used for large value refs
  kPrevLogNumber = 9
};

void VersionEdit::Clear() {
  comparator_.clear();
  log_number_ = 0;
  prev_log_number_ = 0;
  last_sequence_ = 0;
  next_file_number_ = 0;
  has_comparator_ = false;
  has_log_number_ = false;
  has_prev_log_number_ = false;
  has_next_file_number_ = false;
  has_last_sequence_ = false;
  deleted_files_.clear();
  new_files_.clear();
}

void VersionEdit::EncodeTo(std::string* dst) const {
  if (has_comparator_) {
    PutVarint32(dst, kComparator);
    PutLengthPrefixedSlice(dst, comparator_);
  }
  if (has_log_number_) {
    PutVarint32(dst, kLogNumber);
    PutVarint64(dst, log_number_);
  }
  if (has_prev_log_number_) {
    PutVarint32(dst, kPrevLogNumber);
    PutVarint64(dst, prev_log_number_);
  }
  if (has_next_file_number_) {
    PutVarint32(dst, kNextFileNumber);
    PutVarint64(dst, next_file_number_);
  }
  if (has_last_sequence_) {
    PutVarint32(dst, kLastSequence);
    PutVarint64(dst, last_sequence_);
  }

  for (size_t i = 0; i < compact_pointers_.size(); i++) {
    PutVarint32(dst, kCompactPointer);
    PutVarint32(dst, compact_pointers_[i].first);  // level
    PutLengthPrefixedSlice(dst, compact_pointers_[i].second.Encode());
  }

  for (const auto& deleted_file_kvp : deleted_files_) {
    PutVarint32(dst, kDeletedFile);
    PutVarint32(dst, deleted_file_kvp.first);   // level
    PutVarint64(dst, deleted_file_kvp.second);  // file number
  }

  for (size_t i = 0; i < new_files_.size(); i++) {
    const std::shared_ptr<RemoteMemTableMetaData> f = new_files_[i].second;
    PutVarint32(dst, kNewFile);
    PutVarint32(dst, new_files_[i].first);  // level
    f->EncodeTo(dst);

  }
}

static bool GetInternalKey(Slice* input, InternalKey* dst) {
  Slice str;
  if (GetLengthPrefixedSlice(input, &str)) {
    return dst->DecodeFrom(str);
  } else {
    return false;
  }
}

static bool GetLevel(Slice* input, int* level) {
  uint32_t v;
  if (GetVarint32(input, &v) && v < config::kNumLevels) {
    *level = v;
    return true;
  } else {
    return false;
  }
}

Status VersionEdit::DecodeFrom(const Slice& src, int sstable_type,
                               std::shared_ptr<RDMA_Manager> rdma) {
  Clear();
  Slice input = src;
  const char* msg = nullptr;
  uint32_t tag;

  // Temporary storage for parsing
  int level;
  uint64_t number;
  Slice str;
  InternalKey key;

  while (msg == nullptr && GetVarint32(&input, &tag)) {
    switch (tag) {
      case kComparator:
        if (GetLengthPrefixedSlice(&input, &str)) {
          comparator_ = str.ToString();
          has_comparator_ = true;
        } else {
          msg = "comparator name";
        }
        break;

      case kLogNumber:
        if (GetVarint64(&input, &log_number_)) {
          has_log_number_ = true;
        } else {
          msg = "log number";
        }
        break;

      case kPrevLogNumber:
        if (GetVarint64(&input, &prev_log_number_)) {
          has_prev_log_number_ = true;
        } else {
          msg = "previous log number";
        }
        break;

      case kNextFileNumber:
        if (GetVarint64(&input, &next_file_number_)) {
          has_next_file_number_ = true;
        } else {
          msg = "next file number";
        }
        break;

      case kLastSequence:
        if (GetVarint64(&input, &last_sequence_)) {
          has_last_sequence_ = true;
        } else {
          msg = "last sequence number";
        }
        break;

      case kCompactPointer:
        if (GetLevel(&input, &level) && GetInternalKey(&input, &key)) {
          compact_pointers_.push_back(std::make_pair(level, key));
        } else {
          msg = "compaction pointer";
        }
        break;

      case kDeletedFile:
        if (GetLevel(&input, &level) && GetVarint64(&input, &number)) {
          deleted_files_.insert(std::make_pair(level, number));
        } else {
          msg = "deleted file";
        }
        break;

      case kNewFile:
        if (GetLevel(&input, &level)) {
          std::shared_ptr<RemoteMemTableMetaData> f = std::make_shared<RemoteMemTableMetaData>(sstable_type);
          f->DecodeFrom(input);
          assert(level == 0);
          new_files_.push_back(std::make_pair(level, f));
        } else {
          msg = "new-file entry";
        }
        break;

      default:
        msg = "unknown tag";
        break;
    }
  }

  if (msg == nullptr && !input.empty()) {
    msg = "invalid tag";
  }

  Status result;
  if (msg != nullptr) {
    result = Status::Corruption("VersionEdit", msg);
  }
  return result;
}

std::string VersionEdit::DebugString() const {
  std::string r;
  r.append("VersionEdit {");
  if (has_comparator_) {
    r.append("\n  Comparator: ");
    r.append(comparator_);
  }
  if (has_log_number_) {
    r.append("\n  LogNumber: ");
    AppendNumberTo(&r, log_number_);
  }
  if (has_prev_log_number_) {
    r.append("\n  PrevLogNumber: ");
    AppendNumberTo(&r, prev_log_number_);
  }
  if (has_next_file_number_) {
    r.append("\n  NextFile: ");
    AppendNumberTo(&r, next_file_number_);
  }
  if (has_last_sequence_) {
    r.append("\n  LastSeq: ");
    AppendNumberTo(&r, last_sequence_);
  }
  for (size_t i = 0; i < compact_pointers_.size(); i++) {
    r.append("\n  CompactPointer: ");
    AppendNumberTo(&r, compact_pointers_[i].first);
    r.append(" ");
    r.append(compact_pointers_[i].second.DebugString());
  }
  for (const auto& deleted_files_kvp : deleted_files_) {
    r.append("\n  RemoveFile: ");
    AppendNumberTo(&r, deleted_files_kvp.first);
    r.append(" ");
    AppendNumberTo(&r, deleted_files_kvp.second);
  }
  for (size_t i = 0; i < new_files_.size(); i++) {
    const std::shared_ptr<RemoteMemTableMetaData> f = new_files_[i].second;
    r.append("\n  AddFile: ");
    AppendNumberTo(&r, new_files_[i].first);
    r.append(" ");
    AppendNumberTo(&r, f->number);
    r.append(" ");
    AppendNumberTo(&r, f->file_size);
    r.append(" ");
    r.append(f->smallest.DebugString());
    r.append(" .. ");
    r.append(f->largest.DebugString());
  }
  r.append("\n}\n");
  return r;
}

}  // namespace leveldb
