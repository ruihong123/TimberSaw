// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_VERSION_EDIT_H_
#define STORAGE_LEVELDB_DB_VERSION_EDIT_H_

#include <set>
#include <utility>
#include <vector>

#include "db/dbformat.h"
#include "util/rdma.h"
namespace leveldb {

class VersionSet;
class RDMA_Manager;
//TODO; Make a new data structure for remote SST with no file name, just remote chunks
// Solved
struct RemoteMemTableMetaData {
  RemoteMemTableMetaData() : refs(0), allowed_seeks(1 << 30) {}
  //TOTHINK: the garbage collection of the Remmote table is not triggered!
  ~RemoteMemTableMetaData() {
    DEBUG("Destroying RemoteMemtableMetaData");
    if(Remote_blocks_deallocate(remote_data_mrs) &&
        Remote_blocks_deallocate(remote_dataindex_mrs) &&
    Remote_blocks_deallocate(remote_filter_mrs)){
      DEBUG("Remote blocks deleted successfully\n");
    }
  }
  bool Remote_blocks_deallocate(std::map<int, ibv_mr*> map){
    std::map<int, ibv_mr*>::iterator it;

    for (it = map.begin(); it != map.end(); it++){
      if(!rdma_mg->Deallocate_Local_RDMA_Slot(it->second->addr, "FlushBuffer")){
        return false;
      }
    }
    return true;
  }
  static std::shared_ptr<RDMA_Manager> rdma_mg;
  int refs;
  int allowed_seeks;  // Seeks allowed until compaction
  uint64_t number;
  std::map<int, ibv_mr*> remote_data_mrs;
  std::map<int, ibv_mr*> remote_dataindex_mrs;
  std::map<int, ibv_mr*> remote_filter_mrs;
  //std::vector<ibv_mr*> remote_data_mrs
  uint64_t file_size;    // File size in bytes
  InternalKey smallest;  // Smallest internal key served by table
  InternalKey largest;   // Largest internal key served by table
};

class VersionEdit {
 public:
  VersionEdit() { Clear(); }
  ~VersionEdit() = default;

  void Clear();

  void SetComparatorName(const Slice& name) {
    has_comparator_ = true;
    comparator_ = name.ToString();
  }
  void SetLogNumber(uint64_t num) {
    has_log_number_ = true;
    log_number_ = num;
  }
  void SetPrevLogNumber(uint64_t num) {
    has_prev_log_number_ = true;
    prev_log_number_ = num;
  }
  void SetNextFile(uint64_t num) {
    has_next_file_number_ = true;
    next_file_number_ = num;
  }
  void SetLastSequence(SequenceNumber seq) {
    has_last_sequence_ = true;
    last_sequence_ = seq;
  }
  void SetCompactPointer(int level, const InternalKey& key) {
    compact_pointers_.push_back(std::make_pair(level, key));
  }

  // Add the specified file at the specified number.
  // REQUIRES: This version has not been saved (see VersionSet::SaveTo)
  // REQUIRES: "smallest" and "largest" are smallest and largest keys in file
  void AddFile(int level,
               std::shared_ptr<RemoteMemTableMetaData> remote_table) {
    new_files_.push_back(std::make_pair(level, remote_table));
  }

  // Delete the specified "file" from the specified "level".
  void RemoveFile(int level, uint64_t file) {
    deleted_files_.insert(std::make_pair(level, file));
  }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(const Slice& src);

  std::string DebugString() const;

 private:
  friend class VersionSet;

  typedef std::set<std::pair<int, uint64_t>> DeletedFileSet;

  std::string comparator_;
  uint64_t log_number_;
  uint64_t prev_log_number_;
  uint64_t next_file_number_;
  SequenceNumber last_sequence_;
  bool has_comparator_;
  bool has_log_number_;
  bool has_prev_log_number_;
  bool has_next_file_number_;
  bool has_last_sequence_;

  std::vector<std::pair<int, InternalKey>> compact_pointers_;
  DeletedFileSet deleted_files_;
  std::vector<std::pair<int, std::shared_ptr<RemoteMemTableMetaData>>> new_files_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VERSION_EDIT_H_
