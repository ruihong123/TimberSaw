// Copyright (c) 2011 The TimberSaw Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_TimberSaw_DB_VERSION_EDIT_H_
#define STORAGE_TimberSaw_DB_VERSION_EDIT_H_

#include <set>
#include <utility>
#include <vector>

#include "db/dbformat.h"
#include "util/rdma.h"

namespace TimberSaw {

class VersionSet;
class RDMA_Manager;
//TODO; Make a new data structure for remote SST with no file name, just remote chunks
// Solved
struct RemoteMemTableMetaData {
//  RemoteMemTableMetaData();
// this_machine_type 0 means compute node, 1 means memory node
// creater_node_id  0 means compute node, 1 means memory node
  RemoteMemTableMetaData(int side);
  //TOTHINK: the garbage collection of the Remmote table is not triggered!
  ~RemoteMemTableMetaData() {
    //TODO and Tothink: when destroy this metadata check whether this is compute node, if yes, send a message to
    // home node to deference. Or the remote dereference is conducted in the granularity of version.
#ifndef NDEBUG
    printf("Destroying RemoteMemtableMetaData, Table number is %lu, creator node id is %d \n", number, creator_node_id);
#endif
    assert(this_machine_type ==0 || this_machine_type == 1);
    assert(creator_node_id == 0 || creator_node_id == 1);
    if (this_machine_type == 0 && creator_node_id == 0){
      if(Remote_blocks_deallocate(remote_data_mrs) &&
      Remote_blocks_deallocate(remote_dataindex_mrs) &&
      Remote_blocks_deallocate(remote_filter_mrs)){
        DEBUG("Remote blocks deleted successfully\n");
      }else{
        DEBUG("Remote memory collection not found\n");
        assert(false);
      }
    }else if(this_machine_type == 1 && creator_node_id == 1){
      //TODO: memory collection for the remote memory.
      if(Local_blocks_deallocate(remote_data_mrs) &&
      Local_blocks_deallocate(remote_dataindex_mrs) &&
      Local_blocks_deallocate(remote_filter_mrs)){
        DEBUG("Local blocks deleted successfully\n");
      }else{
        DEBUG("Local memory collection not found\n");
        assert(false);
      }
    }

  }
  bool Remote_blocks_deallocate(std::map<uint32_t , ibv_mr*> map){
    std::map<uint32_t , ibv_mr*>::iterator it;

    for (it = map.begin(); it != map.end(); it++){
      if(!rdma_mg->Deallocate_Remote_RDMA_Slot(it->second->addr)){
        return false;
      }
      delete it->second;
    }
    return true;
  }
  bool Local_blocks_deallocate(std::map<uint32_t , ibv_mr*> map){
    std::map<uint32_t , ibv_mr*>::iterator it;

    for (it = map.begin(); it != map.end(); it++){
      if(!rdma_mg->Deallocate_Local_RDMA_Slot(it->second->addr, "FlushBuffer")){
        return false;
      }
      delete it->second;
    }
    return true;
  }
  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice& src);
  void mr_serialization(std::string* dst, ibv_mr* mr) const;
  std::shared_ptr<RDMA_Manager> rdma_mg;
  int this_machine_type;
//  uint64_t refs;
  uint64_t level;
  uint64_t allowed_seeks;  // Seeks allowed until compaction
  uint64_t number;
  uint8_t creator_node_id;// The node id who create this SSTable.
  std::map<uint32_t , ibv_mr*> remote_data_mrs;
  std::map<uint32_t, ibv_mr*> remote_dataindex_mrs;
  std::map<uint32_t, ibv_mr*> remote_filter_mrs;
  //std::vector<ibv_mr*> remote_data_mrs
  uint64_t file_size;    // File size in bytes
  size_t num_entries;
  InternalKey smallest;  // Smallest internal key served by table
  InternalKey largest;   // Largest internal key served by table
  bool UnderCompaction = false;
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
  bool IsTrival(){
    return deleted_files_.size() == 1;
  }
  void GetTrivalFile(int& level, uint64_t& file_number, uint8_t& node_id){
    level = std::get<0>(*deleted_files_.begin());
    file_number = std::get<1>(*deleted_files_.begin());
    node_id = std::get<2>(*deleted_files_.begin());
  }
  void SetCompactPointer(int level, const InternalKey& key) {
    compact_pointers_.push_back(std::make_pair(level, key));
  }

  // Add the specified file at the specified number.
  // REQUIRES: This version has not been saved (see VersionSet::SaveTo)
  // REQUIRES: "smallest" and "largest" are smallest and largest keys in file
  void AddFile(int level,
               const std::shared_ptr<RemoteMemTableMetaData>& remote_table) {
    new_files_.emplace_back(level, remote_table);
  }
  void AddFileIfNotExist(int level,
               const std::shared_ptr<RemoteMemTableMetaData>& remote_table) {
    for(auto iter : new_files_){
      if (iter.second == remote_table){
        return;
      }
    }
    if (remote_table->file_size >0)
      new_files_.emplace_back(level, remote_table);
    return;
  }
  // Delete the specified "file" from the specified "level".
  void RemoveFile(int level, uint64_t file, uint8_t node_id) {
    //TODO(ruihong): remove this.
    assert(node_id < 2);
    deleted_files_.insert(std::make_tuple(level, file, node_id));
  }
  size_t GetNewFilesNum(){
    return new_files_.size();
  }
  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(const Slice& src, int sstable_type);
  void EncodeToDiskFormat(std::string* dst) const;
  Status DecodeFromDiskFormat(const Slice& src, int sstable_type);
  std::string DebugString() const;
  int compactlevel(){
    return std::get<0>(*deleted_files_.begin());
  }
 private:
  friend class VersionSet;

  typedef std::set<std::tuple<int, uint64_t, uint8_t>> DeletedFileSet;

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

}  // namespace TimberSaw

#endif  // STORAGE_TimberSaw_DB_VERSION_EDIT_H_
