//
// Created by ruihong on 7/9/21.
//

#ifndef LEVELDB_FULL_FILTER_BLOCK_H
#define LEVELDB_FULL_FILTER_BLOCK_H
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "leveldb/slice.h"
#include "leveldb/options.h"
#include "util/hash.h"

#include "block_builder.h"

namespace leveldb {

class FilterPolicy;
class Env;
class Options;
// A FullFilterBlockBuilder is used to construct all of the filters for a
// particular Table.  It generates a single string which is stored as
// a special block in the Table.
//
// The sequence of calls to FullFilterBlockBuilder must match the regexp:
//      (StartBlock AddKey*)* Finish
class FullFilterBlockBuilder {
 public:
  explicit FullFilterBlockBuilder(const FilterPolicy* policy,
                              std::vector<ibv_mr*>* mrs,
                              std::map<int, ibv_mr*>* remote_mrs,
                              std::shared_ptr<RDMA_Manager> rdma_mg,
                              std::string& type_string);

  FullFilterBlockBuilder(const FullFilterBlockBuilder&) = delete;
  FullFilterBlockBuilder& operator=(const FullFilterBlockBuilder&) = delete;

  void RestartBlock(uint64_t block_offset);
  size_t CurrentSizeEstimate();
  void AddKey(const Slice& key);
  Slice Finish();
  void Reset();
  void Flush();
  void Move_buffer(const char* p);
  Slice result;           // Filter data computed so far
 private:
  void GenerateFilter();

  const FilterPolicy* policy_;
  std::shared_ptr<RDMA_Manager> rdma_mg_;
  std::vector<ibv_mr*>* local_mrs;
  std::map<int, ibv_mr*>* remote_mrs_;
  std::string keys_;             // Flattened key contents
  std::vector<size_t> start_;    // Starting index in keys_ of each key
  //todo Make result Slice; make Policy->CreateFilter accept Slice rather than string
  std::vector<Slice> tmp_keys_;  // policy_->CreateFilter() argument
  std::vector<uint32_t> filter_offsets_; // The filters' offset within the filter block.
  std::string type_string_;


};

class FullFilterBlockReader {
 public:
  // REQUIRES: "contents" and *policy must stay live while *this is live.
  FullFilterBlockReader(const FilterPolicy* policy, const Slice& contents,
                    std::shared_ptr<RDMA_Manager> rdma_mg);
  ~FullFilterBlockReader();
  bool KeyMayMatch(const Slice& key); // full filter.
 private:
  const FilterPolicy* policy_;
  Slice filter_content;
//  const char* data_;    // Pointer to filter data (at block-start)
  size_t filter_size;
  std::shared_ptr<RDMA_Manager> rdma_mg_;
};

}  // namespace leveldb
#endif  // LEVELDB_FULL_FILTER_BLOCK_H
