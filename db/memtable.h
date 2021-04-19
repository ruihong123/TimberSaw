// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_MEMTABLE_H_
#define STORAGE_LEVELDB_DB_MEMTABLE_H_
#define MEMTABLE_SEQ_SIZE 10000
#include "db/dbformat.h"
#include "db/inlineskiplist.h"
#include <string>

#include "leveldb/db.h"

//#include "util/arena_old.h"

namespace leveldb {

class InternalKeyComparator;
class MemTableIterator;

class MemTable {
 public:
  enum FlushStateEnum { FLUSH_NOT_REQUESTED, FLUSH_REQUESTED, FLUSH_SCHEDULED };
  // MemTables are reference counted.  The initial reference count
  // is zero and the caller must call Ref() at least once.
  std::atomic<bool> able_to_flush = false;
  explicit MemTable(const InternalKeyComparator& comparator);
  MemTable(const MemTable&) = delete;
  MemTable& operator=(const MemTable&) = delete;
  ~MemTable();
  struct KeyComparator {
    typedef Slice DecodedType;

    virtual DecodedType decode_key(const char* key) const {
      // The format of key is frozen and can be terated as a part of the API
      // contract. Refer to MemTable::Add for details.
      return GetLengthPrefixedSlice(key);
    }
    const InternalKeyComparator comparator;
    explicit KeyComparator(const InternalKeyComparator& c) : comparator(c) {}
    int operator()(const char* a, const char* b) const;
    int operator()(const char* prefix_len_key,
                   const DecodedType& key) const;
  };
  typedef InlineSkipList<KeyComparator> Table;
  Table* GetTable(){
    return &table_;
  }
  // Increase reference count.
  void Ref() { refs_.fetch_add(1); }

  // Drop reference count.  Delete if no more references exist.
  void Unref() {
    refs_.fetch_sub(1);
    assert(refs_ >= 0);
    if (refs_ <= 0) {
      // TODO: THis assertion may changed in the future
      assert(kv_num.load() == MEMTABLE_SEQ_SIZE);
      delete this;
    }
  }
  void SimpleDelete(){
    delete this;
  }

  // Returns an estimate of the number of bytes of data in use by this
  // data structure. It is safe to call when MemTable is being modified.
  size_t ApproximateMemoryUsage();

  // Return an iterator that yields the contents of the memtable.
  //
  // The caller must ensure that the underlying MemTable remains live
  // while the returned iterator is live.  The keys returned by this
  // iterator are internal keys encoded by AppendInternalKey in the
  // db/format.{h,cc} module.
  Iterator* NewIterator();

  // Add an entry into memtable that maps key to value at the
  // specified sequence number and with the specified type.
  // Typically value will be empty if type==kTypeDeletion.
  void Add(SequenceNumber seq, ValueType type, const Slice& key,
           const Slice& value);

  // If memtable contains a value for key, store it in *value and return true.
  // If memtable contains a deletion for key, store a NotFound() error
  // in *status and return true.
  // Else, return false.
  bool Get(const LookupKey& key, std::string* value, Status* s);
  void SetLargestSeq(uint64_t seq){
    largest_seq_supposed = seq;
  }
  void SetFirstSeq(uint64_t seq){
    first_seq = seq;
  }
//  void SetLargestSeqTillNow(uint64_t seq){
//
//  }
  bool CheckFlushScheduled(){
    return flush_state_ == FLUSH_SCHEDULED;
  }
  void SetFlushState(FlushStateEnum state){
    flush_state_.store(state);
  }
  uint64_t Getlargest_seq_supposed() const{
    return largest_seq_supposed;
  }
  uint64_t GetFirstseq() const{
    return first_seq;
  }
  void increase_kv_num(size_t num){
    kv_num.fetch_add(num);
    assert(num == 1);
    assert(kv_num <= MEMTABLE_SEQ_SIZE);
    //TODO; For a write batch you the write may cross the boder, we need to modify
    // the boder of the next table
    if (kv_num >= MEMTABLE_SEQ_SIZE){
      able_to_flush.store(true);
    }
  }
  size_t Get_kv_num(){
    return kv_num;
  }
 private:
  friend class MemTableIterator;
  friend class MemTableBackwardIterator;





  KeyComparator comparator_;
  std::atomic<int> refs_;
  std::atomic<size_t> kv_num = 0;

  ConcurrentArena arena_;
  Table table_;
  std::atomic<FlushStateEnum> flush_state_ = FLUSH_NOT_REQUESTED;
  int64_t first_seq;
  std::atomic<int64_t> largest_seq_till_now = 0;
  int64_t largest_seq_supposed;

};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_MEMTABLE_H_
