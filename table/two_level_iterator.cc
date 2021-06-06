// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/two_level_iterator.h"



namespace leveldb {




TwoLevelIterator::TwoLevelIterator(Iterator* index_iter,
                                   BlockFunction block_function, void* arg,
                                   const ReadOptions& options)
    : block_function_(block_function),
      arg_(arg),
      options_(options),
      index_iter_(index_iter),
      data_iter_(nullptr), valid_(false) {}

TwoLevelIterator::~TwoLevelIterator() = default;

void TwoLevelIterator::Seek(const Slice& target) {
  index_iter_.Seek(target);
  InitDataBlock();
  if (data_iter_.iter() != nullptr) {
    data_iter_.Seek(target);
    valid_ = true;
  }
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::SeekToFirst() {
  index_iter_.SeekToFirst();
  InitDataBlock();
  if (data_iter_.iter() != nullptr) {
    data_iter_.SeekToFirst();
    valid_ = true;
  }
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::SeekToLast() {
  index_iter_.SeekToLast();
  InitDataBlock();
  if (data_iter_.iter() != nullptr){
    data_iter_.SeekToLast();
    valid_ = true;
  }
  SkipEmptyDataBlocksBackward();
}

void TwoLevelIterator::Next() {
  assert(Valid());
  data_iter_.Next();
  SkipEmptyDataBlocksForward();
#ifndef NDEBUG
  if (Valid()){
    if (num_entries > 0) {
      assert(static_cast<Block::Iter*>(data_iter_.iter())->Compare(key(), Slice(last_key)) >= 0);
    }
    num_entries++;
    last_key = key().ToString();
  }
#endif
}

void TwoLevelIterator::Prev() {
  assert(Valid());
  data_iter_.Prev();
  SkipEmptyDataBlocksBackward();
}

void TwoLevelIterator::SkipEmptyDataBlocksForward() {

  while (data_iter_.iter() == nullptr || !data_iter_.Valid()) {
    // Move to next block
    if (!index_iter_.Valid()) {
//      SetDataIterator(nullptr);
      valid_ = false;
      return;
    }
    index_iter_.Next();
//    printf("Move to next block\n");
    InitDataBlock();
    if (data_iter_.iter() != nullptr) data_iter_.SeekToFirst();
  }
//  printf("Move to next data, key is %s", data_iter_.key().ToString().c_str());
//  printf("Iterator pointer is %p\n", this);
}

void TwoLevelIterator::SkipEmptyDataBlocksBackward() {
  while (data_iter_.iter() == nullptr || !data_iter_.Valid()) {
    // Move to next block
    if (!index_iter_.Valid()) {
//      SetDataIterator(nullptr);
      valid_ = false;
      return;
    }
    index_iter_.Prev();
    InitDataBlock();
    if (data_iter_.iter() != nullptr) data_iter_.SeekToLast();
  }
}

void TwoLevelIterator::SetDataIterator(Iterator* data_iter) {
  if (data_iter_.iter() != nullptr) SaveError(data_iter_.status());
  data_iter_.Set(data_iter);
}

void TwoLevelIterator::InitDataBlock() {
  if (!index_iter_.Valid()) {
//    SetDataIterator(nullptr);
    valid_ = false;
    DEBUG_arg("Index block invalid, error: %s\n", status().ToString().c_str());
  } else {
//    DEBUG("Index block valid\n");
    Slice handle = index_iter_.value();
#ifndef NDEBUG
    Slice test_handle = handle;
    BlockHandle bhandle;
    bhandle.DecodeFrom(&test_handle);
//    printf("Iterator pointer is %p, Offset is %lu, this data block size is %lu\n", this, bhandle.offset(), bhandle.size());
#endif
    if (valid_ &&
        handle.compare(data_block_handle_) == 0) {
      // data_iter_ is already constructed with this iterator, so
      // no need to change anything
    } else {
      Iterator* iter = (*block_function_)(arg_, options_, handle);
      data_block_handle_.assign(handle.data(), handle.size());
      SetDataIterator(iter);
    }
  }
}

  // namespace




TwoLevelFileIterator::TwoLevelFileIterator(Version::LevelFileNumIterator* index_iter,
                                                 FileFunction file_function, void* arg,
                                   const ReadOptions& options)
    : file_function_(file_function),
      arg_(arg),
      options_(options),
      index_iter_(index_iter),
      data_iter_(nullptr), valid_(false) {}

TwoLevelFileIterator::~TwoLevelFileIterator() = default;

void TwoLevelFileIterator::Seek(const Slice& target) {
  index_iter_.Seek(target);
  InitDataBlock();
  if (data_iter_.iter() != nullptr) {
    data_iter_.Seek(target);
    valid_ = true;
  }
  SkipEmptyDataBlocksForward();
}

void TwoLevelFileIterator::SeekToFirst() {
  index_iter_.SeekToFirst();
  InitDataBlock();
  if (data_iter_.iter() != nullptr) {
    data_iter_.SeekToFirst();
    valid_ = true;
  }
  SkipEmptyDataBlocksForward();
}

void TwoLevelFileIterator::SeekToLast() {
  index_iter_.SeekToLast();
  InitDataBlock();
  if (data_iter_.iter() != nullptr){
    data_iter_.SeekToLast();
    valid_ = true;
  }
  SkipEmptyDataBlocksBackward();
}

void TwoLevelFileIterator::Next() {
  assert(Valid());
  data_iter_.Next();
  SkipEmptyDataBlocksForward();
}

void TwoLevelFileIterator::Prev() {
  assert(Valid());
  data_iter_.Prev();
  SkipEmptyDataBlocksBackward();
}

void TwoLevelFileIterator::SkipEmptyDataBlocksForward() {
  while (data_iter_.iter() == nullptr || !data_iter_.Valid()) {
    // Move to next block
    if (!index_iter_.Valid()) {
//      SetDataIterator(nullptr);
      valid_ = false;
      return;
    }
    index_iter_.Next();
    InitDataBlock();
    if (data_iter_.iter() != nullptr) data_iter_.SeekToFirst();
  }
}

void TwoLevelFileIterator::SkipEmptyDataBlocksBackward() {
  while (data_iter_.iter() == nullptr || !data_iter_.Valid()) {
    // Move to next block
    if (!index_iter_.Valid()) {
      // Not set as nullptr
//      SetDataIterator(nullptr);
      valid_ = false;
      return;
    }
    index_iter_.Prev();
    InitDataBlock();
    if (data_iter_.iter() != nullptr) data_iter_.SeekToLast();
  }
}

void TwoLevelFileIterator::SetDataIterator(Iterator* data_iter) {
  if (data_iter_.iter() != nullptr) SaveError(data_iter_.status());
  data_iter_.Set(data_iter);
}

void TwoLevelFileIterator::InitDataBlock() {
  if (!index_iter_.Valid()) {
    // Not set the iter as nullptr when reaching the end
//    SetDataIterator(nullptr);
    valid_ = false;
  } else {
    std::shared_ptr<RemoteMemTableMetaData> remote_table = index_iter_.value();
    if (valid_ && remote_table == this_remote_table) {
      // data_iter_ is already constructed with this iterator, so
      // no need to change anything
    } else {
      Iterator* iter = (*file_function_)(arg_, options_, remote_table);
      this_remote_table = remote_table;
      SetDataIterator(iter);
    }
  }


}  // namespace
Iterator* NewTwoLevelIterator(Iterator* index_iter,
                              BlockFunction block_function, void* arg,
                              const ReadOptions& options) {
  return new TwoLevelIterator(index_iter, block_function, arg, options);
}
Iterator* NewTwoLevelFileIterator(Version::LevelFileNumIterator* index_iter,
                                  FileFunction file_function, void* arg,
                              const ReadOptions& options) {
  return new TwoLevelFileIterator(index_iter, file_function, arg, options);
}

}  // namespace leveldb
