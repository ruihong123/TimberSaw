// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/format.h"

#include "leveldb/env.h"
#include "port/port.h"
#include "table/block.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {

void BlockHandle::EncodeTo(std::string* dst) const {
  // Sanity check that all fields have been set
  assert(offset_ != ~static_cast<uint64_t>(0));
  assert(size_ != ~static_cast<uint64_t>(0));
  PutVarint64(dst, offset_);
  PutVarint64(dst, size_);
}

Status BlockHandle::DecodeFrom(Slice* input) {
  if (GetVarint64(input, &offset_) && GetVarint64(input, &size_)) {
    return Status::OK();
  } else {
    return Status::Corruption("bad block handle");
  }
}

void Footer::EncodeTo(std::string* dst) const {
  const size_t original_size = dst->size();
  metaindex_handle_.EncodeTo(dst);
  index_handle_.EncodeTo(dst);
  dst->resize(2 * BlockHandle::kMaxEncodedLength);  // Padding
  PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber & 0xffffffffu));
  PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber >> 32));
  assert(dst->size() == original_size + kEncodedLength);
  (void)original_size;  // Disable unused variable warning.
}

Status Footer::DecodeFrom(Slice* input) {
  const char* magic_ptr = input->data() + kEncodedLength - 8;
  const uint32_t magic_lo = DecodeFixed32(magic_ptr);
  const uint32_t magic_hi = DecodeFixed32(magic_ptr + 4);
  const uint64_t magic = ((static_cast<uint64_t>(magic_hi) << 32) |
                          (static_cast<uint64_t>(magic_lo)));
  if (magic != kTableMagicNumber) {
    return Status::Corruption("not an sstable (bad magic number)");
  }

  Status result = metaindex_handle_.DecodeFrom(input);
  if (result.ok()) {
    result = index_handle_.DecodeFrom(input);
  }
  if (result.ok()) {
    // We skip over any leftover data (just padding for now) in "input"
    const char* end = magic_ptr + 8;
    *input = Slice(end, input->data() + input->size() - end);
  }
  return result;
}
bool Find_Remote_mr(std::map<int, ibv_mr*> remote_data_blocks,
                    const BlockHandle& handle, ibv_mr* remote_mr) {
  uint64_t  position = handle.offset();
  auto iter = remote_data_blocks.begin();
  while(iter != remote_data_blocks.end()){
    if (position >= iter->second->length){// the missing of the equal, cause the
                                          // problem of iterator sometime drop, make the compaction failed
      position -= ((iter->second->length));
      iter++;
    }else{
      assert(position + handle.size() + kBlockTrailerSize <= iter->second->length);
      *(remote_mr) = *(iter->second);
//      DEBUG_arg("Block buffer position %lu\n", position);
      remote_mr->addr = static_cast<void*>(static_cast<char*>(iter->second->addr) + position);
      return true;
    }
  }
  return false;
}
//TODO: Make the block mr searching and creating outside this function, so that datablock is
// the same as data index block and filter block.
Status ReadDataBlock(std::map<int, ibv_mr*> remote_data_blocks, const ReadOptions& options,
                 const BlockHandle& handle, BlockContents* result) {
  result->data = Slice();
//  result->cachable = false;
//  result->heap_allocated = false;
  // Read the block contents as well as the type/crc footer.
  // See table_builder.cc for the code that built this structure.
  Status s = Status::OK();
  std::shared_ptr<RDMA_Manager> rdma_mg = Env::Default()->rdma_mg;
  size_t n = static_cast<size_t>(handle.size());
  assert(n + kBlockTrailerSize < rdma_mg->name_to_size["DataBlock"]);
  ibv_mr* contents= nullptr;
  ibv_mr remote_mr;
//#ifndef NDEBUG
//  ibv_wc wc;
//  int check_poll_number =
//      rdma_mg->try_poll_this_thread_completions(&wc, 1, "read_local");
//  assert( check_poll_number == 0);
//#endif
#ifdef GETANALYSIS
  auto start = std::chrono::high_resolution_clock::now();
#endif
  if (Find_Remote_mr(remote_data_blocks, handle, &remote_mr)){
#ifdef GETANALYSIS
    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
    printf("RDMA Read time elapse is %zu\n",  duration.count());
#endif
    rdma_mg->Allocate_Local_RDMA_Slot(contents, "DataBlock");

    rdma_mg->RDMA_Read(&remote_mr, contents, n + kBlockTrailerSize, "read_local", IBV_SEND_SIGNALED, 1);

  }else{
    s = Status::Corruption("Remote memtable out of buffer");
  }
//#ifndef NDEBUG
//  usleep(100);
//  check_poll_number = rdma_mg->try_poll_this_thread_completions(&wc,1);
//  assert( check_poll_number == 0);
//#endif
  // Check the crc of the type and the block contents
  const char* data = static_cast<char*>(contents->addr);  // Pointer to where Read put the data
  if (options.verify_checksums) {
    const uint32_t crc = crc32c::Unmask(DecodeFixed32(data + n + 1));
    const uint32_t actual = crc32c::Value(data, n + 1);
    if (actual != crc) {
//      delete[] buf;
      DEBUG("Data block Checksum mismatch\n");
      assert(false);
      s = Status::Corruption("block checksum mismatch");
      return s;
    }
  }
//  printf("data[n] is %c\n", data[n]);
  switch (data[n]) {
    case kNoCompression:
        result->data = Slice(data, n);
//        result->heap_allocated = false;
//        result->cachable = false;  // Do not double-cache


      // Ok
        break;
//    case kSnappyCompression: {
//      size_t ulength = 0;
//      if (!port::Snappy_GetUncompressedLength(data, n, &ulength)) {
//        rdma_mg->Deallocate_Local_RDMA_Slot(data, "DataBlock")
//        return Status::Corruption("corrupted compressed block contents");
//      }
//      char* ubuf = new char[ulength];
//      if (!port::Snappy_Uncompress(data, n, ubuf)) {
//        delete[] buf;
////        delete[] ubuf;
//        return Status::Corruption("corrupted compressed block contents");
//      }
//      delete[] buf;
//      result->data = Slice(ubuf, ulength);
//      result->heap_allocated = true;
//      result->cachable = true;
//      break;
//    }
    default:
      assert(data[n] != kNoCompression);
      assert(false);
      rdma_mg->Deallocate_Local_RDMA_Slot(static_cast<void*>(const_cast<char *>(data)), "DataBlock");
      DEBUG("Data block illegal compression type\n");
      return Status::Corruption("bad block type");
  }

  return Status::OK();
}

Status ReadDataIndexBlock(ibv_mr* remote_mr, const ReadOptions& options,
                          BlockContents* result) {
  result->data = Slice();
//  result->cachable = false;
//  result->heap_allocated = false;
  // Read the block contents as well as the type/crc footer.
  // See table_builder.cc for the code that built this structure.
  Status s = Status::OK();
  std::shared_ptr<RDMA_Manager> rdma_mg = Env::Default()->rdma_mg;
  size_t n = remote_mr->length - kBlockTrailerSize;
  assert(n>0);
  assert(n + kBlockTrailerSize < rdma_mg->name_to_size["DataIndexBlock"]);
  ibv_mr* contents;
  rdma_mg->Allocate_Local_RDMA_Slot(contents, "DataIndexBlock");
  rdma_mg->RDMA_Read(remote_mr, contents, n + kBlockTrailerSize, "read_local", IBV_SEND_SIGNALED, 1);

  // Check the crc of the type and the block contents
  const char* data = static_cast<char*>(contents->addr);  // Pointer to where Read put the data
  if (options.verify_checksums) {
    const uint32_t crc = crc32c::Unmask(DecodeFixed32(data + n + 1));
    const uint32_t actual = crc32c::Value(data, n + 1);
    if (actual != crc) {
//      delete[] buf;
      DEBUG("Index block Checksum mismatch\n");
      assert(false);
      s = Status::Corruption("block checksum mismatch");
      return s;
    }
  }

  switch (data[n]) {
    case kNoCompression:
      //block content do not contain compression type and check sum
      result->data = Slice(data, n);
//      result->heap_allocated = false;
//      result->cachable = false;  // Do not double-cache


      // Ok
      break;
//    case kSnappyCompression: {
//      size_t ulength = 0;
//      if (!port::Snappy_GetUncompressedLength(data, n, &ulength)) {
//        rdma_mg->Deallocate_Local_RDMA_Slot(data, "DataBlock")
//        return Status::Corruption("corrupted compressed block contents");
//      }
//      char* ubuf = new char[ulength];
//      if (!port::Snappy_Uncompress(data, n, ubuf)) {
//        delete[] buf;
////        delete[] ubuf;
//        return Status::Corruption("corrupted compressed block contents");
//      }
//      delete[] buf;
//      result->data = Slice(ubuf, ulength);
//      result->heap_allocated = true;
//      result->cachable = true;
//      break;
//    }
    default:
      rdma_mg->Deallocate_Local_RDMA_Slot(static_cast<void*>(const_cast<char *>(data)), "DataBlock");
      DEBUG("index block illegal compression type\n");
      return Status::Corruption("bad block type");
  }

  return Status::OK();
}
Status ReadFilterBlock(ibv_mr* remote_mr,
                          const ReadOptions& options, BlockContents* result) {
  result->data = Slice();
//  result->cachable = false;
//  result->heap_allocated = false;
  // Read the block contents as well as the type/crc footer.
  // See table_builder.cc for the code that built this structure.
  Status s = Status::OK();
  std::shared_ptr<RDMA_Manager> rdma_mg = Env::Default()->rdma_mg;
  size_t n = remote_mr->length - kBlockTrailerSize;
  assert(n + kBlockTrailerSize < rdma_mg->name_to_size["FilterBlock"]);
  ibv_mr* contents;
  rdma_mg->Allocate_Local_RDMA_Slot(contents, "FilterBlock");
  rdma_mg->RDMA_Read(remote_mr, contents, n + kBlockTrailerSize, "read_local", IBV_SEND_SIGNALED, 1);

  // Check the crc of the type and the block contents
  const char* data = static_cast<char*>(contents->addr);  // Pointer to where Read put the data
  if (options.verify_checksums) {
    const uint32_t crc = crc32c::Unmask(DecodeFixed32(data + n + 1));
    const uint32_t actual = crc32c::Value(data, n + 1);
    if (actual != crc) {
//      delete[] buf;
      DEBUG("Filter Checksum mismatch\n");
//      assert(false);
      s = Status::Corruption("block checksum mismatch");
      return s;
    }
  }

  switch (data[n]) {
    case kNoCompression:
      result->data = Slice(data, n);
//      result->heap_allocated = false;
//      result->cachable = false;  // Do not double-cache


      // Ok
      break;
//    case kSnappyCompression: {
//      size_t ulength = 0;
//      if (!port::Snappy_GetUncompressedLength(data, n, &ulength)) {
//        rdma_mg->Deallocate_Local_RDMA_Slot(data, "DataBlock")
//        return Status::Corruption("corrupted compressed block contents");
//      }
//      char* ubuf = new char[ulength];
//      if (!port::Snappy_Uncompress(data, n, ubuf)) {
//        delete[] buf;
////        delete[] ubuf;
//        return Status::Corruption("corrupted compressed block contents");
//      }
//      delete[] buf;
//      result->data = Slice(ubuf, ulength);
//      result->heap_allocated = true;
//      result->cachable = true;
//      break;
//    }
    default:
      rdma_mg->Deallocate_Local_RDMA_Slot(static_cast<void*>(const_cast<char *>(data)), "DataBlock");
      DEBUG("Filter illegal compression type\n");
      return Status::Corruption("bad block type");
  }

  return Status::OK();
}
}  // namespace leveldb
