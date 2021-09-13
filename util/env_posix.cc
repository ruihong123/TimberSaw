// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <dirent.h>
#include <fcntl.h>
#include <pthread.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <queue>
#include <set>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>

#include "leveldb/env.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"
#include "port/port.h"
#include "port/thread_annotations.h"
#include "util/env_posix_test_helper.h"
#include "util/posix_logger.h"
#include "util/rdma.h"

namespace leveldb {

namespace {

// Set by EnvPosixTestHelper::SetReadOnlyMMapLimit() and MaxOpenFiles().
int g_open_read_only_file_limit = -1;

// Up to 1000 mmap regions for 64-bit binaries; none for 32-bit.
constexpr const int kDefaultMmapLimit = (sizeof(void*) >= 8) ? 1000 : 0;

// Can be set using EnvPosixTestHelper::SetReadOnlyMMapLimit().
int g_mmap_limit = kDefaultMmapLimit;

// Common flags defined for all posix open operations
#if defined(HAVE_O_CLOEXEC)
constexpr const int kOpenBaseFlags = O_CLOEXEC;
#else
constexpr const int kOpenBaseFlags = 0;
#endif  // defined(HAVE_O_CLOEXEC)

constexpr const size_t kWritableFileBufferSize = 65536;

Status PosixError(const std::string& context, int error_number) {
  if (error_number == ENOENT) {
    return Status::NotFound(context, std::strerror(error_number));
  } else {
    return Status::IOError(context, std::strerror(error_number));
  }
}

// Helper class to limit resource usage to avoid exhaustion.
// Currently used to limit read-only file descriptors and mmap file usage
// so that we do not run out of file descriptors or virtual memory, or run into
// kernel performance problems for very large databases.
class Limiter {
 public:
  // Limit maximum number of resources to |max_acquires|.
  Limiter(int max_acquires) : acquires_allowed_(max_acquires) {}

  Limiter(const Limiter&) = delete;
  Limiter operator=(const Limiter&) = delete;

  // If another resource is available, acquire it and return true.
  // Else return false.
  bool Acquire() {
    int old_acquires_allowed =
        acquires_allowed_.fetch_sub(1, std::memory_order_relaxed);

    if (old_acquires_allowed > 0) return true;

    acquires_allowed_.fetch_add(1, std::memory_order_relaxed);
    return false;
  }

  // Release a resource acquired by a previous call to Acquire() that returned
  // true.
  void Release() { acquires_allowed_.fetch_add(1, std::memory_order_relaxed); }

 private:
  // The number of available resources.
  //
  // This is a counter and is not tied to the invariants of any other class, so
  // it can be operated on safely using std::memory_order_relaxed.
  std::atomic<int> acquires_allowed_;
};

// Implements sequential read access in a file using read().
//
// Instances of this class are thread-friendly but not thread-safe, as required
// by the SequentialFile API.
class RDMASequentialFile final : public SequentialFile {
 public:
  RDMASequentialFile(SST_Metadata* sst_meta,
                     RDMA_Manager* rdma_mg)
      : sst_meta_(sst_meta), rdma_mg_(rdma_mg) {}
  ~RDMASequentialFile() override { }

  Status Read(size_t n, Slice* result, char* scratch) override {
    const std::shared_lock<std::shared_mutex> lock(sst_meta_->file_lock);
//  auto myid = std::this_thread::get_id();
//  std::stringstream ss;
//  ss << myid;
//  std::string posix_tid = ss.str();
    Status s;
//  assert((position_+ n) <= sst_meta_->file_size);

    ibv_mr* local_mr_pointer;
    local_mr_pointer = nullptr;
    ibv_mr remote_mr = {}; // value copy of the ibv_mr in the sst metadata
    remote_mr = *(sst_meta_->mr);
    remote_mr.addr = static_cast<void*>(static_cast<char*>(remote_mr.addr) + position_);
    int flag;
    rdma_mg_->Allocate_Local_RDMA_Slot(local_mr_pointer, std::string("read"));
    if (position_ == sst_meta_->file_size)
      return Status::OK();
    unsigned int position_temp = position_;
    SST_Metadata* sst_meta_current = sst_meta_;// set sst_current to head.

    //find the SST_Metadata for current chunk.
    size_t chunk_offset = position_temp%(rdma_mg_->Table_Size);
    while (position_temp >= rdma_mg_->Table_Size){
      sst_meta_current = sst_meta_current->next_ptr;
      position_temp = position_temp- rdma_mg_->Table_Size;
    }
//  std::string thread_id = *(static_cast<std::string*>(rdma_mg_->t_local_1->Get()));
    std::string thread_id;
    char* chunk_src = scratch;

    if (n + position_>=sst_meta_->file_size)
      n = sst_meta_->file_size - position_;
    size_t n_original = n;
    while (n > rdma_mg_->name_to_size.at("read")){
      Read_chunk(chunk_src, rdma_mg_->name_to_size.at("read"), local_mr_pointer, remote_mr,
                 chunk_offset, sst_meta_current, thread_id);
//    chunk_src += rdma_mg_->name_to_size.at("read");
      n -= rdma_mg_->name_to_size.at("read");
//    remote_mr.addr = static_cast<void*>(static_cast<char*>(remote_mr.addr) + rdma_mg_->name_to_size.at("read"));
      position_ = position_ + rdma_mg_->name_to_size.at("read");
    }
    Read_chunk(chunk_src, n, local_mr_pointer, remote_mr, chunk_offset,
               sst_meta_current, thread_id);
    position_ = position_+ n;
//  auto stop = std::chrono::high_resolution_clock::now();
//  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//  printf("RDMA read size: %zu time elapse: %ld\n", n, duration.count());
//  memcpy(scratch, static_cast<char*>(local_mr_pointer->addr),n);
//  start = std::chrono::high_resolution_clock::now();
    *result = Slice(scratch, n_original);// n has been changed, so we need record the original n.
    if(rdma_mg_->Deallocate_Local_RDMA_Slot(local_mr_pointer->addr,
                                            std::string("read")))
      delete local_mr_pointer;
    else
      s = Status::IOError(
          "While RDMA Local Buffer Deallocate failed " + std::to_string(position_) + " len " + std::to_string(n),
          sst_meta_->fname);
    return s;
  }
  Status Read_chunk(char*& buff_ptr, size_t size,
                                          ibv_mr* local_mr_pointer,
                                          ibv_mr& remote_mr,
                                          size_t& chunk_offset,
                                          SST_Metadata*& sst_meta_current,
                                          std::string& thread_id) const {
//  auto start = std::chrono::high_resolution_clock::now();
    Status s = Status::OK();
    assert(size <= rdma_mg_->name_to_size.at("read"));

    if (size + chunk_offset >= rdma_mg_->Table_Size ){
      // if block write accross two SSTable chunks, seperate it into 2 steps.
      //First step
      size_t first_half = rdma_mg_->Table_Size - chunk_offset;
      size_t second_half = size - (rdma_mg_->Table_Size - chunk_offset);
      int flag = rdma_mg_->RDMA_Read(&remote_mr, local_mr_pointer, first_half,
                                     thread_id, IBV_SEND_SIGNALED,1);
      memcpy(buff_ptr, local_mr_pointer->addr, first_half);// copy to the buffer

      if (flag!=0){
        return Status::IOError("While appending to file", sst_meta_->fname);
      }
      //move the buffer to the next part
      buff_ptr += first_half;
      assert(sst_meta_current->next_ptr != nullptr);
      sst_meta_current = sst_meta_current->next_ptr;
      remote_mr = *(sst_meta_current->mr);
      chunk_offset = 0;
      flag = rdma_mg_->RDMA_Read(&remote_mr, local_mr_pointer, second_half,
                                 thread_id, IBV_SEND_SIGNALED,1);
      memcpy(buff_ptr, local_mr_pointer->addr, second_half);// copy to the buffer
//    std::cout << "read blocks accross Table chunk" << std::endl;
      if (flag!=0){

        return Status::IOError("While appending to file", sst_meta_->fname);


      }
      remote_mr.addr = static_cast<void*>(static_cast<char*>(remote_mr.addr) + second_half);
      chunk_offset = second_half;
      buff_ptr += second_half;
    }else{
//    auto start = std::chrono::high_resolution_clock::now();
      int flag =
          rdma_mg_->RDMA_Read(&remote_mr, local_mr_pointer, size, thread_id, IBV_SEND_SIGNALED,1);
//    auto stop = std::chrono::high_resolution_clock::now();
//    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//    printf("Bare RDMA read size: %zu time elapse: %ld\n", size, duration.count());
//    start = std::chrono::high_resolution_clock::now();
      memcpy(buff_ptr, local_mr_pointer->addr, size);// copy to the buffer
//    stop = std::chrono::high_resolution_clock::now();
//    duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//    printf("Read Memcopy size: %zu time elapse: %ld\n", size, duration.count());
//    std::cout << "read blocks within Table chunk" << std::endl;

      if (flag!=0){

        return Status::IOError("While appending to file", sst_meta_->fname);


      }
      remote_mr.addr = static_cast<void*>(static_cast<char*>(remote_mr.addr) + size);
      chunk_offset += size;
      buff_ptr += size;
    }
//  auto stop = std::chrono::high_resolution_clock::now();
//  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
//  std::cout << size <<"inner Read Chunk time elapse :" << duration.count() << std::endl;
    return s;
  }

  Status Skip(uint64_t n) override {
    position_ = n;
    return Status::OK();
  }

 private:
//  const int fd_;
  const std::string filename_;
  SST_Metadata* sst_meta_;
  RDMA_Manager* rdma_mg_;
  unsigned int position_ = 0;
};
//class RDMASequentialFile final : public SequentialFile {
// public:
//  RDMASequentialFile(std::string filename, int fd)
//      : fd_(fd), filename_(filename) {}
//  RDMASequentialFile) override { close(fd_); }
//
//  Status Read(size_t n, Slice* result, char* scratch) override {
//    Status status;
//    while (true) {
//      ::ssize_t read_size = ::read(fd_, scratch, n);
//      if (read_size < 0) {  // Read error.
//        if (errno == EINTR) {
//          continue;  // Retry
//        }
//        status = PosixError(filename_, errno);
//        break;
//      }
//      *result = Slice(scratch, read_size);
//      break;
//    }
//    return status;
//  }
//
//  Status Skip(uint64_t n) override {
//    if (::lseek(fd_, n, SEEK_CUR) == static_cast<off_t>(-1)) {
//      return PosixError(filename_, errno);
//    }
//    return Status::OK();
//  }
//
// private:
//  const int fd_;
//  const std::string filename_;
//};
// Implements random read access in a file using pread().
//
// Instances of this class are thread-safe, as required by the RandomAccessFile
// API. Instances are immutable and Read() only calls thread-safe library
// functions.
class RDMARandomAccessFile final : public RandomAccessFile {
 public:
  // The new instance takes ownership of |fd|. |fd_limiter| must outlive this
  // instance, and will be used to determine if .
  RDMARandomAccessFile(SST_Metadata* sst_meta,
                        RDMA_Manager* rdma_mg)
      : sst_meta_head_(sst_meta),
        rdma_mg_(rdma_mg) {}

  ~RDMARandomAccessFile() override {}

  Status Read(uint64_t offset, size_t n, Slice* result,
              char* scratch) const override {
    const std::shared_lock<std::shared_mutex> lock(sst_meta_head_->file_lock);
    //  auto stop = std::chrono::high_resolution_clock::now();
//  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//  printf("Read Get the lock time elapse: %ld\n", duration.count());
//  const std::lock_guard<std::mutex> lock(sst_meta_head_->file_lock);

//  const std::lock_guard<std::mutex> lock(
//      rdma_mg_->remote_mem_mutex);// write lock
    Status s;
    //Find the Poxis thread ID for the key for the qp_map in rdma manager.
//  auto myid = std::this_thread::get_id();
//  std::stringstream ss;
//  ss << myid;
//  std::string posix_tid = ss.str();
//  start = std::chrono::high_resolution_clock::now();
//  assert(n<= rdma_mg_->Read_Block_Size);
    assert(offset + n <= sst_meta_head_->file_size);
    size_t n_original = n;
    SST_Metadata* sst_meta_current = sst_meta_head_;// set sst_current to head.
    //find the SST_Metadata for current chunk.
    size_t chunk_offset = offset%(rdma_mg_->Table_Size);
    while (offset >= rdma_mg_->Table_Size){
      sst_meta_current = sst_meta_current->next_ptr;
      offset = offset- rdma_mg_->Table_Size;
    }
//  std::string thread_id = *(static_cast<std::string*>(rdma_mg_->t_local_1->Get()));
    std::string thread_id;
//  stop = std::chrono::high_resolution_clock::now();
//  duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//  printf("Read Set up sst file pointer time elapse: %ld\n", duration.count());
//  std::cout << "Read data from " << sst_meta_head_->fname <<" " << sst_meta_current->mr->addr <<  " offset: "
//            << offset << "size: " << n << std::endl;
//  start = std::chrono::high_resolution_clock::now();
    ibv_mr remote_mr = {}; // value copy of the ibv_mr in the sst metadata
    remote_mr = *(sst_meta_current->mr);
    remote_mr.addr = static_cast<void*>(static_cast<char*>(remote_mr.addr) + offset);
    assert(offset == chunk_offset);
    char* chunk_src = scratch;

    std::_Rb_tree_iterator<std::pair<void * const, In_Use_Array>> mr_start;
//  std::cout << "Read data from " << sst_meta_head_->mr << " " << sst_meta_current->mr->addr << " offset: "
//                          << chunk_offset << "size: " << n << std::endl;
    if (rdma_mg_->CheckInsideLocalBuff(scratch, mr_start,
                                       &rdma_mg_->name_to_mem_pool.at("read"))){
//    auto mr_start = rdma_mg_->Read_Local_Mem_Bitmap->lower_bound(scratch);
      ibv_mr local_mr;
      local_mr = *(mr_start->second.get_mr_ori());
      local_mr.addr = scratch;
      assert(n <= rdma_mg_->name_to_size.at("read"));
      if (n + chunk_offset >= rdma_mg_->Table_Size ){
        // if block write accross two SSTable chunks, seperate it into 2 steps.
        //First step
        size_t first_half = rdma_mg_->Table_Size - chunk_offset;
        size_t second_half = n - (rdma_mg_->Table_Size - chunk_offset);
        int flag = rdma_mg_->RDMA_Read(&remote_mr, &local_mr, first_half,
                                       thread_id, IBV_SEND_SIGNALED,1);

        if (flag!=0){
          return Status::IOError("While appending to file", sst_meta_head_->fname);
        }
        //move the buffer to the next part
        local_mr.addr = static_cast<void*>(static_cast<char*>(local_mr.addr) + first_half);
        assert(sst_meta_current->next_ptr != nullptr);
        sst_meta_current = sst_meta_current->next_ptr;
        remote_mr = *(sst_meta_current->mr);
        chunk_offset = 0;
        flag = rdma_mg_->RDMA_Read(&remote_mr, &local_mr, second_half,
                                   thread_id, IBV_SEND_SIGNALED,1);
//    std::cout << "New read blocks accross Table chunk" << std::endl;
        if (flag!=0){

          return Status::IOError("While appending to file", sst_meta_head_->fname);


        }
//      remote_mr.addr = static_cast<void*>(static_cast<char*>(remote_mr.addr) + second_half);
        chunk_offset = second_half;
//      local_mr.addr = static_cast<void*>(static_cast<char*>(local_mr.addr) + second_half);
      }else{
//    auto start = std::chrono::high_resolution_clock::now();
        int flag =
            rdma_mg_->RDMA_Read(&remote_mr, &local_mr, n, thread_id, IBV_SEND_SIGNALED,1);
//    auto stop = std::chrono::high_resolution_clock::now();
//    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//    printf("New Bare RDMA read size: %zu time elapse: %ld\n", n, duration.count());
//    printf("%s", scratch);

//    start = std::chrono::high_resolution_clock::now();
//    stop = std::chrono::high_resolution_clock::now();
//    duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//    printf("Read Memcopy size: %zu time elapse: %ld\n", size, duration.count());
//    std::cout << "read blocks within Table chunk" << std::endl;

        if (flag!=0){

          return Status::IOError("While appending to file", sst_meta_head_->fname);


        }
//      remote_mr.addr = static_cast<void*>(static_cast<char*>(remote_mr.addr) + n);
        chunk_offset += n;
//      local_mr.addr = static_cast<void*>(static_cast<char*>(local_mr.addr) + n);
      }
      *result = Slice(scratch, n_original);
//  auto stop = std::chrono::high_resolution_clock::now();
//  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
//  std::cout << size <<"inner Read Chunk time elapse :" << duration.count() << std::endl;
      return s;

    }else{
      ibv_mr* local_mr_pointer;
      local_mr_pointer = nullptr;
//  stop = std::chrono::high_resolution_clock::now();
//  duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//  printf("Read pointer convert time elapse: %ld\n", duration.count());
//  start = std::chrono::high_resolution_clock::now();
      rdma_mg_->Allocate_Local_RDMA_Slot(local_mr_pointer, std::string("read"));
//  stop = std::chrono::high_resolution_clock::now();
//  duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//  std::printf("Read Memory allocate, time elapse : (%ld)\n", duration.count());
//  auto start = std::chrono::high_resolution_clock::now();
      while (n > rdma_mg_->name_to_size.at("read")){
        Read_chunk(chunk_src, rdma_mg_->name_to_size.at("read"), local_mr_pointer, remote_mr,
                   chunk_offset, sst_meta_current, thread_id);
//    chunk_src += rdma_mg_->name_to_size.at("read");
        n -= rdma_mg_->name_to_size.at("read");
//    remote_mr.addr = static_cast<void*>(static_cast<char*>(remote_mr.addr) + rdma_mg_->name_to_size.at("read"));

      }
      Read_chunk(chunk_src, n, local_mr_pointer, remote_mr, chunk_offset,
                 sst_meta_current, thread_id);
//  auto stop = std::chrono::high_resolution_clock::now();
//  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//  printf("RDMA read size: %zu time elapse: %ld\n", n, duration.count());
//  memcpy(scratch, static_cast<char*>(local_mr_pointer->addr),n);
//  start = std::chrono::high_resolution_clock::now();
      *result = Slice(scratch, n_original);// n has been changed, so we need record the original n.
      if(rdma_mg_->Deallocate_Local_RDMA_Slot(local_mr_pointer->addr,
                                              std::string("read")))
        delete local_mr_pointer;
      else
        s = Status::IOError(
            "While RDMA Local Buffer Deallocate failed " + std::to_string(offset) + " len " + std::to_string(n),
            sst_meta_head_->fname);
//  stop = std::chrono::high_resolution_clock::now();
//  duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//  printf("Read Local buffer deallocate time elapse: %ld\n", duration.count());
//  auto stop = std::chrono::high_resolution_clock::now();
//  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
//  std::cout << n_original <<"Read total time elapse :" << duration.count() << std::endl;
      return s;
    }
  }
  Status Read_chunk(char*& buff_ptr, size_t size,
                                            ibv_mr* local_mr_pointer,
                                            ibv_mr& remote_mr,
                                            size_t& chunk_offset,
                                            SST_Metadata*& sst_meta_current,
                                            std::string& thread_id) const {
//  auto start = std::chrono::high_resolution_clock::now();
    Status s = Status::OK();
    assert(size <= rdma_mg_->name_to_size.at("read"));

    if (size + chunk_offset > rdma_mg_->Table_Size ){
      // if block write accross two SSTable chunks, seperate it into 2 steps.
      //First step
      size_t first_half = rdma_mg_->Table_Size - chunk_offset;
      size_t second_half = size - (rdma_mg_->Table_Size - chunk_offset);
      int flag = rdma_mg_->RDMA_Read(&remote_mr, local_mr_pointer, first_half,
                                     thread_id, IBV_SEND_SIGNALED,1);
      memcpy(buff_ptr, local_mr_pointer->addr, first_half);// copy to the buffer

      if (flag!=0){
        return Status::IOError("While appending to file", sst_meta_head_->fname);
      }
      //move the buffer to the next part
      buff_ptr += first_half;
      assert(sst_meta_current->next_ptr != nullptr);
      sst_meta_current = sst_meta_current->next_ptr;
      remote_mr = *(sst_meta_current->mr);
      chunk_offset = 0;
      flag = rdma_mg_->RDMA_Read(&remote_mr, local_mr_pointer, second_half,
                                 thread_id, IBV_SEND_SIGNALED,1);
      memcpy(buff_ptr, local_mr_pointer->addr, second_half);// copy to the buffer
//    std::cout << "read blocks accross Table chunk" << std::endl;
      if (flag!=0){

        return Status::IOError("While appending to file", sst_meta_head_->fname);


      }
      remote_mr.addr = static_cast<void*>(static_cast<char*>(remote_mr.addr) + second_half);
      chunk_offset = second_half;
      buff_ptr += second_half;
    }else{
//    auto start = std::chrono::high_resolution_clock::now();
      int flag =
          rdma_mg_->RDMA_Read(&remote_mr, local_mr_pointer, size, thread_id, IBV_SEND_SIGNALED,1);
//    auto stop = std::chrono::high_resolution_clock::now();
//    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//    printf("Bare RDMA read size: %zu time elapse: %ld\n", size, duration.count());
//    start = std::chrono::high_resolution_clock::now();
      memcpy(buff_ptr, local_mr_pointer->addr, size);// copy to the buffer
//    stop = std::chrono::high_resolution_clock::now();
//    duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//    printf("Read Memcopy size: %zu time elapse: %ld\n", size, duration.count());
//    std::cout << "read blocks within Table chunk" << std::endl;

      if (flag!=0){

        return Status::IOError("While appending to file", sst_meta_head_->fname);


      }
      remote_mr.addr = static_cast<void*>(static_cast<char*>(remote_mr.addr) + size);
      chunk_offset += size;
      buff_ptr += size;
    }
//  auto stop = std::chrono::high_resolution_clock::now();
//  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
//  std::cout << size <<"inner Read Chunk time elapse :" << duration.count() << std::endl;
    return s;
  }

 private:
  SST_Metadata* sst_meta_head_;
  RDMA_Manager* rdma_mg_;
};
//class RDMARandomAccessFile final : public RandomAccessFile {
// public:
//  // The new instance takes ownership of |fd|. |fd_limiter| must outlive this
//  // instance, and will be used to determine if .
//  RDMARandomAccessFile(std::string filename, int fd, Limiter* fd_limiter)
//      : has_permanent_fd_(fd_limiter->Acquire()),
//        fd_(has_permanent_fd_ ? fd : -1),
//        fd_limiter_(fd_limiter),
//        filename_(std::move(filename)) {
//    if (!has_permanent_fd_) {
//      assert(fd_ == -1);
//      ::close(fd);  // The file will be opened on every read.
//    }
//  }
//
//  RDMARandomAccessFile) override {
//    if (has_permanent_fd_) {
//      assert(fd_ != -1);
//      ::close(fd_);
//      fd_limiter_->Release();
//    }
//  }
//
//  Status Read(uint64_t offset, size_t n, Slice* result,
//              char* scratch) const override {
//    int fd = fd_;
//    if (!has_permanent_fd_) {
//      fd = ::open(filename_.c_str(), O_RDONLY | kOpenBaseFlags);
//      if (fd < 0) {
//        return PosixError(filename_, errno);
//      }
//    }
//
//    assert(fd != -1);
//
//    Status status;
//    ssize_t read_size = ::pread(fd, scratch, n, static_cast<off_t>(offset));
//    *result = Slice(scratch, (read_size < 0) ? 0 : read_size);
//    if (read_size < 0) {
//      // An error: return a non-ok status.
//      status = PosixError(filename_, errno);
//    }
//    if (!has_permanent_fd_) {
//      // Close the temporary file descriptor opened earlier.
//      assert(fd != fd_);
//      ::close(fd);
//    }
//    return status;
//  }
//
// private:
//  const bool has_permanent_fd_;  // If false, the file is opened on every read.
//  const int fd_;                 // -1 if has_permanent_fd_ is false.
//  Limiter* const fd_limiter_;
//  const std::string filename_;
//};
// Implements random read access in a file using mmap().
//
// Instances of this class are thread-safe, as required by the RandomAccessFile
// API. Instances are immutable and Read() only calls thread-safe library
// functions.
class PosixMmapReadableFile final : public RandomAccessFile {
 public:
  // mmap_base[0, length-1] points to the memory-mapped contents of the file. It
  // must be the result of a successful call to mmap(). This instances takes
  // over the ownership of the region.
  //
  // |mmap_limiter| must outlive this instance. The caller must have already
  // aquired the right to use one mmap region, which will be released when this
  // instance is destroyed.
  PosixMmapReadableFile(std::string filename, char* mmap_base, size_t length,
                        Limiter* mmap_limiter)
      : mmap_base_(mmap_base),
        length_(length),
        mmap_limiter_(mmap_limiter),
        filename_(std::move(filename)) {}

  ~PosixMmapReadableFile() override {
    ::munmap(static_cast<void*>(mmap_base_), length_);
    mmap_limiter_->Release();
  }

  Status Read(uint64_t offset, size_t n, Slice* result,
              char* scratch) const override {
    if (offset + n > length_) {
      *result = Slice();
      return PosixError(filename_, EINVAL);
    }

    *result = Slice(mmap_base_ + offset, n);
    return Status::OK();
  }

 private:
  char* const mmap_base_;
  const size_t length_;
  Limiter* const mmap_limiter_;
  const std::string filename_;
};

class RDMAWritableFile final : public WritableFile {
 public:
  RDMAWritableFile(SST_Metadata* sst_meta,
                    RDMA_Manager* rdma_mg)
      : pos_(0),
        chunk_offset(0),
        sst_meta_head(sst_meta),
        rdma_mg_(rdma_mg) {
// when open writeable file, get the read lock for the file.
    const std::shared_lock<std::shared_mutex> lock(
        sst_meta_head->file_lock);// write lock
//  const std::lock_guard<std::mutex> lock(sst_meta_head->file_lock);
    chunk_offset = sst_meta->file_size;
    sst_meta_current = sst_meta;

    while (chunk_offset >= rdma_mg_->Table_Size){
      chunk_offset -= rdma_mg_->Table_Size;
      sst_meta_current = sst_meta_current->next_ptr;
    }
    assert(chunk_offset < rdma_mg_->Table_Size);
    if(!rdma_mg_->Mempool_exist("write"))
      rdma_mg->Mempool_initialize(std::string("write"), kWritableFileBufferSize);

    rdma_mg_->Allocate_Local_RDMA_Slot(local_mr, std::string("write"));
    buf_ = static_cast<char*>(local_mr->addr);
  }

  ~RDMAWritableFile() override {
  }

  Status Append(const Slice& data) override {
    size_t write_size = data.size();
    const char* write_data = data.data();

    // Fit as much as possible into buffer.
    size_t copy_size = std::min(write_size, kWritableFileBufferSize - pos_);
    std::memcpy(buf_ + pos_, write_data, copy_size);
    write_data += copy_size;
    write_size -= copy_size;
    pos_ += copy_size;
    if (write_size == 0) {
      return Status::OK();
    }

    // Can't fit in buffer, so need to do at least one write.
    Status status = FlushBuffer();
    if (!status.ok()) {
      return status;
    }

    // Small writes go to buffer, large writes are written directly.
    if (write_size < kWritableFileBufferSize) {
      std::memcpy(buf_, write_data, write_size);
      pos_ = write_size;
      return Status::OK();
    }
    Slice to_write = Slice(write_data, write_size);
    return Direct_Write(to_write);
  }

  Status Close() override {
    Status status = FlushBuffer();
    return status;
  }

  Status Flush() override { return FlushBuffer(); }

  Status Sync() override {
    // Ensure new files referred to by the manifest are in the filesystem.
    //
    // This needs to happen before the manifest file is flushed to disk, to
    // avoid crashing in a state where the manifest refers to files that are not
    // yet on disk.
    return Status::OK();
  }

 private:
  Status FlushBuffer() {
    Status status = FlushBuffer_Inner(pos_);
    pos_ = 0;
    return status;
  }

  Status FlushBuffer_Inner(size_t msg_size) {
    const std::unique_lock<std::shared_mutex> lock(
        sst_meta_head->file_lock);
    assert(msg_size <= rdma_mg_->name_to_size.at("write"));
    ibv_mr remote_mr = {}; //
    remote_mr = *(sst_meta_current->mr);
    remote_mr.addr = static_cast<void*>(static_cast<char*>(sst_meta_current->mr->addr) + chunk_offset);
//  std::string thread_id = *(static_cast<std::string*>(rdma_mg_->t_local_1->Get()));
    std::string thread_id;
    //  auto start = std::chrono::high_resolution_clock::now();
    Status s = Status::OK();
    assert(msg_size <= rdma_mg_->name_to_size.at("write"));
//  std::cout << "Write data to " << sst_meta_head->fname << " " << sst_meta_current->mr->addr << " offset: "
//            << chunk_offset << "size: " << msg_size << std::endl;
    int flag;
    if (chunk_offset + msg_size >= rdma_mg_->Table_Size){
      // if block write accross two SSTable chunks, seperate it into 2 steps.
      //First step
      size_t first_half = rdma_mg_->Table_Size - chunk_offset;
      size_t second_half = msg_size - (rdma_mg_->Table_Size - chunk_offset);
      ibv_mr temp_mr = *(local_mr);
      flag = rdma_mg_->RDMA_Write(&remote_mr, local_mr, first_half,
                                  thread_id,IBV_SEND_SIGNALED,1);
      temp_mr.addr = static_cast<void*>(static_cast<char*>(local_mr->addr) + first_half);

      if (flag!=0){

        return Status::IOError("While appending to file", sst_meta_head->fname);


      }
      // move the buffer pointer.
      // Second step, create a new SSTable chunk and new sst_metadata, append it to the file
      // chunk list. then write the second part on it.
      SST_Metadata* new_sst;
      rdma_mg_->Allocate_Remote_RDMA_Slot(sst_meta_head->fname, new_sst);
      new_sst->last_ptr = sst_meta_current;
//    std::cout << "write blocks cross Table chunk" << std::endl;
      assert(sst_meta_current->next_ptr == nullptr);
      sst_meta_current->next_ptr = new_sst;
      sst_meta_current = new_sst;
      remote_mr = *(sst_meta_current->mr);
      chunk_offset = 0;
      flag = rdma_mg_->RDMA_Write(&remote_mr, &temp_mr, second_half,
                                  thread_id, IBV_SEND_SIGNALED,1);
      if (flag!=0){

        return Status::IOError("While appending to file", sst_meta_head->fname);


      }
      remote_mr.addr = static_cast<void*>(static_cast<char*>(remote_mr.addr) + second_half);
      chunk_offset = second_half;
    }
    else{
      // append the whole size.
      flag =
          rdma_mg_->RDMA_Write(&remote_mr, local_mr, msg_size, thread_id, IBV_SEND_SIGNALED,1);

      remote_mr.addr = static_cast<void*>(static_cast<char*>(remote_mr.addr) + msg_size);
      chunk_offset += msg_size;
//    std::cout << "write blocks within Table chunk" << std::endl;
      if (flag!=0){

        return Status::IOError("While appending to file", sst_meta_head->fname);


      }
    }

//  sst_meta_->mr->addr = static_cast<void*>(static_cast<char*>(sst_meta_->mr->addr) + nbytes);
//
    sst_meta_head->file_size += msg_size;
//  assert(sst_meta_head->file_size <= rdma_mg_->Table_Size);
//  auto stop = std::chrono::high_resolution_clock::now();
//  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
//  std::cout << size <<"Write inner chunk time elapse:" << duration.count() << std::endl;
    return s;
  }
  Status Direct_Write(const Slice& data) {

//  auto start = std::chrono::high_resolution_clock::now();
    const std::unique_lock<std::shared_mutex> lock(
        sst_meta_head->file_lock);// write lock
//  auto stop = std::chrono::high_resolution_clock::now();
//  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//  printf("Write Get the lock time elapse: %ld\n", duration.count());
//  const std::lock_guard<std::mutex> lock(sst_meta_head->file_lock);
//  const std::lock_guard<std::mutex> lock(
//      rdma_mg_->remote_mem_mutex);// write lock
//  auto myid = std::this_thread::get_id();
//  std::stringstream ss;
//  ss << myid;
//  std::string posix_tid = ss.str();
//  start = std::chrono::high_resolution_clock::now();

    const char* src = data.data();
    size_t nbytes = data.size();
    char* chunk_src = const_cast<char*>(src);
//  std::cout << "Old Write data to " << sst_meta_head->fname << " " << sst_meta_current->mr->addr << " offset: "
//            << chunk_offset << "size: " << nbytes << std::endl;
    Status s = Status::OK();
    ibv_mr* local_mr_pointer = nullptr;
    ibv_mr remote_mr = {}; //
    remote_mr = *(sst_meta_current->mr);
    remote_mr.addr = static_cast<void*>(static_cast<char*>(sst_meta_current->mr->addr) + chunk_offset);
//  stop = std::chrono::high_resolution_clock::now();
//  duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//  printf("Write Set up ibv_mr pointer time elapse: %ld\n", duration.count());
//  start = std::chrono::high_resolution_clock::now();
    rdma_mg_->Allocate_Local_RDMA_Slot(local_mr_pointer, std::string("write"));
//  stop = std::chrono::high_resolution_clock::now();
//  duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//  printf("Write Memory allocate, time elapse: %ld\n", duration.count());
//  start = std::chrono::high_resolution_clock::now();
    std::string thread_id;
    while (nbytes > rdma_mg_->name_to_size.at("write")){
      Append_chunk(chunk_src, rdma_mg_->name_to_size.at("write"), local_mr_pointer, remote_mr,
                   thread_id);
//                 *(static_cast<std::string*>(rdma_mg_->t_local_1->Get())));
      nbytes -= rdma_mg_->name_to_size.at("write");

    }
    Append_chunk(chunk_src, nbytes, local_mr_pointer, remote_mr,
                 thread_id);
//               *(static_cast<std::string*>(rdma_mg_->t_local_1->Get())));
//  stop = std::chrono::high_resolution_clock::now();
//  duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//  printf("RDMA Write size: %zu time elapse: %ld\n", data.size(), duration.count());
//  start = std::chrono::high_resolution_clock::now();
    if(rdma_mg_->Deallocate_Local_RDMA_Slot(local_mr_pointer->addr,
                                            std::string("write")))
      delete local_mr_pointer;
    else
      s = Status::IOError(
          "While RDMA Local Buffer Deallocate failed ",
          sst_meta_head->fname);
//  stop = std::chrono::high_resolution_clock::now();
//  duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//  printf("Write Local buffer deallocate time elapse: %ld\n", duration.count());
//  auto stop = std::chrono::high_resolution_clock::now();
//  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
//  std::cout << data.size() <<"Write total time elapse:" << duration.count() << std::endl;
    return s;
  }
// make sure the local buffer can hold the transferred data if not then send it by multiple times.
  Status Append_chunk(char*& buff_ptr, size_t size,
                                          ibv_mr* local_mr_pointer,
                                          ibv_mr& remote_mr,
                                          std::string& thread_id) {
//  auto start = std::chrono::high_resolution_clock::now();
    Status s = Status::OK();
    assert(size <= rdma_mg_->name_to_size.at("write"));
    int flag;
    if (chunk_offset + size >= rdma_mg_->Table_Size){
      // if block write accross two SSTable chunks, seperate it into 2 steps.
      //First step
      size_t first_half = rdma_mg_->Table_Size - chunk_offset;
      size_t second_half = size - (rdma_mg_->Table_Size - chunk_offset);
      memcpy(local_mr_pointer->addr, buff_ptr, first_half);
      flag = rdma_mg_->RDMA_Write(&remote_mr, local_mr_pointer, first_half,
                                  thread_id, IBV_SEND_SIGNALED,1);
      if (flag!=0){

        return Status::IOError("While appending to file", sst_meta_head->fname);


      }
      buff_ptr +=  first_half;
      // move the buffer pointer.
      // Second step, create a new SSTable chunk and new sst_metadata, append it to the file
      // chunk list. then write the second part on it.
      SST_Metadata* new_sst;
      rdma_mg_->Allocate_Remote_RDMA_Slot(sst_meta_head->fname, new_sst);
      new_sst->last_ptr = sst_meta_current;
//    std::cout << "write blocks cross Table chunk" << std::endl;
      assert(sst_meta_current->next_ptr == nullptr);
      sst_meta_current->next_ptr = new_sst;
      sst_meta_current = new_sst;
      remote_mr = *(sst_meta_current->mr);
      chunk_offset = 0;
      memcpy(local_mr_pointer->addr, buff_ptr, second_half);
      flag = rdma_mg_->RDMA_Write(&remote_mr, local_mr_pointer, second_half,
                                  thread_id, IBV_SEND_SIGNALED,1);
      if (flag!=0){

        return Status::IOError("While appending to file", sst_meta_head->fname);


      }
      remote_mr.addr = static_cast<void*>(static_cast<char*>(remote_mr.addr) + second_half);
      chunk_offset = second_half;
      buff_ptr += second_half;
    }
    else{
      // append the whole size.
//    auto start = std::chrono::high_resolution_clock::now();
      memcpy(local_mr_pointer->addr, buff_ptr, size);
//    auto stop = std::chrono::high_resolution_clock::now();
//    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//    printf("Write Memcopy size: %zu time elapse: %ld\n", size, duration.count());
      //  stop = std::chrono::high_resolution_clock::now();
//  duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//  printf("Write Local buffer deallocate time elapse: %ld\n", duration.count());
//    start = std::chrono::high_resolution_clock::now();
      flag =
          rdma_mg_->RDMA_Write(&remote_mr, local_mr_pointer, size, thread_id, IBV_SEND_SIGNALED,1);
//    stop = std::chrono::high_resolution_clock::now();
//    duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//    printf("Bare RDMA write size: %zu time elapse: %ld\n", size, duration.count());
      //  stop = std::chrono::high_resolution_clock::now();
//  duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//  printf("Write Local buffer deallocate time elapse: %ld\n", duration.count());
      remote_mr.addr = static_cast<void*>(static_cast<char*>(remote_mr.addr) + size);
      chunk_offset += size;
      buff_ptr += size;
//    std::cout << "write blocks within Table chunk" << std::endl;
      if (flag!=0){

        return Status::IOError("While appending to file", sst_meta_head->fname);


      }
    }

//  sst_meta_->mr->addr = static_cast<void*>(static_cast<char*>(sst_meta_->mr->addr) + nbytes);
//
    sst_meta_head->file_size += size;
//  assert(sst_meta_head->file_size <= rdma_mg_->Table_Size);
//  auto stop = std::chrono::high_resolution_clock::now();
//  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
//  std::cout << size <<"Write inner chunk time elapse:" << duration.count() << std::endl;
    return s;
  }
  Status SyncDirIfManifest() {
    return Status::OK();
  }

  // Ensures that all the caches associated with the given file descriptor's
  // data are flushed all the way to durable media, and can withstand power
  // failures.
  //
  // The path argument is only used to populate the description string in the
  // returned Status if an error occurs.
  static Status SyncFd(int fd, const std::string& fd_path) {
    return Status::OK();
  }


  // buf_[0, pos_ - 1] contains data to be written to fd_.
  char* buf_;
  ibv_mr* local_mr;
  size_t pos_;
  size_t chunk_offset;
  size_t logical_sector_size_;
  SST_Metadata* sst_meta_head;
  SST_Metadata* sst_meta_current;
  RDMA_Manager* rdma_mg_;
};
//class RDMAWritableFile final : public WritableFile {
// public:
//  RDMAWritableFile(std::string filename, int fd)
//      : pos_(0),
//        fd_(fd),
//        is_manifest_(IsManifest(filename)),
//        filename_(std::move(filename)),
//        dirname_(Dirname(filename_)) {}
//
//  RDMAWritableFile) override {
//    if (fd_ >= 0) {
//      // Ignoring any potential errors
//      Close();
//    }
//  }
//
//  Status Append(const Slice& data) override {
//    size_t write_size = data.size();
//    const char* write_data = data.data();
//
//    // Fit as much as possible into buffer.
//    size_t copy_size = std::min(write_size, kWritableFileBufferSize - pos_);
//    std::memcpy(buf_ + pos_, write_data, copy_size);
//    write_data += copy_size;
//    write_size -= copy_size;
//    pos_ += copy_size;
//    if (write_size == 0) {
//      return Status::OK();
//    }
//
//    // Can't fit in buffer, so need to do at least one write.
//    Status status = FlushBuffer();
//    if (!status.ok()) {
//      return status;
//    }
//
//    // Small writes go to buffer, large writes are written directly.
//    if (write_size < kWritableFileBufferSize) {
//      std::memcpy(buf_, write_data, write_size);
//      pos_ = write_size;
//      return Status::OK();
//    }
//    return FlushBuffer_Inner(write_data, write_size);
//  }
//
//  Status Close() override {
//    Status status = FlushBuffer();
//    const int close_result = ::close(fd_);
//    if (close_result < 0 && status.ok()) {
//      status = PosixError(filename_, errno);
//    }
//    fd_ = -1;
//    return status;
//  }
//
//  Status Flush() override { return FlushBuffer(); }
//
//  Status Sync() override {
//    // Ensure new files referred to by the manifest are in the filesystem.
//    //
//    // This needs to happen before the manifest file is flushed to disk, to
//    // avoid crashing in a state where the manifest refers to files that are not
//    // yet on disk.
//    Status status = SyncDirIfManifest();
//    if (!status.ok()) {
//      return status;
//    }
//
//    status = FlushBuffer();
//    if (!status.ok()) {
//      return status;
//    }
//
//    return SyncFd(fd_, filename_);
//  }
//
// private:
//  Status FlushBuffer() {
//    Status status = FlushBuffer_Inner(buf_, pos_);
//    pos_ = 0;
//    return status;
//  }
//
//  Status FlushBuffer_Inner(const char* data, size_t size) {
//    while (size > 0) {
//      ssize_t write_result = ::write(fd_, data, size);
//      if (write_result < 0) {
//        if (errno == EINTR) {
//          continue;  // Retry
//        }
//        return PosixError(filename_, errno);
//      }
//      data += write_result;
//      size -= write_result;
//    }
//    return Status::OK();
//  }
//
//  Status SyncDirIfManifest() {
//    Status status;
//    if (!is_manifest_) {
//      return status;
//    }
//
//    int fd = ::open(dirname_.c_str(), O_RDONLY | kOpenBaseFlags);
//    if (fd < 0) {
//      status = PosixError(dirname_, errno);
//    } else {
//      status = SyncFd(fd, dirname_);
//      ::close(fd);
//    }
//    return status;
//  }
//
//  // Ensures that all the caches associated with the given file descriptor's
//  // data are flushed all the way to durable media, and can withstand power
//  // failures.
//  //
//  // The path argument is only used to populate the description string in the
//  // returned Status if an error occurs.
//  static Status SyncFd(int fd, const std::string& fd_path) {
//#if HAVE_FULLFSYNC
//    // On macOS and iOS, fsync() doesn't guarantee durability past power
//    // failures. fcntl(F_FULLFSYNC) is required for that purpose. Some
//    // filesystems don't support fcntl(F_FULLFSYNC), and require a fallback to
//    // fsync().
//    if (::fcntl(fd, F_FULLFSYNC) == 0) {
//      return Status::OK();
//    }
//#endif  // HAVE_FULLFSYNC
//
//#if HAVE_FDATASYNC
//    bool sync_success = ::fdatasync(fd) == 0;
//#else
//    bool sync_success = ::fsync(fd) == 0;
//#endif  // HAVE_FDATASYNC
//
//    if (sync_success) {
//      return Status::OK();
//    }
//    return PosixError(fd_path, errno);
//  }
//
//  // Returns the directory name in a path pointing to a file.
//  //
//  // Returns "." if the path does not contain any directory separator.
//  static std::string Dirname(const std::string& filename) {
//    std::string::size_type separator_pos = filename.rfind('/');
//    if (separator_pos == std::string::npos) {
//      return std::string(".");
//    }
//    // The filename component should not contain a path separator. If it does,
//    // the splitting was done incorrectly.
//    assert(filename.find('/', separator_pos + 1) == std::string::npos);
//
//    return filename.substr(0, separator_pos);
//  }
//
//  // Extracts the file name from a path pointing to a file.
//  //
//  // The returned Slice points to |filename|'s data buffer, so it is only valid
//  // while |filename| is alive and unchanged.
//  static Slice Basename(const std::string& filename) {
//    std::string::size_type separator_pos = filename.rfind('/');
//    if (separator_pos == std::string::npos) {
//      return Slice(filename);
//    }
//    // The filename component should not contain a path separator. If it does,
//    // the splitting was done incorrectly.
//    assert(filename.find('/', separator_pos + 1) == std::string::npos);
//
//    return Slice(filename.data() + separator_pos + 1,
//                 filename.length() - separator_pos - 1);
//  }
//
//  // True if the given file is a manifest file.
//  static bool IsManifest(const std::string& filename) {
//    return Basename(filename).starts_with("MANIFEST");
//  }
//
//  // buf_[0, pos_ - 1] contains data to be written to fd_.
//  char buf_[kWritableFileBufferSize];
//  size_t pos_;
//  int fd_;
//
//  const bool is_manifest_;  // True if the file's name starts with MANIFEST.
//  const std::string filename_;
//  const std::string dirname_;  // The directory of filename_.
//};

int LockOrUnlock(int fd, bool lock) {
  errno = 0;
  struct ::flock file_lock_info;
  std::memset(&file_lock_info, 0, sizeof(file_lock_info));
  file_lock_info.l_type = (lock ? F_WRLCK : F_UNLCK);
  file_lock_info.l_whence = SEEK_SET;
  file_lock_info.l_start = 0;
  file_lock_info.l_len = 0;  // Lock/unlock entire file.
  return ::fcntl(fd, F_SETLK, &file_lock_info);
}

// Instances are thread-safe because they are immutable.
class PosixFileLock : public FileLock {
 public:
  PosixFileLock(int fd, std::string filename)
      : fd_(fd), filename_(std::move(filename)) {}

  int fd() const { return fd_; }
  const std::string& filename() const { return filename_; }

 private:
  const int fd_;
  const std::string filename_;
};

// Tracks the files locked by PosixEnv::LockFile().
//
// We maintain a separate set instead of relying on fcntl(F_SETLK) because
// fcntl(F_SETLK) does not provide any protection against multiple uses from the
// same process.
//
// Instances are thread-safe because all member data is guarded by a mutex.
class PosixLockTable {
 public:
  bool Insert(const std::string& fname) LOCKS_EXCLUDED(mu_) {
    mu_.Lock();
    bool succeeded = locked_files_.insert(fname).second;
    mu_.Unlock();
    return succeeded;
  }
  void Remove(const std::string& fname) LOCKS_EXCLUDED(mu_) {
    mu_.Lock();
    locked_files_.erase(fname);
    mu_.Unlock();
  }

 private:
  port::Mutex mu_;
  std::set<std::string> locked_files_ GUARDED_BY(mu_);
};

class PosixEnv : public Env {
 public:
  PosixEnv();
  ~PosixEnv() override {
    fs_meta_save();
    delete Remote_Bitmap;
//  delete Write_Bitmap;
//  delete Read_Bitmap;
    delete rdma_mg;
  }
  enum Open_Type {readtype, write_reopen, write_new, rwtype};
  bool RDMA_Rename(const std::string &new_name, const std::string &old_name){
    std::shared_lock<std::shared_mutex> read_lock(fs_mutex);
    auto entry = file_to_sst_meta.find(old_name);
    if(file_to_sst_meta.find(new_name) != file_to_sst_meta.end()){
      read_lock.unlock();
      RDMA_Delete_File(new_name);
      read_lock.lock();
    }

    if (entry == file_to_sst_meta.end()) {
      std::cout << "File rename did not find the old name" <<std::endl;
      return false;
    } else {
      auto const value = std::move(entry->second);
      read_lock.unlock();
      std::unique_lock<std::shared_mutex> write_lock(fs_mutex);
      file_to_sst_meta.erase(entry);
      file_to_sst_meta.insert({new_name, std::move(value)});
      return true;
    }

  }
  // Delete a file in rdma file system, return 1 mean success, return 0 mean
  //did not find the key in the map, 2 means find keys in map larger than 1.
  int RDMA_Delete_File(const std::string& fname){
    //First find out the meta_data pointer, then search in the map weather there are
    // other file name link to the same file. If it is the last one, unpin the region in the
    // remote buffer pool.
    std::unique_lock<std::shared_mutex> write_lock(fs_mutex);
    SST_Metadata* file_meta = file_to_sst_meta.at(fname);
    int erasenum = file_to_sst_meta.erase(fname);// delete this file name
    if (erasenum==0) return 0; // the file name should only have one entry
//    else if (erasenum>1) return 2;
    auto ptr = file_to_sst_meta.begin();
    while(ptr != file_to_sst_meta.end())
      // check whether there is other filename link to the same file
    {
      // Check if value of this entry matches with given value
      if(ptr->second == file_meta)
        return 2;// if find then return.
      // Go to next entry in map
      ptr++;
    }
    //if not find other filename, then make the all the remote memory slots
    // related to that file not in use (delete the file)

    // delete remove the flage sucessfully
    SST_Metadata* next_file_meta;
    while (file_meta->next_ptr != nullptr){
      next_file_meta = file_meta->next_ptr;
      rdma_mg->Deallocate_Remote_RDMA_Slot(file_meta);

      delete file_meta->mr;
      delete file_meta;
      file_meta = next_file_meta;
    }
    rdma_mg->Deallocate_Remote_RDMA_Slot(file_meta);
    delete file_meta->mr;
    delete file_meta;
    return 1;



  }

  Status RDMA_open(const std::string& file_name, SST_Metadata*& sst_meta, Open_Type type){
    //For write_reopen and read type if not found in the mapping table, then
    //it is an error.
    //for write_new type, if there is one file existed with the same name, then overwrite it.otherwise
    //create a new one
    //for read&write type, try to find in the map table first, if missing then create a
    //new one
    if(type == write_new){
      std::unique_lock<std::shared_mutex> write_lock(fs_mutex);
      if (file_to_sst_meta.find(file_name) == file_to_sst_meta.end()) {
        // std container always copy the value to the container, Don't worry.
        write_lock.unlock();
        // temporarily release the lock to avoid deadlock.
        rdma_mg->Allocate_Remote_RDMA_Slot(file_name, sst_meta);
        write_lock.lock();
        file_to_sst_meta[file_name] = sst_meta;
//        write_lock.unlock();
//        fs_meta_save();
        return Status::OK();
      } else {
        //Rewrite the file
        file_to_sst_meta[file_name]->file_size = 0;// truncate the existing file (need concurrency control)

        if(file_to_sst_meta[file_name]->next_ptr != nullptr){
          SST_Metadata* file_meta = file_to_sst_meta[file_name]->next_ptr;
          SST_Metadata* next_file_meta;
          while (file_meta->next_ptr != nullptr){
            next_file_meta = file_meta->next_ptr;
            rdma_mg->Deallocate_Remote_RDMA_Slot(file_meta);
            delete file_meta->mr;
            delete file_meta;
            file_meta = next_file_meta;
          }
          rdma_mg->Deallocate_Remote_RDMA_Slot(file_meta);
          delete file_meta->mr;
          delete file_meta;
        }

        file_to_sst_meta[file_name]->next_ptr = nullptr;
        sst_meta = file_to_sst_meta[file_name];
//        write_lock.unlock();
//        fs_meta_save();
        return Status::OK();      }
    }
    if(type == rwtype) {
      std::unique_lock<std::shared_mutex> write_lock(fs_mutex);
      if (file_to_sst_meta.find(file_name) == file_to_sst_meta.end()) {
        // std container always copy the value to the container, Don't worry.
        write_lock.unlock();
        rdma_mg->Allocate_Remote_RDMA_Slot(file_name, sst_meta);
        write_lock.lock();
        file_to_sst_meta[file_name] = sst_meta;
//        write_lock.unlock();
//        fs_meta_save();
        return Status::OK();
      } else {
        sst_meta = file_to_sst_meta[file_name];
        return Status::OK();
      }
    }
    if(type == write_reopen || type == readtype){
      std::shared_lock<std::shared_mutex> read_lock(fs_mutex);
      if (file_to_sst_meta.find(file_name) == file_to_sst_meta.end()) {
        // std container always copy the value to the container, Don't worry.
//        errno = ENOENT;
        return Status::IOError("While open a file for random read", "");
      } else {
        sst_meta = file_to_sst_meta[file_name];
        return Status::OK();
      }
    }
    return Status::NotFound("file open not found", "");
  }

  Status NewSequentialFile_RDMA(const std::string& filename,
                           SequentialFile** result) override {
    Status s = Status::OK();
    SST_Metadata* meta_data = nullptr;
    s = RDMA_open(filename, meta_data, readtype);
    if (!s.ok())
      std::cerr << s.ToString() << std::endl;
    if (!s.ok()){
      printf("Sequential file open not found");
      return s;
    }else{
      *result = (new RDMASequentialFile(meta_data, rdma_mg));
      return s;
    }

  }
//  Status NewSequentialFile_RDMA(const std::string& filename,
//                           SequentialFile** result) override {
//    int fd = ::open(filename.c_str(), O_RDONLY | kOpenBaseFlags);
//    if (fd < 0) {
//      *result = nullptr;
//      return PosixError(filename, errno);
//    }
//
//    *result = new RDMASequentialFile(filename, fd);
//    return Status::OK();
//  }
  Status NewRandomAccessFile_RDMA(const std::string& filename,
                             RandomAccessFile** result) override {
    Status s = Status::OK();
    SST_Metadata* meta_data = nullptr;
    RDMA_open(filename, meta_data, readtype);
    if (!s.ok())
      std::cerr << s.ToString() << std::endl;
    if (!s.ok()){
      printf("RandomAccess file open not found");
      return s;
    }else{
      *result = new RDMARandomAccessFile(meta_data, rdma_mg);
      return s;
    }

  }
//  Status NewRandomAccessFile_RDMA(const std::string& filename,
//                             RandomAccessFile** result) override {
//    *result = nullptr;
//    int fd = ::open(filename.c_str(), O_RDONLY | kOpenBaseFlags);
//    if (fd < 0) {
//      return PosixError(filename, errno);
//    }
//
//    if (!mmap_limiter_.Acquire()) {
//      *result = new RDMARandomAccessFile(filename, fd, &fd_limiter_);
//      return Status::OK();
//    }
//
//    uint64_t file_size;
//    Status status = GetFileSize(filename, &file_size);
//    if (status.ok()) {
//      void* mmap_base =
//          ::mmap(/*addr=*/nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
//      if (mmap_base != MAP_FAILED) {
//        *result = new PosixMmapReadableFile(filename,
//                                            reinterpret_cast<char*>(mmap_base),
//                                            file_size, &mmap_limiter_);
//      } else {
//        status = PosixError(filename, errno);
//      }
//    }
//    ::close(fd);
//    if (!status.ok()) {
//      mmap_limiter_.Release();
//    }
//    return status;
//  }
  Status NewWritableFile(const std::string& filename,
                         WritableFile** result) override {
    Status s;
    Open_Type type = (write_new);
    SST_Metadata* meta_data = nullptr;
    s = RDMA_open(filename, meta_data, type);
    if (!s.ok())
      std::cerr << s.ToString() << std::endl;
    if (!s.ok()){
      printf("Writable file open not found");
      return s;
    }else{
      *result = new RDMAWritableFile(meta_data, rdma_mg);
      return s;
    }

  }

  Status NewAppendableFile(const std::string& filename,
                           WritableFile** result) override {
    Status s;
    Open_Type type = (write_reopen);
    SST_Metadata* meta_data = nullptr;
    RDMA_open(filename, meta_data, type);
    if (!s.ok())
      std::cerr << s.ToString() << std::endl;
    if (!s.ok()){
      printf("Writable file open not found");
      return s;
    }else{
      *result = new RDMAWritableFile(meta_data, rdma_mg);
      return s;
    }
  }

  bool FileExists(const std::string& filename) override {
    if(file_to_sst_meta.find(filename) != file_to_sst_meta.end())
      return true;
    return ::access(filename.c_str(), F_OK) == 0;
  }

  Status GetChildren(const std::string& directory_path,
                     std::vector<std::string>* result) override {
    result->clear();
    // First search in the remote memory
    auto iter = file_to_sst_meta.begin();
    while (iter != file_to_sst_meta.end() ) {
      std::string s = iter->first;
      // Find position of ':' using find()
      int pos = s.find_last_of("/");

      // Copy substring after pos
      std::string sub = s.substr(pos + 1);
      result->push_back(sub);
      iter++;
    }
    //Then search in disk file system
    ::DIR* dir = ::opendir(directory_path.c_str());
    if (dir == nullptr) {
      return PosixError(directory_path, errno);
    }
    struct ::dirent* entry;
    while ((entry = ::readdir(dir)) != nullptr) {
      result->emplace_back(entry->d_name);
    }
    ::closedir(dir);
    return Status::OK();
  }

  Status RemoveFile(const std::string& filename) override {
    Status result = Status::OK();
    if (::unlink(filename.c_str()) == 0) {
      return result;
    }
    else{
      // Otherwise it is a RDMA file, delete it through the RDMA file delete.
      if (RDMA_Delete_File(filename)==0){
        result = Status::IOError("while RDMA unlink() file, error occur", std::to_string(errno));
      }
      else result = Status::OK();
      return result;
    }

  }

  Status CreateDir(const std::string& dirname) override {
    if (::mkdir(dirname.c_str(), 0755) != 0) {
      return PosixError(dirname, errno);
    }
    return Status::OK();
  }

  Status RemoveDir(const std::string& dirname) override {
    if (::rmdir(dirname.c_str()) != 0) {
      return PosixError(dirname, errno);
    }
    return Status::OK();
  }

  Status GetFileSize(const std::string& filename, uint64_t* size) override {
    struct ::stat file_stat;
    if (::stat(filename.c_str(), &file_stat) == 0) {
      *size = file_stat.st_size;
    }
    else if (file_to_sst_meta.find(filename) != file_to_sst_meta.end()){
      SST_Metadata* meta = file_to_sst_meta[filename];
      *size = meta->file_size;
    }
    else
      return Status::IOError("while stat a file for size", std::to_string(errno));
    return Status::OK();
  }

  Status RenameFile(const std::string& from, const std::string& to) override {
    if (std::rename(from.c_str(), to.c_str()) != 0 && !RDMA_Rename(to.c_str(), from.c_str())) {
      return PosixError(from, errno);
    }
    return Status::OK();
  }

  Status LockFile(const std::string& filename, FileLock** lock) override {
    *lock = nullptr;

    int fd = ::open(filename.c_str(), O_RDWR | O_CREAT | kOpenBaseFlags, 0644);
    if (fd < 0) {
      return PosixError(filename, errno);
    }

    if (!locks_.Insert(filename)) {
      ::close(fd);
      return Status::IOError("lock " + filename, "already held by process");
    }

    if (LockOrUnlock(fd, true) == -1) {
      int lock_errno = errno;
      ::close(fd);
      locks_.Remove(filename);
      return PosixError("lock " + filename, lock_errno);
    }

    *lock = new PosixFileLock(fd, filename);
    return Status::OK();
  }

  Status UnlockFile(FileLock* lock) override {
    PosixFileLock* posix_file_lock = static_cast<PosixFileLock*>(lock);
    if (LockOrUnlock(posix_file_lock->fd(), false) == -1) {
      return PosixError("unlock " + posix_file_lock->filename(), errno);
    }
    locks_.Remove(posix_file_lock->filename());
    ::close(posix_file_lock->fd());
    delete posix_file_lock;
    return Status::OK();
  }

  void Schedule(void (*background_work_function)(void* background_work_arg),
                void* background_work_arg) override;

  void StartThread(void (*thread_main)(void* thread_main_arg),
                   void* thread_main_arg) override {
    std::thread new_thread(thread_main, thread_main_arg);
    new_thread.detach();
  }

  Status GetTestDirectory(std::string* result) override {
    const char* env = std::getenv("TEST_TMPDIR");
    if (env && env[0] != '\0') {
      *result = env;
    } else {
      char buf[100];
      std::snprintf(buf, sizeof(buf), "/tmp/leveldbtest-%d",
                    static_cast<int>(::geteuid()));
      *result = buf;
    }

    // The CreateDir status is ignored because the directory may already exist.
    CreateDir(*result);

    return Status::OK();
  }

  Status NewLogger(const std::string& filename, Logger** result) override {
    int fd = ::open(filename.c_str(),
                    O_APPEND | O_WRONLY | O_CREAT | kOpenBaseFlags, 0644);
    if (fd < 0) {
      *result = nullptr;
      return PosixError(filename, errno);
    }

    std::FILE* fp = ::fdopen(fd, "w");
    if (fp == nullptr) {
      ::close(fd);
      *result = nullptr;
      return PosixError(filename, errno);
    } else {
      *result = new PosixLogger(fp);
      return Status::OK();
    }
  }

  uint64_t NowMicros() override {
    static constexpr uint64_t kUsecondsPerSecond = 1000000;
    struct ::timeval tv;
    ::gettimeofday(&tv, nullptr);
    return static_cast<uint64_t>(tv.tv_sec) * kUsecondsPerSecond + tv.tv_usec;
  }

  void SleepForMicroseconds(int micros) override {
    std::this_thread::sleep_for(std::chrono::microseconds(micros));
  }

 private:
  void BackgroundThreadMain();

  static void BackgroundThreadEntryPoint(PosixEnv* env) {
    env->BackgroundThreadMain();
  }

  // Stores the work item data in a Schedule() call.
  //
  // Instances are constructed on the thread calling Schedule() and used on the
  // background thread.
  //
  // This structure is thread-safe beacuse it is immutable.
  struct BackgroundWorkItem {
    explicit BackgroundWorkItem(void (*function)(void* arg), void* arg)
        : function(function), arg(arg) {}

    void (*const function)(void*);
    void* const arg;
  };

  port::Mutex background_work_mutex_;
  port::CondVar background_work_cv_ GUARDED_BY(background_work_mutex_);
  bool started_background_thread_ GUARDED_BY(background_work_mutex_);

  std::queue<BackgroundWorkItem> background_work_queue_
      GUARDED_BY(background_work_mutex_);

  PosixLockTable locks_;  // Thread-safe.
  Limiter mmap_limiter_;  // Thread-safe.
  Limiter fd_limiter_;    // Thread-safe.
};

// Return the maximum number of concurrent mmaps.
int MaxMmaps() { return g_mmap_limit; }

// Return the maximum number of read-only files to keep open.
int MaxOpenFiles() {
  if (g_open_read_only_file_limit >= 0) {
    return g_open_read_only_file_limit;
  }
  struct ::rlimit rlim;
  if (::getrlimit(RLIMIT_NOFILE, &rlim)) {
    // getrlimit failed, fallback to hard-coded default.
    g_open_read_only_file_limit = 50;
  } else if (rlim.rlim_cur == RLIM_INFINITY) {
    g_open_read_only_file_limit = std::numeric_limits<int>::max();
  } else {
    // Allow use of 20% of available file descriptors for read-only files.
    g_open_read_only_file_limit = rlim.rlim_cur / 5;
  }
  return g_open_read_only_file_limit;
}

}  // namespace

PosixEnv::PosixEnv()
    : background_work_cv_(&background_work_mutex_),
      started_background_thread_(false),
      mmap_limiter_(MaxMmaps()),
      fd_limiter_(MaxOpenFiles()) {
  struct config_t config = {
      NULL,  /* dev_name */
      NULL,  /* server_name */
      19875, /* tcp_port */
      1,	 /* ib_port */
      1, /* gid_idx */
      1024*1024*1024 /*initial local buffer size*/
  };

//  Write_Bitmap = new std::map<void*, In_Use_Array>;
//  Read_Bitmap = new std::map<void*, In_Use_Array>;
//  size_t read_block_size = 8*1024;
//  size_t write_block_size = 4*1024*1024;
  size_t table_size = 8*1024*1024;
  Remote_Bitmap = new std::map<void*, In_Use_Array>;
  rdma_mg = new RDMA_Manager(config, Remote_Bitmap, table_size, &db_name,
                             &file_to_sst_meta,
                             &fs_mutex);
  rdma_mg->Client_Set_Up_Resources();
}

void PosixEnv::Schedule(
    void (*background_work_function)(void* background_work_arg),
    void* background_work_arg) {
  background_work_mutex_.Lock();

  // Start the background thread, if we haven't done so already.
  if (!started_background_thread_) {
    started_background_thread_ = true;
    std::thread background_thread(PosixEnv::BackgroundThreadEntryPoint, this);
    background_thread.detach();
  }

  // If the queue is empty, the background thread may be waiting for work.
  if (background_work_queue_.empty()) {
    background_work_cv_.Signal();
  }

  background_work_queue_.emplace(background_work_function, background_work_arg);
  background_work_mutex_.Unlock();
}

void PosixEnv::BackgroundThreadMain() {
  while (true) {
    background_work_mutex_.Lock();

    // Wait until there is work to be done.
    while (background_work_queue_.empty()) {
      background_work_cv_.Wait();
    }

    assert(!background_work_queue_.empty());
    auto background_work_function = background_work_queue_.front().function;
    void* background_work_arg = background_work_queue_.front().arg;
    background_work_queue_.pop();

    background_work_mutex_.Unlock();
    background_work_function(background_work_arg);
  }
}

namespace {

// Wraps an Env instance whose destructor is never created.
//
// Intended usage:
//   using PlatformSingletonEnv = SingletonEnv<PlatformEnv>;
//   void ConfigurePosixEnv(int param) {
//     PlatformSingletonEnv::AssertEnvNotInitialized();
//     // set global configuration flags.
//   }
//   Env* Env::Default() {
//     static PlatformSingletonEnv default_env;
//     return default_env.env();
//   }
template <typename EnvType>
class SingletonEnv {
 public:
  SingletonEnv() {
#if !defined(NDEBUG)
    env_initialized_.store(true, std::memory_order::memory_order_relaxed);
#endif  // !defined(NDEBUG)
    static_assert(sizeof(env_storage_) >= sizeof(EnvType),
                  "env_storage_ will not fit the Env");
    static_assert(alignof(decltype(env_storage_)) >= alignof(EnvType),
                  "env_storage_ does not meet the Env's alignment needs");
    new (&env_storage_) EnvType();
  }
  ~SingletonEnv() = default;

  SingletonEnv(const SingletonEnv&) = delete;
  SingletonEnv& operator=(const SingletonEnv&) = delete;

  Env* env() { return reinterpret_cast<Env*>(&env_storage_); }

  static void AssertEnvNotInitialized() {
#if !defined(NDEBUG)
    assert(!env_initialized_.load(std::memory_order::memory_order_relaxed));
#endif  // !defined(NDEBUG)
  }

 private:
  typename std::aligned_storage<sizeof(EnvType), alignof(EnvType)>::type
      env_storage_;
#if !defined(NDEBUG)
  static std::atomic<bool> env_initialized_;
#endif  // !defined(NDEBUG)
};

#if !defined(NDEBUG)
template <typename EnvType>
std::atomic<bool> SingletonEnv<EnvType>::env_initialized_;
#endif  // !defined(NDEBUG)

using PosixDefaultEnv = SingletonEnv<PosixEnv>;

}  // namespace

void EnvPosixTestHelper::SetReadOnlyFDLimit(int limit) {
  PosixDefaultEnv::AssertEnvNotInitialized();
  g_open_read_only_file_limit = limit;
}

void EnvPosixTestHelper::SetReadOnlyMMapLimit(int limit) {
  PosixDefaultEnv::AssertEnvNotInitialized();
  g_mmap_limit = limit;
}

Env* Env::Default() {
  static PosixDefaultEnv env_container;
  return env_container.env();
}

}  // namespace leveldb
