// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_DB_IMPL_H_
#define STORAGE_LEVELDB_DB_DB_IMPL_H_

#include "db/dbformat.h"
#include "db/log_writer.h"
#include "db/snapshot.h"
#include <atomic>
#include <deque>
#include <set>
#include <string>

#include "leveldb/db.h"
#include "leveldb/env.h"

#include "port/port.h"
#include "port/thread_annotations.h"
#include "util/mutexlock.h"

#include "memtable_list.h"
#include "version_set.h"

namespace leveldb {

class MemTable;
class TableCache;
class Version;
class VersionEdit;
class VersionSet;
class MemTableList;
//TODO: make memtableversionlist and LSM versionset 's function integrated into
// Superversion.
struct SuperVersion {
  // Accessing members of this class is not thread-safe and requires external
  // synchronization (ie db mutex held or on write thread).
  MemTable* mem;
  MemTableListVersion* imm;
  Version* current;
  // Version number of the current SuperVersion
  uint64_t version_number;

  // should be called outside the mutex
  SuperVersion(MemTable* new_mem,
               MemTableListVersion* new_imm, Version* new_current);
  ~SuperVersion();
  SuperVersion* Ref();
  // If Unref() returns true, Cleanup() should be called with mutex held
  // before deleting this SuperVersion.
  void Unref();

  // call these two methods with db mutex held
  // Cleanup unrefs mem, imm and current. Also, it stores all memtables
  // that needs to be deleted in to_delete vector. Unrefing those
  // objects needs to be done in the mutex
  void Cleanup();
  void Init();

  // The value of dummy is not actually used. kSVInUse takes its address as a
  // mark in the thread local storage to indicate the SuperVersion is in use
  // by thread. This way, the value of kSVInUse is guaranteed to have no
  // conflict with SuperVersion object address and portable on different
  // platform.
  static int dummy;
  static void* const kSVInUse;
  static void* const kSVObsolete;

 private:
  std::atomic<uint32_t> refs;
  // We need to_delete because during Cleanup(), imm->Unref() returns
  // all memtables that we need to free through this vector. We then
  // delete all those memtables outside of mutex, during destruction
  autovector<MemTable*> to_delete;
};
// The structure for storing argument for thread pool.

class DBImpl : public DB {
 public:
  DBImpl(const Options& options, const std::string& dbname);

  DBImpl(const DBImpl&) = delete;
  DBImpl& operator=(const DBImpl&) = delete;

  ~DBImpl() override;

  // Implementations of the DB interface
  Status Put(const WriteOptions&, const Slice& key,
             const Slice& value) override;
  Status Delete(const WriteOptions&, const Slice& key) override;
  Status Write(const WriteOptions& options, WriteBatch* updates) override;
  Status Get(const ReadOptions& options, const Slice& key,
             std::string* value) override;
  Iterator* NewIterator(const ReadOptions&) override;
  const Snapshot* GetSnapshot() override;
  void ReleaseSnapshot(const Snapshot* snapshot) override;
  bool GetProperty(const Slice& property, std::string* value) override;
  void GetApproximateSizes(const Range* range, int n, uint64_t* sizes) override;
  void CompactRange(const Slice* begin, const Slice* end) override;

  // Extra methods (for testing) that are not in the public DB interface

  // Compact any files in the named level that overlap [*begin,*end]
  void TEST_CompactRange(int level, const Slice* begin, const Slice* end);

  // Force current memtable contents to be compacted.
//  Status TEST_CompactMemTable();

  // Return an internal iterator over the current state of the database.
  // The keys of this iterator are internal keys (see format.h).
  // The returned iterator should be deleted when no longer needed.
  Iterator* TEST_NewInternalIterator();

  // Return the maximum overlapping data (in bytes) at next level for any
  // file at a level >= 1.
  int64_t TEST_MaxNextLevelOverlappingBytes();

  // Record a sample of bytes read at the specified internal key.
  // Samples are taken approximately once every config::kReadBytesPeriod
  // bytes.
  void RecordReadSample(Slice key);

 private:
  friend class DB;
  struct CompactionState;
  struct SubcompactionState;
  struct Writer;

  // Information for a manual compaction
  struct ManualCompaction {
    int level;
    bool done;
    const InternalKey* begin;  // null means beginning of key range
    const InternalKey* end;    // null means end of key range
    InternalKey tmp_storage;   // Used to keep track of compaction progress
  };

  // Per level compaction stats.  stats_[level] stores the stats for
  // compactions that produced data for the specified "level".
  struct CompactionStats {
    CompactionStats() : micros(0), bytes_read(0), bytes_written(0) {}

    void Add(const CompactionStats& c) {
      this->micros += c.micros;
      this->bytes_read += c.bytes_read;
      this->bytes_written += c.bytes_written;
    }

    int64_t micros;
    int64_t bytes_read;
    int64_t bytes_written;
  };

  Iterator* NewInternalIterator(const ReadOptions&,
                                SequenceNumber* latest_snapshot,
                                uint32_t* seed);

  Status NewDB();

  // Recover the descriptor from persistent storage.  May do a significant
  // amount of work to recover recently logged updates.  Any changes to
  // be made to the descriptor are added to *edit.
  Status Recover(VersionEdit* edit, bool* save_manifest)
      EXCLUSIVE_LOCKS_REQUIRED(undefine_mutex);

  void MaybeIgnoreError(Status* s) const;

  // Delete any unneeded files and stale in-memory entries.
  void RemoveObsoleteFiles() EXCLUSIVE_LOCKS_REQUIRED(undefine_mutex);

  // Compact the in-memory write buffer to disk.  Switches to a new
  // log-file/memtable and writes a new descriptor iff successful.
  // Errors are recorded in bg_error_.
  void CompactMemTable() EXCLUSIVE_LOCKS_REQUIRED(undefine_mutex);

  Status RecoverLogFile(uint64_t log_number, bool last_log, bool* save_manifest,
                        VersionEdit* edit, SequenceNumber* max_sequence)
      EXCLUSIVE_LOCKS_REQUIRED(undefine_mutex);

  Status WriteLevel0Table(FlushJob* job, VersionEdit* edit)
      EXCLUSIVE_LOCKS_REQUIRED(undefine_mutex);
  Status WriteLevel0Table(MemTable* job, VersionEdit* edit, Version* base)
  EXCLUSIVE_LOCKS_REQUIRED(undefine_mutex);
  Status PickupTableToWrite(bool force, uint64_t seq_num, MemTable*& mem_r)
      EXCLUSIVE_LOCKS_REQUIRED(undefine_mutex);
  WriteBatch* BuildBatchGroup(Writer** last_writer)
      EXCLUSIVE_LOCKS_REQUIRED(undefine_mutex);

  void RecordBackgroundError(const Status& s);

  void MaybeScheduleFlushOrCompaction() EXCLUSIVE_LOCKS_REQUIRED(undefine_mutex);
  static void BGWork_Flush(void* thread_args);
  static void BGWork_Compaction(void* thread_args);
  void BackgroundCall();
  void BackgroundFlush(void* p);
  void BackgroundCompaction(void* p) EXCLUSIVE_LOCKS_REQUIRED(undefine_mutex);
  void CleanupCompaction(CompactionState* compact)
      EXCLUSIVE_LOCKS_REQUIRED(undefine_mutex);
  Status DoCompactionWork(CompactionState* compact)
      EXCLUSIVE_LOCKS_REQUIRED(undefine_mutex);
  void ProcessKeyValueCompaction(SubcompactionState* sub_compact);
  Status DoCompactionWorkWithSubcompaction(CompactionState* compact);
  Status OpenCompactionOutputFile(SubcompactionState* compact);
  Status OpenCompactionOutputFile(CompactionState* compact);
  Status FinishCompactionOutputFile(SubcompactionState* compact,
                                    Iterator* input);
  Status FinishCompactionOutputFile(CompactionState* compact, Iterator* input);
  Status InstallCompactionResults(CompactionState* compact,
                                  std::unique_lock<std::mutex>* lck_p)
      EXCLUSIVE_LOCKS_REQUIRED(undefine_mutex);
  Status TryInstallMemtableFlushResults(
      FlushJob* job, VersionSet* vset,
      std::shared_ptr<RemoteMemTableMetaData>& sstable, VersionEdit* edit);
  void InstallSuperVersion();
  const Comparator* user_comparator() const {
    return internal_comparator_.user_comparator();
  }

  // Constant after construction
  Env* const env_;
  const InternalKeyComparator internal_comparator_;
  const InternalFilterPolicy internal_filter_policy_;
  const Options options_;  // options_.comparator == &internal_comparator_
  const bool owns_info_log_;
  const bool owns_cache_;
  const std::string dbname_;

  // table_cache_ provides its own synchronization
  TableCache* const table_cache_;

  // Lock over the persistent DB state.  Non-null iff successfully acquired.
  FileLock* db_lock_;
  std::atomic<bool> mem_switching;
  int thread_ready_num;
  // State below is protected by undefine_mutex
  // we could rename it as superversion mutex
  port::Mutex undefine_mutex;
  std::mutex FlushPickMTX;
//  port::Mutex write_stall_mutex_;
//  SpinMutex spin_memtable_switch_mutex;
  std::atomic<bool> shutting_down_;
  std::condition_variable write_stall_cv GUARDED_BY(superversion_mtx);
  std::mutex superversion_mtx;
  bool locked = false;
//  SpinMutex LSMv_mtx;
  std::atomic<MemTable*> mem_;
//  std::atomic<MemTable*> imm_;  // Memtable being compacted
  MemTableList imm_;
  std::atomic<bool> has_imm_;         // So bg thread can detect non-null imm_
  WritableFile* logfile_;
  uint64_t logfile_number_;
  log::Writer* log_;
  uint32_t seed_;  // For sampling.

  // Queue of writers.
  std::deque<Writer*> writers_;
  WriteBatch* tmp_batch_;

  SnapshotList snapshots_;

  // Set of table files to protect from deletion because they are
  // part of ongoing compactions.
//  std::set<uint64_t> pending_outputs_;

  // Has a background compaction been scheduled or is running?
  bool background_compaction_scheduled_;

  ManualCompaction* manual_compaction_;

  VersionSet* const versions_;

  // Have we encountered a background error in paranoid mode?
  Status bg_error_;

  CompactionStats stats_[config::kNumLevels];
  std::atomic<size_t> memtable_counter = 0;
  std::atomic<size_t> kv_counter0 = 0;
  std::atomic<size_t> kv_counter1 = 0;
  std::atomic<SuperVersion*> super_version = nullptr;
};
struct BGThreadMetadata {
  DBImpl* db;
  void* func_args;
};
// Sanitize db options.  The caller should delete result.info_log if
// it is not equal to src.info_log.
Options SanitizeOptions(const std::string& db,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_DB_IMPL_H_
