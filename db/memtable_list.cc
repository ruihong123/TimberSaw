//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include "db/memtable_list.h"

#include <cinttypes>
#include <limits>
#include <queue>
#include <string>
#include "db/db_impl.h"
#include "db/memtable.h"
//#include "db/range_tombstone_fragmenter.h"
#include "db/version_set.h"
//#include "logging/log_buffer.h"
//#include "monitoring/thread_status_util.h"
#include "leveldb/db.h"
#include "leveldb//env.h"
#include "leveldb/iterator.h"
//#include "table/merging_iterator.h"
//#include "test_util/sync_point.h"
#include "util/coding.h"
#include "table/merger.h"


namespace leveldb {

class InternalKeyComparator;
class Mutex;
class VersionSet;

void MemTableListVersion::AddMemTable(MemTable* m) {
  memlist_.push_front(m);
  *parent_memtable_list_memory_usage_ += m->ApproximateMemoryUsage();
}

//void MemTableListVersion::UnrefMemTable(autovector<MemTable*>* to_delete,
//                                        MemTable* m) {
//  if (m->Unref()) {
//    to_delete->push_back(m);
//    assert(*parent_memtable_list_memory_usage_ >= m->ApproximateMemoryUsage());
//    *parent_memtable_list_memory_usage_ -= m->ApproximateMemoryUsage();
//  }
//}

MemTableListVersion::MemTableListVersion(
    size_t* parent_memtable_list_memory_usage, const MemTableListVersion& old)
    : max_write_buffer_number_to_maintain_(
          old.max_write_buffer_number_to_maintain_),
      max_write_buffer_size_to_maintain_(
          old.max_write_buffer_size_to_maintain_),
      parent_memtable_list_memory_usage_(parent_memtable_list_memory_usage) {
  memlist_ = old.memlist_;
  for (auto& m : memlist_) {
    m->Ref();
  }

  memlist_history_ = old.memlist_history_;
  for (auto& m : memlist_history_) {
    m->Ref();
  }
}

MemTableListVersion::MemTableListVersion(
    size_t* parent_memtable_list_memory_usage,
    int max_write_buffer_number_to_maintain,
    int64_t max_write_buffer_size_to_maintain)
    : max_write_buffer_number_to_maintain_(max_write_buffer_number_to_maintain),
      max_write_buffer_size_to_maintain_(max_write_buffer_size_to_maintain),
      parent_memtable_list_memory_usage_(parent_memtable_list_memory_usage) {}

void MemTableListVersion::Ref() { ++refs_; }

// called by superversion::clean()
void MemTableListVersion::Unref(autovector<MemTable*>* to_delete) {
  assert(refs_ >= 1);
  --refs_;
  if (refs_ == 0) {
    // if to_delete is equal to nullptr it means we're confident
    // that refs_ will not be zero
    assert(to_delete != nullptr);
    for (const auto& m : memlist_) {
      m->Unref();
    }
    for (const auto& m : memlist_history_) {
      m->Unref();
    }
    delete this;
  }
}

int MemTableList::NumNotFlushed() const {
  int size = static_cast<int>(current_->memlist_.size());
  assert(num_flush_not_started_ <= size);
  return size;
}

int MemTableList::NumFlushed() const {
  return static_cast<int>(current_->memlist_history_.size());
}

// Search all the memtables starting from the most recent one.
// Return the most recent value found, if any.
// Operands stores the list of merge operations to apply, so far.
bool MemTableListVersion::Get(const LookupKey& key, std::string* value,
                              Status* s) {
  return GetFromList(&memlist_, key, value, s);
}

//void MemTableListVersion::MultiGet(const ReadOptions& read_options,
//                                   MultiGetRange* range, ReadCallback* callback,
//                                   bool* is_blob) {
//  for (auto memtable : memlist_) {
//    memtable->MultiGet(read_options, range, callback, is_blob);
//    if (range->empty()) {
//      return;
//    }
//  }
//}

//bool MemTableListVersion::GetMergeOperands(
//    const LookupKey& key, Status* s, MergeContext* merge_context,
//    SequenceNumber* max_covering_tombstone_seq, const ReadOptions& read_opts) {
//  for (MemTable* memtable : memlist_) {
//    bool done = memtable->Get(key, /*value*/ nullptr, /*timestamp*/ nullptr, s,
//                              merge_context, max_covering_tombstone_seq,
//                              read_opts, nullptr, nullptr, false);
//    if (done) {
//      return true;
//    }
//  }
//  return false;
//}

//bool MemTableListVersion::GetFromHistory(
//    const LookupKey& key, std::string* value, std::string* timestamp, Status* s,
//    MergeContext* merge_context, SequenceNumber* max_covering_tombstone_seq,
//    SequenceNumber* seq, const ReadOptions& read_opts, bool* is_blob_index) {
//  return GetFromList(&memlist_history_, key, value, timestamp, s, merge_context,
//                     max_covering_tombstone_seq, seq, read_opts,
//                     nullptr /*read_callback*/, is_blob_index);
//}

bool MemTableListVersion::GetFromList(std::list<MemTable*>* list,
                                      const LookupKey& key, std::string* value,
                                      Status* s) {
  for (auto& memtable : *list) {
    SequenceNumber current_seq = kMaxSequenceNumber;

    bool done = memtable->Get(key, value, s);

    if (done) {
      return true;
    }
    if (!done && !s->ok() && !s->IsNotFound()) {
      return false;
    }
  }
  return false;
}

//Status MemTableListVersion::AddRangeTombstoneIterators(
//    const ReadOptions& read_opts, Arena* /*arena*/,
//    RangeDelAggregator* range_del_agg) {
//  assert(range_del_agg != nullptr);
//  // Except for snapshot read, using kMaxSequenceNumber is OK because these
//  // are immutable memtables.
//  SequenceNumber read_seq = read_opts.snapshot != nullptr
//                                ? read_opts.snapshot->GetSequenceNumber()
//                                : kMaxSequenceNumber;
//  for (auto& m : memlist_) {
//    std::unique_ptr<FragmentedRangeTombstoneIterator> range_del_iter(
//        m->NewRangeTombstoneIterator(read_opts, read_seq));
//    range_del_agg->AddTombstones(std::move(range_del_iter));
//  }
//  return Status::OK();
//}

void MemTableListVersion::AddIteratorsToVector(
    const ReadOptions& options, std::vector<Iterator*>* iterator_list,
    Arena* arena) {
  for (auto& m : memlist_) {
    iterator_list->push_back(m->NewIterator());
  }
}
MemTable* MemTableListVersion::PickMemtablesSeqBelong(size_t seq) {
  for(auto iter : memlist_){
    if (seq >= iter->GetFirstseq() && seq <= iter->Getlargest_seq_supposed()) {
      return iter;
    }
  }
  return nullptr;
}
//void MemTableListVersion::AddIterators(
//    const ReadOptions& options, MergeIteratorBuilder* merge_iter_builder) {
//  for (auto& m : memlist_) {
//    merge_iter_builder->AddIterator(
//        m->NewIterator(options, merge_iter_builder->GetArena()));
//  }
//}
// The number of sequential number
uint64_t MemTableListVersion::GetTotalNumEntries() const {
  uint64_t total_num = 0;
  for (auto& m : memlist_) {
    total_num += m->Get_seq_count();
  }
  return total_num;
}
void MemTableListVersion::AddIteratorsToList(
    std::vector<Iterator*>* list) {
//  int iter_num = memlist_.size();
  for (auto iter : memlist_) {
    this->Ref();
    list->push_back(iter->NewIterator());
  }
}

//MemTable::MemTableStats MemTableListVersion::ApproximateStats(
//    const Slice& start_ikey, const Slice& end_ikey) {
//  MemTable::MemTableStats total_stats = {0, 0};
//  for (auto& m : memlist_) {
//    auto mStats = m->ApproximateStats(start_ikey, end_ikey);
//    total_stats.size += mStats.size;
//    total_stats.count += mStats.count;
//  }
//  return total_stats;
//}

//uint64_t MemTableListVersion::GetTotalNumDeletes() const {
//  uint64_t total_num = 0;
//  for (auto& m : memlist_) {
//    total_num += m->num_deletes();
//  }
//  return total_num;
//}

//SequenceNumber MemTableListVersion::GetEarliestSequenceNumber(
//    bool include_history) const {
//  if (include_history && !memlist_history_.empty()) {
//    return memlist_history_.back()->GetEarliestSequenceNumber();
//  } else if (!memlist_.empty()) {
//    return memlist_.back()->GetEarliestSequenceNumber();
//  } else {
//    return kMaxSequenceNumber;
//  }
//}

// caller is responsible for referencing m
void MemTableListVersion::Add(MemTable* m) {
  assert(refs_ == 1);  // only when refs_ == 1 is MemTableListVersion mutable
  AddMemTable(m);
  TrimHistory(m->ApproximateMemoryUsage());
}

// Removes m from list of memtables not flushed.  Caller should NOT Unref m.
// we can remove the argument to_delete.
void MemTableListVersion::Remove(MemTable* m) {
  // THIS SHOULD be protected by a lock.

  assert(refs_ == 1);  // only when refs_ == 1 is MemTableListVersion mutable
  memlist_.remove(m);

//  m->MarkFlushed();
  if (max_write_buffer_size_to_maintain_ > 0 ||
      max_write_buffer_number_to_maintain_ > 0) {
    memlist_history_.push_front(m);
    // Unable to get size of mutable memtable at this point, pass 0 to
    // TrimHistory as a best effort.
    TrimHistory(0);
  } else {
    //TOFIX: this unreference will trigger memtable garbage collection, if there
    // is a reader refering the tables, there could be problem. The solution could be
    // we do not unrefer the table here, we unref it outside
    // Answer: The statement above is incorrect, because the Unref here will not trigger the
    // contention. The reader will pin another memtable list version which will prohibit
    // the memtable from being deallocated.
    m->Unref();
    //TOThink: what is the parent_memtable_list_memory_usage used for?
    *parent_memtable_list_memory_usage_ -= m->ApproximateMemoryUsage();
  }
}

// return the total memory usage assuming the oldest flushed memtable is dropped
size_t MemTableListVersion::ApproximateMemoryUsageExcludingLast() const {
  size_t total_memtable_size = 0;
  for (auto& memtable : memlist_) {
    total_memtable_size += memtable->ApproximateMemoryUsage();
  }
  for (auto& memtable : memlist_history_) {
    total_memtable_size += memtable->ApproximateMemoryUsage();
  }
  if (!memlist_history_.empty()) {
    total_memtable_size -= memlist_history_.back()->ApproximateMemoryUsage();
  }
  return total_memtable_size;
}

bool MemTableListVersion::MemtableLimitExceeded(size_t usage) {
  if (max_write_buffer_size_to_maintain_ > 0) {
    // calculate the total memory usage after dropping the oldest flushed
    // memtable, compare with max_write_buffer_size_to_maintain_ to decide
    // whether to trim history
    return ApproximateMemoryUsageExcludingLast() + usage >=
           static_cast<size_t>(max_write_buffer_size_to_maintain_);
  } else if (max_write_buffer_number_to_maintain_ > 0) {
    return memlist_.size() + memlist_history_.size() >
           static_cast<size_t>(max_write_buffer_number_to_maintain_);
  } else {
    return false;
  }
}

// Make sure we don't use up too much space in history
bool MemTableListVersion::TrimHistory(size_t usage) {
  bool ret = false;
  while (MemtableLimitExceeded(usage) && !memlist_history_.empty()) {
    MemTable* x = memlist_history_.back();
    memlist_history_.pop_back();


    *parent_memtable_list_memory_usage_ -= x->ApproximateMemoryUsage();
    ret = true;
    x->Unref();
  }
  return ret;
}

// Returns true if there is at least one memtable on which flush has
// not yet started.
bool MemTableList::IsFlushPending() const {
  return this->num_flush_not_started_.load() >= 2;
}

// Returns the memtables that need to be flushed.
//Pick up a configurable number of memtable, not too much and not too less.2~4 could be better
void MemTableList::PickMemtablesToFlush(autovector<MemTable*>* mems) {
//  AutoThreadOperationStageUpdater stage_updater(
//      ThreadStatus::STAGE_PICK_MEMTABLES_TO_FLUSH);
  const auto& memlist = current_->memlist_;
  bool atomic_flush = false;
  int table_counter = 0;
  for (auto it = memlist.rbegin(); it != memlist.rend(); ++it) {
    MemTable* m = *it;

    if (!m->CheckFlushInProcess()) {
      assert(!m->CheckFlushFinished());
      num_flush_not_started_.fetch_sub(1);
      m->SetFlushState(MemTable::FLUSH_PROCESSING);
//      if (num_flush_not_started_ == 0) {
//        imm_flush_needed.store(false, std::memory_order_release);
//      }
//      m->flush_in_progress_ = true;  // flushing will start very soon
      mems->push_back(m);
      // at most pick 2 table and do the merge
      if(++table_counter>= leveldb::config::ImmuNumPerFlush)
        break;
    }
  }
  DEBUG_arg("table picked is %d", table_counter);
  if (!atomic_flush || num_flush_not_started_ == 0) {
    flush_requested_ = false;  // start-flush request is complete
  }
}

MemTable* MemTableList::PickMemtablesSeqBelong(size_t seq) {
    return current_->PickMemtablesSeqBelong(seq);
}


Iterator* MemTableList::MakeInputIterator(FlushJob* job) {
  int iter_num = job->mem_vec.size();
  auto** list = new Iterator*[iter_num];
  for (int i = 0; i<job->mem_vec.size(); i++) {
    list[i] = job->mem_vec[i]->NewIterator();
  }

  Iterator* result = NewMergingIterator(&cmp, list, iter_num);
  delete[] list;
  return result;
}

void MemTableList::RollbackMemtableFlush(const autovector<MemTable*>& mems,
                                         uint64_t /*file_number*/) {
//  AutoThreadOperationStageUpdater stage_updater(
//      ThreadStatus::STAGE_MEMTABLE_ROLLBACK);
  assert(!mems.empty());

  // If the flush was not successful, then just reset state.
  // Maybe a succeeding attempt to flush will be successful.
  for (MemTable* m : mems) {
    assert(m->CheckFlushInProcess());

    m->SetFlushState(MemTable::FLUSH_REQUESTED);
    num_flush_not_started_.fetch_add(1);
  }
  imm_flush_needed.store(true, std::memory_order_release);
}

// Try record a successful flush in the manifest file. It might just return
// Status::OK letting a concurrent flush to do actual the recording..
Status MemTableList::TryInstallMemtableFlushResults(
    FlushJob* job, VersionSet* vset,
    std::shared_ptr<RemoteMemTableMetaData>& sstable, VersionEdit* edit) {
//  AutoThreadOperationStageUpdater stage_updater(
//      ThreadStatus::STAGE_MEMTABLE_INSTALL_FLUSH_RESULTS);
  autovector<MemTable*> mems = job->mem_vec;
  assert(mems.size() >0);
  imm_mtx->lock();
  if (mems.size() == 1)
    printf("check 1 memtable installation\n");
  // Flush was successful
  // Record the status on the memtable object. Either this call or a call by a
  // concurrent flush thread will read the status and write it to manifest.
  for (size_t i = 0; i < mems.size(); ++i) {
    // First mark the flushing is finished in the immutables
    mems[i]->MarkFlushed();
    mems[i]->sstable = sstable;
  }

  // if some other thread is already committing, then return
  Status s;

  // Only a single thread can be executing this piece of code
  commit_in_progress_ = true;

  // Retry until all completed flushes are committed. New flushes can finish
  // while the current thread is writing manifest where mutex is released.
  while (s.ok()) {
    auto& memlist = current_->memlist_;
    // The back is the oldest; if flush_completed_ is not set to it, it means
    // that we were assigned a more recent memtable. The memtables' flushes must
    // be recorded in manifest in order. A concurrent flush thread, who is
    // assigned to flush the oldest memtable, will later wake up and does all
    // the pending writes to manifest, in order.
    if (memlist.empty() || !memlist.back()->CheckFlushFinished()) {
      //Unlock the spinlock and do not write to the version
      imm_mtx->unlock();
      break;
    }
    // scan all memtables from the earliest, and commit those
    // (in that order) that have finished flushing. Memtables
    // are always committed in the order that they were created.
    uint64_t batch_file_number = 0;
    size_t batch_count = 0;
//    autovector<VersionEdit*> edit_list;
//    autovector<MemTable*> memtables_to_flush;
    // enumerate from the last (earliest) element to see how many batch finished
    for (auto it = memlist.rbegin(); it != memlist.rend(); ++it) {
      MemTable* m = *it;
      if (!m->CheckFlushFinished()) {
        break;
      }
      assert(m->sstable != nullptr);
      edit->AddFileIfNotExist(0,m->sstable);
      batch_count++;
      if (batch_count == 3)
        printf("check\n");
    }

    // TODO(myabandeh): Not sure how batch_count could be 0 here.


      // we will be changing the version in the next code path,
      // so we better create a new one, since versions are immutable
    InstallNewVersion();
    size_t batch_count_for_fetch_sub = batch_count;
    while (batch_count-- > 0) {
      MemTable* m = current_->memlist_.back();

      assert(m->sstable != nullptr);
      autovector<MemTable*> dummy_to_delete = autovector<MemTable*>();
      current_->Remove(m);
      UpdateCachedValuesFromMemTableListVersion();
      ResetTrimHistoryNeeded();
    }
    current_memtable_num_.fetch_sub(batch_count_for_fetch_sub);
    job->write_stall_cv_->SignalAll();
    imm_mtx->unlock();

    s = vset->LogAndApply(edit);

  }

  commit_in_progress_ = false;
  return s;
}

// New memtables are inserted at the front of the list.
// This should be guarded by imm_mtx
void MemTableList::Add(MemTable* m) {
  assert(static_cast<int>(current_->memlist_.size()) >= num_flush_not_started_);
  InstallNewVersion();
  // this method is used to move mutable memtable into an immutable list.
  // since mutable memtable is already refcounted by the DBImpl,
  // and when moving to the imutable list we don't unref it,
  // we don't have to ref the memtable here. we just take over the
  // reference from the DBImpl.
  current_->Add(m);
  // Add memtable number atomically.
  current_memtable_num_.fetch_add(1);
  m->SetFlushState(MemTable::FLUSH_REQUESTED);
  num_flush_not_started_.fetch_add(1);
  if (num_flush_not_started_ == 1) {
    imm_flush_needed.store(true, std::memory_order_release);
  }
  UpdateCachedValuesFromMemTableListVersion();
  ResetTrimHistoryNeeded();
}

bool MemTableList::TrimHistory(autovector<MemTable*>* to_delete, size_t usage) {
  InstallNewVersion();
  bool ret = current_->TrimHistory(usage);
  UpdateCachedValuesFromMemTableListVersion();
  ResetTrimHistoryNeeded();
  return ret;
}

// Returns an estimate of the number of bytes of data in use.
size_t MemTableList::ApproximateUnflushedMemTablesMemoryUsage() {
  size_t total_size = 0;
  for (auto& memtable : current_->memlist_) {
    total_size += memtable->ApproximateMemoryUsage();
  }
  return total_size;
}

size_t MemTableList::ApproximateMemoryUsage() { return current_memory_usage_; }

size_t MemTableList::ApproximateMemoryUsageExcludingLast() const {
  const size_t usage =
      current_memory_usage_excluding_last_.load(std::memory_order_relaxed);
  return usage;
}

bool MemTableList::HasHistory() const {
  const bool has_history = current_has_history_.load(std::memory_order_relaxed);
  return has_history;
}

void MemTableList::UpdateCachedValuesFromMemTableListVersion() {
  const size_t total_memtable_size =
      current_->ApproximateMemoryUsageExcludingLast();
  current_memory_usage_excluding_last_.store(total_memtable_size,
                                             std::memory_order_relaxed);

  const bool has_history = current_->HasHistory();
  current_has_history_.store(has_history, std::memory_order_relaxed);
}

//uint64_t MemTableList::ApproximateOldestKeyTime() const {
//  if (!current_->memlist_.empty()) {
//    return current_->memlist_.back()->ApproximateOldestKeyTime();
//  }
//  return std::numeric_limits<uint64_t>::max();
//}

void MemTableList::InstallNewVersion() {
  if (current_->refs_ == 1) {
    // we're the only one using the version, just keep using it
  } else {
    // somebody else holds the current version, we need to create new one
    MemTableListVersion* version = current_;
    current_ = new MemTableListVersion(&current_memory_usage_, *version);
    current_->Ref();
    version->Unref();
  }
}

//uint64_t MemTableList::PrecomputeMinLogContainingPrepSection(
//    const autovector<MemTable*>& memtables_to_flush) {
//  uint64_t min_log = 0;
//
//  for (auto& m : current_->memlist_) {
//    // Assume the list is very short, we can live with O(m*n). We can optimize
//    // if the performance has some problem.
//    bool should_skip = false;
//    for (MemTable* m_to_flush : memtables_to_flush) {
//      if (m == m_to_flush) {
//        should_skip = true;
//        break;
//      }
//    }
//    if (should_skip) {
//      continue;
//    }
//
//    auto log = m->GetMinLogContainingPrepSection();
//
//    if (log > 0 && (min_log == 0 || log < min_log)) {
//      min_log = log;
//    }
//  }
//
//  return min_log;
//}

// Commit a successful atomic flush in the manifest file.
//Status InstallMemtableAtomicFlushResults(
//    const autovector<MemTableList*>* imm_lists,
//    const autovector<MemTable*>*& mems_list, VersionSet* vset,
//    port::Mutex* mu, const autovector<RemoteMemTableMetaData*>& file_metas,
//    autovector<MemTable*>* to_delete, FSDirectory* db_directory,
//    LogBuffer* log_buffer) {
//  AutoThreadOperationStageUpdater stage_updater(
//      ThreadStatus::STAGE_MEMTABLE_INSTALL_FLUSH_RESULTS);
//  mu->AssertHeld();
//
//  size_t num = mems_list.size();
//  assert(cfds.size() == num);
//  if (imm_lists != nullptr) {
//    assert(imm_lists->size() == num);
//  }
//  for (size_t k = 0; k != num; ++k) {
//#ifndef NDEBUG
//    const auto* imm =
//        (imm_lists == nullptr) ? cfds[k]->imm() : imm_lists->at(k);
//    if (!mems_list[k]->empty()) {
//      assert((*mems_list[k])[0]->GetID() == imm->GetEarliestMemTableID());
//    }
//#endif
//    assert(nullptr != file_metas[k]);
//    for (size_t i = 0; i != mems_list[k]->size(); ++i) {
//      assert(i == 0 || (*mems_list[k])[i]->GetEdits()->NumEntries() == 0);
//      (*mems_list[k])[i]->SetFlushCompleted(true);
//      (*mems_list[k])[i]->SetFileNumber(file_metas[k]->fd.GetNumber());
//    }
//  }
//
//  Status s;
//
//  autovector<autovector<VersionEdit*>> edit_lists;
//  uint32_t num_entries = 0;
//  for (const auto mems : mems_list) {
//    assert(mems != nullptr);
//    autovector<VersionEdit*> edits;
//    assert(!mems->empty());
//    edits.emplace_back((*mems)[0]->GetEdits());
//    ++num_entries;
//    edit_lists.emplace_back(edits);
//  }
//  // Mark the version edits as an atomic group if the number of version edits
//  // exceeds 1.
//  if (cfds.size() > 1) {
//    for (auto& edits : edit_lists) {
//      assert(edits.size() == 1);
//      edits[0]->MarkAtomicGroup(--num_entries);
//    }
//    assert(0 == num_entries);
//  }
//
//  // this can release and reacquire the mutex.
//  s = vset->LogAndApply(cfds, mutable_cf_options_list, edit_lists, mu,
//                        db_directory);
//
//  for (size_t k = 0; k != cfds.size(); ++k) {
//    auto* imm = (imm_lists == nullptr) ? cfds[k]->imm() : imm_lists->at(k);
//    imm->InstallNewVersion();
//  }
//
//  if (s.ok() || s.IsColumnFamilyDropped()) {
//    for (size_t i = 0; i != cfds.size(); ++i) {
//      if (cfds[i]->IsDropped()) {
//        continue;
//      }
//      auto* imm = (imm_lists == nullptr) ? cfds[i]->imm() : imm_lists->at(i);
//      for (auto m : *mems_list[i]) {
//        assert(m->GetFileNumber() > 0);
//        uint64_t mem_id = m->GetID();
//
//        const VersionEdit* const edit = m->GetEdits();
//        assert(edit);
//
//        if (edit->GetBlobFileAdditions().empty()) {
//          ROCKS_LOG_BUFFER(log_buffer,
//                           "[%s] Level-0 commit table #%" PRIu64
//                           ": memtable #%" PRIu64 " done",
//                           cfds[i]->GetName().c_str(), m->GetFileNumber(),
//                           mem_id);
//        } else {
//          ROCKS_LOG_BUFFER(log_buffer,
//                           "[%s] Level-0 commit table #%" PRIu64
//                           " (+%zu blob files)"
//                           ": memtable #%" PRIu64 " done",
//                           cfds[i]->GetName().c_str(), m->GetFileNumber(),
//                           edit->GetBlobFileAdditions().size(), mem_id);
//        }
//
//        imm->current_->Remove(m, to_delete);
//        imm->UpdateCachedValuesFromMemTableListVersion();
//        imm->ResetTrimHistoryNeeded();
//      }
//    }
//  } else {
//    for (size_t i = 0; i != cfds.size(); ++i) {
//      auto* imm = (imm_lists == nullptr) ? cfds[i]->imm() : imm_lists->at(i);
//      for (auto m : *mems_list[i]) {
//        uint64_t mem_id = m->GetID();
//
//        const VersionEdit* const edit = m->GetEdits();
//        assert(edit);
//
//        if (edit->GetBlobFileAdditions().empty()) {
//          ROCKS_LOG_BUFFER(log_buffer,
//                           "[%s] Level-0 commit table #%" PRIu64
//                           ": memtable #%" PRIu64 " failed",
//                           cfds[i]->GetName().c_str(), m->GetFileNumber(),
//                           mem_id);
//        } else {
//          ROCKS_LOG_BUFFER(log_buffer,
//                           "[%s] Level-0 commit table #%" PRIu64
//                           " (+%zu blob files)"
//                           ": memtable #%" PRIu64 " failed",
//                           cfds[i]->GetName().c_str(), m->GetFileNumber(),
//                           edit->GetBlobFileAdditions().size(), mem_id);
//        }
//
//        m->SetFlushCompleted(false);
//        m->SetFlushInProgress(false);
//        m->GetEdits()->Clear();
//        m->SetFileNumber(0);
//        imm->num_flush_not_started_++;
//      }
//      imm->imm_flush_needed.store(true, std::memory_order_release);
//    }
//  }
//
//  return s;
//}

//void MemTableList::RemoveOldMemTables(uint64_t log_number,
//                                      autovector<MemTable*>* to_delete) {
//  assert(to_delete != nullptr);
//  InstallNewVersion();
//  auto& memlist = current_->memlist_;
//  autovector<MemTable*> old_memtables;
//  for (auto it = memlist.rbegin(); it != memlist.rend(); ++it) {
//    MemTable* mem = *it;
//    if (mem->GetNextLogNumber() > log_number) {
//      break;
//    }
//    old_memtables.push_back(mem);
//  }
//
//  for (auto it = old_memtables.begin(); it != old_memtables.end(); ++it) {
//    MemTable* mem = *it;
//    current_->Remove(mem, to_delete);
//    --num_flush_not_started_;
//    if (0 == num_flush_not_started_) {
//      imm_flush_needed.store(false, std::memory_order_release);
//    }
//  }
//
//  UpdateCachedValuesFromMemTableListVersion();
//  ResetTrimHistoryNeeded();
//}

void FlushJob::Waitforpendingwriter() {
  size_t counter = 0;
  for (auto iter: mem_vec) {
    while (!iter->able_to_flush.load()) {
      counter++;
      if (counter == 500) {
//        printf("signal all the wait threads\n");
        usleep(10);
        write_stall_cv_->SignalAll();
        counter = 0;
      }
    }
  }

}
void FlushJob::SetAllMemStateProcessing() {
  for (auto iter: mem_vec) {
    iter->SetFlushState(MemTable::FLUSH_PROCESSING);

  }
}
FlushJob::FlushJob(port::Mutex* write_stall_mutex,
                   port::CondVar* write_stall_cv) :write_stall_mutex_(write_stall_mutex),
                                                   write_stall_cv_(write_stall_cv){}
}  // namespace ROCKSDB_NAMESPACE
