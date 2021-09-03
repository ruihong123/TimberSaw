//
// Created by ruihong on 7/29/21.
//
#include "memory_node/memory_node_keeper.h"

#include "db/table_cache.h"
#include <list>

#include "table/table_builder_memoryside.h"

namespace leveldb{
std::shared_ptr<RDMA_Manager> Memory_Node_Keeper::rdma_mg = std::shared_ptr<RDMA_Manager>();
leveldb::Memory_Node_Keeper::Memory_Node_Keeper(bool use_sub_compaction): internal_comparator_(BytewiseComparator()), opts(std::make_shared<Options>(true)),
usesubcompaction(use_sub_compaction), table_cache_(new TableCache("home_node", *opts, opts->max_open_files)),
versions_(new VersionSet("home_node", opts.get(), table_cache_, &internal_comparator_, &versionset_mtx)){
    struct leveldb::config_t config = {
        NULL,  /* dev_name */
        NULL,  /* server_name */
        19833, /* tcp_port */
        1,	 /* ib_port */
        1, /* gid_idx */
        0};
    //  size_t write_block_size = 4*1024*1024;
    //  size_t read_block_size = 4*1024;
    size_t table_size = 10*1024*1024;
    rdma_mg = std::make_shared<RDMA_Manager>(config, table_size, 1); //set memory server node id as 1.
    rdma_mg->Mempool_initialize(std::string("FlushBuffer"), RDMA_WRITE_BLOCK);
    //TODO: add a handle function for the option value to get the non-default bloombits.
    opts->filter_policy = new InternalFilterPolicy(NewBloomFilterPolicy(opts->bloom_bits));
    opts->comparator = &internal_comparator_;
    ClipToRange(&opts->max_open_files, 64 + kNumNonTableCacheFiles, 50000);
    ClipToRange(&opts->write_buffer_size, 64 << 10, 1 << 30);
    ClipToRange(&opts->max_file_size, 1 << 20, 1 << 30);
    ClipToRange(&opts->block_size, 1 << 10, 4 << 20);
  }
//  void leveldb::Memory_Node_Keeper::Schedule(void (*background_work_function)(void*),
//                                             void* background_work_arg,
//                                             ThreadPoolType type) {
//    message_handler_pool_.Schedule(background_work_function, background_work_arg);
//  }
  void Memory_Node_Keeper::SetBackgroundThreads(int num, ThreadPoolType type) {
    message_handler_pool_.SetBackgroundThreads(num);
  }
  void Memory_Node_Keeper::MaybeScheduleCompaction(std::string& client_ip) {
    if (versions_->NeedsCompaction()) {
      //    background_compaction_scheduled_ = true;
      void* function_args = new std::string(client_ip);
      BGThreadMetadata* thread_pool_args = new BGThreadMetadata{.db = this, .func_args = function_args};
      if (message_handler_pool_.queue_len_.load()>256){
        //If there has already be enough compaction scheduled, then drop this one
        return;
      }
      message_handler_pool_.Schedule(BGWork_Compaction, static_cast<void*>(thread_pool_args));
      DEBUG("Schedule a Compaction !\n");
    }
  }
  void Memory_Node_Keeper::BGWork_Compaction(void* thread_arg) {
    BGThreadMetadata* p = static_cast<BGThreadMetadata*>(thread_arg);
    ((Memory_Node_Keeper*)p->db)->BackgroundCompaction(p->func_args);
    delete static_cast<BGThreadMetadata*>(thread_arg);
  }
  void Memory_Node_Keeper::BackgroundCompaction(void* p) {
  //  write_stall_mutex_.AssertNotHeld();
  std::string* client_ip = static_cast<std::string*>(p);
  if (versions_->NeedsCompaction()) {
    Compaction* c;
//    bool is_manual = (manual_compaction_ != nullptr);
//    InternalKey manual_end;
//    if (is_manual) {
//      ManualCompaction* m = manual_compaction_;
//      c = versions_->CompactRange(m->level, m->begin, m->end);
//      m->done = (c == nullptr);
//      if (c != nullptr) {
//        manual_end = c->input(0, c->num_input_files(0) - 1)->largest;
//      }
//      Log(options_.info_log,
//          "Manual compaction at level-%d from %s .. %s; will stop at %s\n",
//          m->level, (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
//          (m->end ? m->end->DebugString().c_str() : "(end)"),
//          (m->done ? "(end)" : manual_end.DebugString().c_str()));
//    } else {
      c = versions_->PickCompaction();
      //if there is no task to pick up, just return.
      if (c== nullptr){
        DEBUG("compaction task executed but not found doable task.\n");
        delete c;
        return;
      }

//    }
    //    write_stall_mutex_.AssertNotHeld();
    Status status;
    if (c == nullptr) {
      // Nothing to do
    } else if (c->IsTrivialMove()) {
      // Move file to next level
      assert(c->num_input_files(0) == 1);
      std::shared_ptr<RemoteMemTableMetaData> f = c->input(0, 0);
      c->edit()->RemoveFile(c->level(), f->number, f->creator_node_id);
      c->edit()->AddFile(c->level() + 1, f);
      f->level = f->level +1;
      {
//        std::unique_lock<std::mutex> l(versions_mtx);// TODO(ruihong): remove all the superversion mutex usage.
        c->ReleaseInputs();
        {
          std::unique_lock<std::mutex> lck(versionset_mtx);
          status = versions_->LogAndApply(c->edit(), 0);
          versions_->Pin_Version_For_Compute();
          Edit_sync_to_remote(c->edit(), *client_ip);
        }
//        InstallSuperVersion();
      }

      DEBUG("Trival compaction\n");
    } else {
      CompactionState* compact = new CompactionState(c);
#ifndef NDEBUG
      if (c->level() >= 1){
        printf("Compaction level > 1");
      }
#endif
      auto start = std::chrono::high_resolution_clock::now();
      //      write_stall_mutex_.AssertNotHeld();
      // Only when there is enough input level files and output level files will the subcompaction triggered
      if (usesubcompaction && c->num_input_files(0)>=4 && c->num_input_files(1)>1){
        status = DoCompactionWorkWithSubcompaction(compact, *client_ip);
//        status = DoCompactionWork(compact, *client_ip);
      }else{
        status = DoCompactionWork(compact, *client_ip);
      }

      auto stop = std::chrono::high_resolution_clock::now();
      auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
//#ifndef NDEBUG
      printf("Table compaction time elapse (%ld) us, compaction level is %d, first level file number %d, the second level file number %d \n",
             duration.count(), compact->compaction->level(), compact->compaction->num_input_files(0),compact->compaction->num_input_files(1) );
//#endif
      DEBUG("Non-trivalcompaction!\n");
      std::cout << "compaction task table number in the first level"<<compact->compaction->inputs_[0].size() << std::endl;
      if (!status.ok()) {
        std::cerr << "compaction failed" << std::endl;
//        RecordBackgroundError(status);
      }
      CleanupCompaction(compact);
      //    RemoveObsoleteFiles();
    }
    delete c;

//    if (status.ok()) {
//      // Done
//    } else if (shutting_down_.load(std::memory_order_acquire)) {
//      // Ignore compaction errors found during shutting down
//    } else {
//      Log(options_.info_log, "Compaction error: %s", status.ToString().c_str());
//    }

//    if (is_manual) {
//      ManualCompaction* m = manual_compaction_;
//      if (!status.ok()) {
//        m->done = true;
//      }
//      if (!m->done) {
//        // We only compacted part of the requested range.  Update *m
//        // to the range that is left to be compacted.
//        m->tmp_storage = manual_end;
//        m->begin = &m->tmp_storage;
//      }
//      manual_compaction_ = nullptr;
//    }
  }
  MaybeScheduleCompaction(*client_ip);
  delete client_ip;

}
void Memory_Node_Keeper::CleanupCompaction(CompactionState* compact) {
  //  undefine_mutex.AssertHeld();
  if (compact->builder != nullptr) {
    // May happen if we get a shutdown call in the middle of compaction
    compact->builder->Abandon();
    delete compact->builder;
  } else {
    //    assert(compact->outfile == nullptr);
  }
  //  delete compact->outfile;
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionOutput& out = compact->outputs[i];
    //    pending_outputs_.erase(out.number);
  }
  delete compact;
}
Status Memory_Node_Keeper::DoCompactionWork(CompactionState* compact,
                                            std::string& client_ip) {
//  const uint64_t start_micros = env_->NowMicros();
  int64_t imm_micros = 0;  // Micros spent doing imm_ compactions


  assert(versions_->NumLevelFiles(compact->compaction->level()) > 0);
  assert(compact->builder == nullptr);
  //  assert(compact->outfile == nullptr);
//  if (snapshots_.empty()) {
//    compact->smallest_snapshot = versions_->LastSequence();
//  } else {
//    compact->smallest_snapshot = snapshots_.oldest()->sequence_number();
//  }

  Iterator* input = versions_->MakeInputIteratorMemoryServer(compact->compaction);

  // Release mutex while we're actually doing the compaction work
  //  undefine_mutex.Unlock();

  input->SeekToFirst();
#ifndef NDEBUG
  int Not_drop_counter = 0;
  int number_of_key = 0;
#endif
  Status status;
  // TODO: try to create two ikey for parsed key, they can in turn represent the current user key
  //  and former one, which can save the data copy overhead.
  ParsedInternalKey ikey;
  std::string current_user_key;
  bool has_current_user_key = false;
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
  Slice key;
  assert(input->Valid());
#ifndef NDEBUG
  printf("first key is %s", input->key().ToString().c_str());
#endif
  while (input->Valid()) {
    key = input->key();
    //    assert(key.data()[0] == '0');
    //Check whether the output file have too much overlap with level n + 2
    if (compact->compaction->ShouldStopBefore(key) &&
    compact->builder != nullptr) {
      status = FinishCompactionOutputFile(compact, input);
      if (!status.ok()) {
        break;
      }
    }
    // key merged below!!!
    // Handle key/value, add to state, etc.
    bool drop = false;
    if (!ParseInternalKey(key, &ikey)) {
      // Do not hide error keys
      current_user_key.clear();
      has_current_user_key = false;
      last_sequence_for_key = kMaxSequenceNumber;
    } else {
      if (!has_current_user_key){
        //TODO: can we avoid the data copy here, can we set two buffers in block and make
        // the old user key not be garbage collected so that the old Slice can be
        // directly used here.
        current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
        has_current_user_key = true;
      }
      else if(user_comparator()->Compare(ikey.user_key, Slice(current_user_key)) !=
      0) {
        // First occurrence of this user key
        current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
        //        has_current_user_key = true;
        //        last_sequence_for_key = kMaxSequenceNumber;
        // this will result in the key not drop, next if will always be false because of
        // the last_sequence_for_key.
      }else{
        drop = true;
      }
    }
#ifndef NDEBUG
    number_of_key++;
#endif
    if (!drop) {
      // Open output file if necessary
      if (compact->builder == nullptr) {
        status = OpenCompactionOutputFile(compact);
        if (!status.ok()) {
          break;
        }
      }
      if (compact->builder->NumEntries() == 0) {
        compact->current_output()->smallest.DecodeFrom(key);
      }
#ifndef NDEBUG
      Not_drop_counter++;
#endif
      compact->builder->Add(key, input->value());
      //      assert(key.data()[0] == '0');
      // Close output file if it is big enough
      if (compact->builder->FileSize() >=
      compact->compaction->MaxOutputFileSize()) {
        //        assert(key.data()[0] == '0');
        compact->current_output()->largest.DecodeFrom(key);
        status = FinishCompactionOutputFile(compact, input);
        if (!status.ok()) {
          break;
        }
      }
    }
    //    assert(key.data()[0] == '0');
    input->Next();
    //NOTE(ruihong): When the level iterator is invalid it will be deleted and then the key will
    // be invalid also.
    //    assert(key.data()[0] == '0');
  }
  //  reinterpret_cast<leveldb::MergingIterator>
  // You can not call prev here because the iterator is not valid any more
  //  input->Prev();
  //  assert(input->Valid());
#ifndef NDEBUG
printf("For compaction, Total number of key touched is %d, KV left is %d\n", number_of_key,
       Not_drop_counter);
#endif
  //  assert(key.data()[0] == '0');
//  if (status.ok() && shutting_down_.load(std::memory_order_acquire)) {
//    status = Status::IOError("Deleting DB during compaction");
//  }
  if (status.ok() && compact->builder != nullptr) {
    //    assert(key.data()[0] == '0');
    compact->current_output()->largest.DecodeFrom(key);
    status = FinishCompactionOutputFile(compact, input);
  }
  if (status.ok()) {
    status = input->status();
  }
  delete input;
  input = nullptr;

  CompactionStats stats;
//  stats.micros = env_->NowMicros() - start_micros - imm_micros;
//  for (int which = 0; which < 2; which++) {
//    for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
//      stats.bytes_read += compact->compaction->input(which, i)->file_size;
//    }
//  }
//  for (size_t i = 0; i < compact->outputs.size(); i++) {
//    stats.bytes_written += compact->outputs[i].file_size;
//  }
  // TODO: we can remove this lock.
//  undefine_mutex.Lock();
//  stats_[compact->compaction->level() + 1].Add(stats);

  if (status.ok()) {
//    std::unique_lock<std::mutex> l(versions_mtx, std::defer_lock);
    status = InstallCompactionResults(compact, client_ip);
//    InstallSuperVersion();
  }
//  undefine_mutex.Unlock();
//  if (!status.ok()) {
//    RecordBackgroundError(status);
//  }
//  VersionSet::LevelSummaryStorage tmp;
//  Log(options_.info_log, "compacted to: %s", versions_->LevelSummary(&tmp));
  // NOtifying all the waiting threads.
//  write_stall_cv.notify_all();
//  Versionset_Sync_To_Compute(VersionEdit* ve);
  return status;
}
Status Memory_Node_Keeper::DoCompactionWorkWithSubcompaction(
    CompactionState* compact, std::string& client_ip) {
  Compaction* c = compact->compaction;
  c->GenSubcompactionBoundaries();
  auto boundaries = c->GetBoundaries();
  auto sizes = c->GetSizes();
  assert(boundaries->size() == sizes->size() - 1);
  //  int subcompaction_num = std::min((int)c->GetBoundariesNum(), config::MaxSubcompaction);
  if (boundaries->size()<=opts->MaxSubcompaction){
    for (size_t i = 0; i <= boundaries->size(); i++) {
      Slice* start = i == 0 ? nullptr : &(*boundaries)[i - 1];
      Slice* end = i == boundaries->size() ? nullptr : &(*boundaries)[i];
      compact->sub_compact_states.emplace_back(c, start, end, (*sizes)[i]);
    }
  }else{
    //Get output level total file size.
    uint64_t sum = c->GetFileSizesForLevel(1);
    std::list<int> small_files{};
    for (int i=0; i< sizes->size(); i++) {
      if ((*sizes)[i] <= opts->max_file_size/4)
        small_files.push_back(i);
    }
    int big_files_num = boundaries->size() - small_files.size();
    int files_per_subcompaction = big_files_num/opts->MaxSubcompaction + 1;//Due to interger round down, we need add 1.
    double mean = sum * 1.0 / opts->MaxSubcompaction;
    for (size_t i = 0; i <= boundaries->size(); i++) {
      size_t range_size = (*sizes)[i];
      Slice* start = i == 0 ? nullptr : &(*boundaries)[i - 1];
      int files_counter = range_size <= opts->max_file_size/4 ? 0 : 1;// count this file.
      // TODO(Ruihong) make a better strategy to group the boundaries.
      //Version 1
      //      while (i!=boundaries->size() && range_size < mean &&
      //             range_size + (*sizes)[i+1] <= mean + 3*options_.max_file_size/4){
      //        i++;
      //        range_size += (*sizes)[i];
      //      }
      //Version 2
      while (i!=boundaries->size() &&
      (files_counter<files_per_subcompaction ||(*sizes)[i+1] <= opts->max_file_size/4)){
        i++;
        size_t this_file_size = (*sizes)[i];
        range_size += this_file_size;
        // Only increase the file counter when add big file.
        if (this_file_size >= opts->max_file_size/4)
          files_counter++;
      }
      Slice* end = i == boundaries->size() ? nullptr : &(*boundaries)[i];
      compact->sub_compact_states.emplace_back(c, start, end, range_size);
    }

  }
  printf("Subcompaction number is %zu", compact->sub_compact_states.size());
  const size_t num_threads = compact->sub_compact_states.size();
  assert(num_threads > 0);
//  const uint64_t start_micros = env_->NowMicros();

  // Launch a thread for each of subcompactions 1...num_threads-1
  std::vector<port::Thread> thread_pool;
  thread_pool.reserve(num_threads - 1);
  for (size_t i = 1; i < compact->sub_compact_states.size(); i++) {
    thread_pool.emplace_back(&Memory_Node_Keeper::ProcessKeyValueCompaction, this,
                             &compact->sub_compact_states[i]);
  }

  // Always schedule the first subcompaction (whether or not there are also
  // others) in the current thread to be efficient with resources
  ProcessKeyValueCompaction(&compact->sub_compact_states[0]);
  for (auto& thread : thread_pool) {
    thread.join();
  }
//  CompactionStats stats;
////  stats.micros = env_->NowMicros() - start_micros;
//  for (int which = 0; which < 2; which++) {
//    for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
//      stats.bytes_read += compact->compaction->input(which, i)->file_size;
//    }
//  }
//  for (auto iter : compact->sub_compact_states) {
//    for (size_t i = 0; i < iter.outputs.size(); i++) {
//      stats.bytes_written += iter.outputs[i].file_size;
//    }
//  }

  // TODO: we can remove this lock.


  Status status;
  {
//    std::unique_lock<std::mutex> l(superversion_mtx, std::defer_lock);
    status = InstallCompactionResults(compact, client_ip);
//    InstallSuperVersion();
  }


//  if (!status.ok()) {
//    RecordBackgroundError(status);
//  }
//  VersionSet::LevelSummaryStorage tmp;
//  Log(options_.info_log, "compacted to: %s", versions_->LevelSummary(&tmp));
//  // NOtifying all the waiting threads.
//  write_stall_cv.notify_all();
  return status;
}
void Memory_Node_Keeper::ProcessKeyValueCompaction(SubcompactionState* sub_compact){
  assert(sub_compact->builder == nullptr);
  //Start and End are userkeys.
  Slice* start = sub_compact->start;
  Slice* end = sub_compact->end;
//  if (snapshots_.empty()) {
//    sub_compact->smallest_snapshot = versions_->LastSequence();
//  } else {
//    sub_compact->smallest_snapshot = snapshots_.oldest()->sequence_number();
//  }

  Iterator* input = versions_->MakeInputIteratorMemoryServer(sub_compact->compaction);

  // Release mutex while we're actually doing the compaction work
  //  undefine_mutex.Unlock();
  if (start != nullptr) {
    //The compaction range is (start, end]. so we set 0 as look up key sequence.
    InternalKey start_internal(*start, 0, kValueTypeForSeek);
    //tofix(ruihong): too much data copy for the seek here!
    input->Seek(start_internal.Encode());
    // The first key larger or equal to start_internal was covered in the subtask before it.
    input->Next();
  } else {
    input->SeekToFirst();
  }
#ifndef NDEBUG
  int Not_drop_counter = 0;
  int number_of_key = 0;
#endif
  Status status;
  // TODO: try to create two ikey for parsed key, they can in turn represent the current user key
  //  and former one, which can save the data copy overhead.
  ParsedInternalKey ikey;
  std::string current_user_key;

  bool has_current_user_key = false;
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
  Slice key;
  assert(input->Valid());
#ifndef NDEBUG
  std::string last_internal_key;
  printf("first key is %s", input->key().ToString().c_str());
#endif
  while (input->Valid()) {

    key = input->key();
    assert(key.ToString() != last_internal_key);
#ifndef NDEBUG
    if (start){
      assert(internal_comparator_.Compare(key, *start) > 0);
    }
#endif
    //    assert(key.data()[0] == '0');
    //Check whether the output file have too much overlap with level n + 2
    if (sub_compact->compaction->ShouldStopBefore(key) &&
    sub_compact->builder != nullptr) {
      //TODO: record the largest key as the last ikey, find a more efficient way to record
      // the last key of SSTable.
      sub_compact->current_output()->largest.SetFrom(ikey);
      status = FinishCompactionOutputFile(sub_compact, input);
      if (!status.ok()) {
        DEBUG("Should stop status not OK\n");
        break;
      }
    }
    // key merged below!!!
    // Handle key/value, add to state, etc.
    bool drop = false;
    if (!ParseInternalKey(key, &ikey)) {
      // Do not hide error keys
      current_user_key.clear();
      has_current_user_key = false;
      last_sequence_for_key = kMaxSequenceNumber;
    } else {
      if (!has_current_user_key){
        //TODO: can we avoid the data copy here, can we set two buffers in block and make
        // the old user key not be garbage collected so that the old Slice can be
        // directly used here.
        current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
#ifndef NDEBUG
        last_internal_key = key.ToString();
#endif
        has_current_user_key = true;
      }
      else if(user_comparator()->Compare(ikey.user_key, Slice(current_user_key)) !=
      0) {
        // First occurrence of this user key
        current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
#ifndef NDEBUG
        last_internal_key = key.ToString();
#endif
        //        has_current_user_key = true;
        //        last_sequence_for_key = kMaxSequenceNumber;
        // this will result in the key not drop, next if will always be false because of
        // the last_sequence_for_key.
      }else{
        drop = true;
      }

    }
#ifndef NDEBUG
    number_of_key++;
#endif
    if (!drop) {
      // Open output file if necessary
      if (sub_compact->builder == nullptr) {
        status = OpenCompactionOutputFile(sub_compact);
        if (!status.ok()) {
          break;
        }
      }
      if (sub_compact->builder->NumEntries() == 0) {
//        assert(key.data()[0] == '\000');
        sub_compact->current_output()->smallest.DecodeFrom(key);
      }
#ifndef NDEBUG
      Not_drop_counter++;
#endif
      sub_compact->builder->Add(key, input->value());
      //      assert(key.data()[0] == '0');
      // Close output file if it is big enough
      if (sub_compact->builder->FileSize() >=
      sub_compact->compaction->MaxOutputFileSize()) {
//        assert(key.data()[0] == '\000');
        sub_compact->current_output()->largest.DecodeFrom(key);
        assert(!sub_compact->current_output()->largest.Encode().ToString().empty());

        assert(internal_comparator_.Compare(sub_compact->current_output()->largest,
                                            sub_compact->current_output()->smallest)>0);
        status = FinishCompactionOutputFile(sub_compact, input);
        if (!status.ok()) {
          DEBUG("Iterator status is not OK\n");
          break;
        }
      }
    }
    if (end != nullptr &&
    user_comparator()->Compare(ExtractUserKey(key), *end) >= 0) {
      assert(user_comparator()->Compare(ExtractUserKey(key), *end) == 0);
      break;
    }
    //    assert(key.data()[0] == '0');
    input->Next();
    //NOTE(ruihong): When the level iterator is invalid it will be deleted and then the key will
    // be invalid also.
    //    assert(key.data()[0] == '0');

  }
  //  reinterpret_cast<leveldb::MergingIterator>
  // You can not call prev here because the iterator is not valid any more
  //  input->Prev();
  //  assert(input->Valid());
#ifndef NDEBUG
printf("For compaction, Total number of key touched is %d, KV left is %d\n", number_of_key,
       Not_drop_counter);
#endif
  if (status.ok() && sub_compact->builder != nullptr) {
//    assert(key.size()>0);
//    assert(key.data()[0] == '\000');
    sub_compact->current_output()->largest.DecodeFrom(key);// The SSTable for subcompaction range will be (start, end]
    assert(!sub_compact->current_output()->largest.Encode().ToString().empty());
    status = FinishCompactionOutputFile(sub_compact, input);
  }
  if (status.ok()) {
    status = input->status();
  }
  delete input;
  //  input = nullptr;
}
Status Memory_Node_Keeper::OpenCompactionOutputFile(SubcompactionState* compact) {
  assert(compact != nullptr);
  assert(compact->builder == nullptr);
  uint64_t file_number;
  {
    //    undefine_mutex.Lock();
    file_number = versions_->NewFileNumber();
    //    pending_outputs_.insert(file_number);
    CompactionOutput out;
    out.number = file_number;
    out.smallest.Clear();
    out.largest.Clear();
    compact->outputs.push_back(out);
    //    undefine_mutex.Unlock();
  }

  // Make the output file
  //  std::string fname = TableFileName(dbname_, file_number);
  //  Status s = env_->NewWritableFile(fname, &compact->outfile);
  Status s = Status::OK();
  if (s.ok()) {
    compact->builder = new TableBuilder_Memoryside(
        *opts, Compact, rdma_mg);
  }
  return s;
}
Status Memory_Node_Keeper::OpenCompactionOutputFile(CompactionState* compact) {
  assert(compact != nullptr);
  assert(compact->builder == nullptr);
  uint64_t file_number;
  {
    //    undefine_mutex.Lock();
    file_number = versions_->NewFileNumber();
    //    pending_outputs_.insert(file_number);
    CompactionOutput out;
    out.number = file_number;
    out.smallest.Clear();
    out.largest.Clear();
    compact->outputs.push_back(out);
    //    undefine_mutex.Unlock();
  }

  // Make the output file
  //  std::string fname = TableFileName(dbname_, file_number);
  //  Status s = env_->NewWritableFile(fname, &compact->outfile);
  Status s = Status::OK();
  if (s.ok()) {
    compact->builder = new TableBuilder_Memoryside(*opts, Compact, rdma_mg);
  }
//  printf("rep_ is %p", compact->builder->get_filter_map())
  return s;
}
Status Memory_Node_Keeper::FinishCompactionOutputFile(SubcompactionState* compact,
                                          Iterator* input) {
  assert(compact != nullptr);
  //  assert(compact->outfile != nullptr);
  assert(compact->builder != nullptr);

  const uint64_t output_number = compact->current_output()->number;
  assert(!compact->current_output()->largest.Encode().empty());
#ifndef NDEBUG
//  if (output_number == 11 ||output_number == 12 ){
//    printf("Finish Compaction output number is 11 or 12\n");
    printf("File number %lu largest key size is %lu", compact->current_output()->number,
           compact->current_output()->largest.Encode().ToString().size());
//  }
#endif
  assert(output_number != 0);

  // Check for iterator errors
  Status s = input->status();
  const uint64_t current_entries = compact->builder->NumEntries();
  if (s.ok()) {
    s = compact->builder->Finish();
  } else {
    printf("iterator Error!!!!!!!!!!!, Error: %s\n", s.ToString().c_str());
    compact->builder->Abandon();
  }

  compact->builder->get_datablocks_map(compact->current_output()->remote_data_mrs);
  compact->builder->get_dataindexblocks_map(compact->current_output()->remote_dataindex_mrs);
  compact->builder->get_filter_map(compact->current_output()->remote_filter_mrs);
#ifndef NDEBUG
  uint64_t file_size = 0;
  for(auto iter : compact->current_output()->remote_data_mrs){
    file_size += iter.second->length;
  }
#endif
  const uint64_t current_bytes = compact->builder->FileSize();
  compact->current_output()->file_size = current_bytes;
  assert(file_size == current_bytes);
  compact->total_bytes += current_bytes;
  delete compact->builder;
  compact->builder = nullptr;

  // Finish and check for file errors
  //  if (s.ok()) {
  //    s = compact->outfile->Sync();
  //  }
  //  if (s.ok()) {
  //    s = compact->outfile->Close();
  //  }
  //  delete compact->outfile;
  //  compact->outfile = nullptr;

  if (s.ok() && current_entries > 0) {
    // Verify that the table is usable
    //    Iterator* iter =
    //        table_cache_->NewIterator(ReadOptions(), output_number, current_bytes);
    //    s = iter->status();
    //    delete iter;
  }
  return s;
}
Status Memory_Node_Keeper::FinishCompactionOutputFile(CompactionState* compact,
                                          Iterator* input) {
  assert(compact != nullptr);
  //  assert(compact->outfile != nullptr);
  assert(compact->builder != nullptr);

  const uint64_t output_number = compact->current_output()->number;
  assert(output_number != 0);

  // Check for iterator errors
  Status s = input->status();
  const uint64_t current_entries = compact->builder->NumEntries();
  if (s.ok()) {
    s = compact->builder->Finish();
  } else {
    printf("iterator Error!!!!!!!!!!!, Error: %s\n", s.ToString().c_str());
    compact->builder->Abandon();
  }

  compact->builder->get_datablocks_map(compact->current_output()->remote_data_mrs);
  compact->builder->get_dataindexblocks_map(compact->current_output()->remote_dataindex_mrs);
  compact->builder->get_filter_map(compact->current_output()->remote_filter_mrs);
  assert(compact->current_output()->remote_data_mrs.size()>0);
  assert(compact->current_output()->remote_dataindex_mrs.size()>0);
#ifndef NDEBUG
  uint64_t file_size = 0;
  for(auto iter : compact->current_output()->remote_data_mrs){
    file_size += iter.second->length;
  }
#endif
  const uint64_t current_bytes = compact->builder->FileSize();
  compact->current_output()->file_size = current_bytes;
  assert(file_size == current_bytes);
  compact->total_bytes += current_bytes;
  delete compact->builder;
  compact->builder = nullptr;

  // Finish and check for file errors
  //  if (s.ok()) {
  //    s = compact->outfile->Sync();
  //  }
  //  if (s.ok()) {
  //    s = compact->outfile->Close();
  //  }
  //  delete compact->outfile;
  //  compact->outfile = nullptr;

  if (s.ok() && current_entries > 0) {
    // Verify that the table is usable
    //    Iterator* iter =
    //        table_cache_->NewIterator(ReadOptions(), output_number, current_bytes);
    //    s = iter->status();
    //    delete iter;
//    if (s.ok()) {
//      Log(options_.info_log, "Generated table #%llu@%d: %lld keys, %lld bytes",
//          (unsigned long long)output_number, compact->compaction->level(),
//          (unsigned long long)current_entries,
//          (unsigned long long)current_bytes);
//    }
  }
  return s;
}
Status Memory_Node_Keeper::InstallCompactionResults(CompactionState* compact,
                                                    std::string& client_ip) {
//  Log(options_.info_log, "Compacted %d@%d + %d@%d files => %lld bytes",
//      compact->compaction->num_input_files(0), compact->compaction->level(),
//      compact->compaction->num_input_files(1), compact->compaction->level() + 1,
//      static_cast<long long>(compact->total_bytes));

  // Add compaction outputs
compact->compaction->AddInputDeletions(compact->compaction->edit());
  const int level = compact->compaction->level();
  if (compact->sub_compact_states.size() == 0){
    for (size_t i = 0; i < compact->outputs.size(); i++) {
      const CompactionOutput& out = compact->outputs[i];
      std::shared_ptr<RemoteMemTableMetaData> meta = std::make_shared<RemoteMemTableMetaData>(1);
      //TODO make all the metadata written into out
      meta->number = out.number;
      meta->file_size = out.file_size;
      meta->level = level+1;
      meta->smallest = out.smallest;
      assert(!out.largest.Encode().ToString().empty());
      meta->largest = out.largest;
      meta->remote_data_mrs = out.remote_data_mrs;
      meta->remote_dataindex_mrs = out.remote_dataindex_mrs;
      meta->remote_filter_mrs = out.remote_filter_mrs;
      compact->compaction->edit()->AddFile(level + 1, meta);
      assert(!meta->UnderCompaction);
#ifndef NDEBUG

        // Verify that the table is usable
        Iterator* it = versions_->table_cache_->NewIterator_MemorySide(ReadOptions(), meta);
//        s = it->status();

        it->SeekToFirst();
        while(it->Valid()){
          it->Next();
        }
        printf("Table %p Read successfully after compaction\n", meta.get());
        delete it;
#endif
    }
  }else{
    for(auto subcompact : compact->sub_compact_states){
      for (size_t i = 0; i < subcompact.outputs.size(); i++) {
        const CompactionOutput& out = subcompact.outputs[i];
        std::shared_ptr<RemoteMemTableMetaData> meta =
            std::make_shared<RemoteMemTableMetaData>(1);
        // TODO make all the metadata written into out
        meta->number = out.number;
        meta->file_size = out.file_size;
        meta->level = level+1;
        meta->smallest = out.smallest;
        meta->largest = out.largest;
        meta->remote_data_mrs = out.remote_data_mrs;
        meta->remote_dataindex_mrs = out.remote_dataindex_mrs;
        meta->remote_filter_mrs = out.remote_filter_mrs;
        compact->compaction->edit()->AddFile(level + 1, meta);
        assert(!meta->UnderCompaction);
      }
    }
  }
  assert(compact->compaction->edit()->GetNewFilesNum() > 0 );
//  lck_p->lock();
  compact->compaction->ReleaseInputs();
  std::unique_lock<std::mutex> lck(versionset_mtx);
  Status s = versions_->LogAndApply(compact->compaction->edit(), 0);
  versions_->Pin_Version_For_Compute();

  Edit_sync_to_remote(compact->compaction->edit(), client_ip);

  return s;
}
  void Memory_Node_Keeper::server_communication_thread(std::string client_ip,
                                                 int socket_fd) {
    printf("A new shared memory thread start\n");
    printf("checkpoint1");
    char temp_receive[2];
    char temp_send[] = "Q";
    int rc = 0;
    rdma_mg->ConnectQPThroughSocket(client_ip, socket_fd);
    //TODO: use Local_Memory_Allocation to bulk allocate, and assign within this function.
//    ibv_mr send_mr[32] = {};
//    for(int i = 0; i<32; i++){
//      rdma_mg->Allocate_Local_RDMA_Slot(send_mr[i], "message");
//    }

//    char* send_buff;
//    if (!rdma_mg_->Local_Memory_Register(&send_buff, &send_mr, 1000, std::string())) {
//      fprintf(stderr, "memory registering failed by size of 0x%x\n", 1000);
//    }
//    int buffer_number = 32;
    ibv_mr recv_mr[R_SIZE] = {};
    for(int i = 0; i<R_SIZE; i++){
      rdma_mg->Allocate_Local_RDMA_Slot(recv_mr[i], "message");
    }


//    char* recv_buff;
//    if (!rdma_mg_->Local_Memory_Register(&recv_buff, &recv_mr, 1000, std::string())) {
//      fprintf(stderr, "memory registering failed by size of 0x%x\n", 1000);
//    }
    //  post_receive<int>(recv_mr, client_ip);
    for(int i = 0; i<R_SIZE; i++) {
      rdma_mg->post_receive<RDMA_Request>(&recv_mr[i], client_ip);
    }
//    rdma_mg_->post_receive(recv_mr, client_ip, sizeof(Computing_to_memory_msg));
    // sync after send & recv buffer creation and receive request posting.
    //TODO: preallocate a large amount of RDMA registered memory here.
    rdma_mg->local_mem_pool.reserve(100);
    {
      std::unique_lock<std::shared_mutex> lck(rdma_mg->local_mem_mutex);
      rdma_mg->Preregister_Memory(64);
    }
    if (rdma_mg->sock_sync_data(socket_fd, 1, temp_send,
                       temp_receive)) /* just send a dummy char back and forth */
      {
      fprintf(stderr, "sync error after QPs are were moved to RTS\n");
      rc = 1;
      }
      shutdown(socket_fd, 2);
    close(socket_fd);
    //  post_send<int>(res->mr_send, client_ip);
    ibv_wc wc[3] = {};
    //  if(poll_completion(wc, 2, client_ip))
    //    printf("The main qp not create correctly");
    //  else
    //    printf("The main qp not create correctly");
    // Computing node and share memory connection succeed.
    // Now is the communication through rdma.
    RDMA_Request receive_msg_buf;

    //  receive_msg_buf = (computing_to_memory_msg*)recv_buff;
    //  receive_msg_buf->command = ntohl(receive_msg_buf->command);
    //  receive_msg_buf->content.qp_config.qp_num = ntohl(receive_msg_buf->content.qp_config.qp_num);
    //  receive_msg_buf->content.qp_config.lid = ntohs(receive_msg_buf->content.qp_config.lid);
    //  ibv_wc wc[3] = {};
    // TODO: implement a heart beat mechanism.
    int buffer_counter = 0;
    while (true) {
      rdma_mg->poll_completion(wc, 1, client_ip, false);
      memcpy(&receive_msg_buf, recv_mr[buffer_counter].addr, sizeof(RDMA_Request));

      // copy the pointer of receive buf to a new place because
      // it is the same with send buff pointer.
      if (receive_msg_buf.command == create_mr_) {
        rdma_mg->post_receive<RDMA_Request>(&recv_mr[buffer_counter], client_ip);

        create_mr_handler(receive_msg_buf, client_ip);
//        rdma_mg_->post_send<ibv_mr>(send_mr,client_ip);  // note here should be the mr point to the send buffer.
//        rdma_mg_->poll_completion(wc, 1, client_ip, true);
      } else if (receive_msg_buf.command == create_qp_) {
        rdma_mg->post_receive<RDMA_Request>(&recv_mr[buffer_counter], client_ip);
        create_qp_handler(receive_msg_buf, client_ip);
        //        rdma_mg_->post_send<registered_qp_config>(send_mr, client_ip);
//        rdma_mg_->poll_completion(wc, 1, client_ip, true);
      } else if (receive_msg_buf.command == install_version_edit) {
        rdma_mg->post_receive<RDMA_Request>(&recv_mr[buffer_counter], client_ip);
        install_version_edit_handler(receive_msg_buf, client_ip);
//TODO: add a handle function for the option value
      } else if (receive_msg_buf.command == version_unpin_) {
        rdma_mg->post_receive<RDMA_Request>(&recv_mr[buffer_counter], client_ip);
        version_unpin_handler(receive_msg_buf, client_ip);
      } else {
        printf("corrupt message from client.");
        break;
      }
      // increase the buffer index
      if (buffer_counter== R_SIZE-1 ){
        buffer_counter = 0;
      } else{
        buffer_counter++;
      }
    }
    // TODO: Build up a exit method for shared memory side, don't forget to destroy all the RDMA resourses.
  }
  void Memory_Node_Keeper::Server_to_Client_Communication() {
  if (rdma_mg->resources_create()) {
    fprintf(stderr, "failed to create resources\n");
  }
  int rc;
  if (rdma_mg->rdma_config.gid_idx >= 0) {
    printf("checkpoint0");
    rc = ibv_query_gid(rdma_mg->res->ib_ctx, rdma_mg->rdma_config.ib_port,
                       rdma_mg->rdma_config.gid_idx,
                       &(rdma_mg->res->my_gid));
    if (rc) {
      fprintf(stderr, "could not get gid for port %d, index %d\n",
              rdma_mg->rdma_config.ib_port, rdma_mg->rdma_config.gid_idx);
      return;
    }
  } else
    memset(&(rdma_mg->res->my_gid), 0, sizeof rdma_mg->res->my_gid);
  server_sock_connect(rdma_mg->rdma_config.server_name,
                      rdma_mg->rdma_config.tcp_port);
}
// connection code for server side, will get prepared for multiple connection
// on the same port.
int Memory_Node_Keeper::server_sock_connect(const char* servername, int port) {
  struct addrinfo* resolved_addr = NULL;
  struct addrinfo* iterator;
  char service[6];
  int sockfd = -1;
  int listenfd = 0;
  struct sockaddr address;
  socklen_t len = sizeof(struct sockaddr);
  struct addrinfo hints = {
      .ai_flags = AI_PASSIVE, .ai_family = AF_INET, .ai_socktype = SOCK_STREAM};
  if (sprintf(service, "%d", port) < 0) goto sock_connect_exit;
  /* Resolve DNS address, use sockfd as temp storage */
  sockfd = getaddrinfo(servername, service, &hints, &resolved_addr);
  if (sockfd < 0) {
    fprintf(stderr, "%s for %s:%d\n", gai_strerror(sockfd), servername, port);
    goto sock_connect_exit;
  }

  /* Search through results and find the one we want */
  for (iterator = resolved_addr; iterator; iterator = iterator->ai_next) {
    sockfd = socket(iterator->ai_family, iterator->ai_socktype,
                    iterator->ai_protocol);
    int option = 1;
    setsockopt(sockfd,SOL_SOCKET,SO_REUSEADDR,&option,sizeof(int));
    if (sockfd >= 0) {
      /* Server mode. Set up listening socket an accept a connection */
      listenfd = sockfd;
      sockfd = -1;
      if (bind(listenfd, iterator->ai_addr, iterator->ai_addrlen))
        goto sock_connect_exit;
      listen(listenfd, 20);
      while (1) {
        sockfd = accept(listenfd, &address, &len);
        std::string client_id =
            std::string(
                inet_ntoa(((struct sockaddr_in*)(&address))->sin_addr)) +
                    std::to_string(((struct sockaddr_in*)(&address))->sin_port);
        // Client id must be composed of ip address and port number.
        std::cout << "connection built up from" << client_id << std::endl;
        std::cout << "connection family is " << address.sa_family << std::endl;
        if (sockfd < 0) {
          fprintf(stderr, "Connection accept error, erron: %d\n", errno);
          break;
        }
        main_comm_threads.emplace_back(
            [this](std::string client_ip, int socketfd) {
              this->server_communication_thread(client_ip, socketfd);
              },
              std::string(address.sa_data), sockfd);
        // No detach!! we don't want the thread keep running after we exit the
        // main thread.
//        main_comm_threads.back().detach();
      }
    }
  }
  sock_connect_exit:

  if (listenfd) close(listenfd);
  if (resolved_addr) freeaddrinfo(resolved_addr);
  if (sockfd < 0) {
    if (servername)
      fprintf(stderr, "Couldn't connect to %s:%d\n", servername, port);
    else {
      perror("server accept");
      fprintf(stderr, "accept() failed\n");
    }
  }
  return sockfd;
}
  void Memory_Node_Keeper::JoinAllThreads(bool wait_for_jobs_to_complete) {
  message_handler_pool_.JoinThreads(wait_for_jobs_to_complete);
  }
  void Memory_Node_Keeper::create_mr_handler(RDMA_Request request,
                                             std::string& client_ip) {
  std::cout << "create memory region command receive for" << client_ip
  << std::endl;
  //TODO: consider the edianess of the RDMA request and reply.
  ibv_mr send_mr;
  rdma_mg->Allocate_Local_RDMA_Slot(send_mr, "message");
  RDMA_Reply* send_pointer = (RDMA_Reply*)send_mr.addr;

  ibv_mr* mr;
  char* buff;
  {
    std::unique_lock<std::shared_mutex> lck(rdma_mg->local_mem_mutex);
    assert(request.content.mem_size = 1024*1024*1024); // Preallocation requrie memory is 1GB
      if (!rdma_mg->Local_Memory_Register(&buff, &mr, request.content.mem_size,
                                          std::string())) {
        fprintf(stderr, "memory registering failed by size of 0x%x\n",
                static_cast<unsigned>(request.content.mem_size));
      }
//      printf("Now the Remote memory regularated by compute node is %zu GB",
//             rdma_mg->local_mem_pool.size());
  }

  send_pointer->content.mr = *mr;
  send_pointer->received = true;

  rdma_mg->RDMA_Write(request.reply_buffer, request.rkey,
                       &send_mr, sizeof(RDMA_Reply),client_ip, IBV_SEND_SIGNALED,1);
  rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr, "message");
  }
  void Memory_Node_Keeper::create_qp_handler(RDMA_Request request,
                                             std::string& client_ip) {
    int rc;
  assert(request.reply_buffer != nullptr);
  assert(request.rkey != 0);
  char gid_str[17];
  memset(gid_str, 0, 17);
  memcpy(gid_str, request.content.qp_config.gid, 16);
  std::string new_qp_id =
      std::string(gid_str) +
      std::to_string(request.content.qp_config.lid) +
      std::to_string(request.content.qp_config.qp_num);
  std::cout << "create query pair command receive for" << client_ip
  << std::endl;
  fprintf(stdout, "Remote QP number=0x%x\n",
          request.content.qp_config.qp_num);
  fprintf(stdout, "Remote LID = 0x%x\n",
          request.content.qp_config.lid);
  ibv_mr send_mr;
  rdma_mg->Allocate_Local_RDMA_Slot(send_mr, "message");
  RDMA_Reply* send_pointer = (RDMA_Reply*)send_mr.addr;
  ibv_qp* qp = rdma_mg->create_qp(new_qp_id, false);
  if (rdma_mg->rdma_config.gid_idx >= 0) {
    rc = ibv_query_gid(rdma_mg->res->ib_ctx, rdma_mg->rdma_config.ib_port,
                       rdma_mg->rdma_config.gid_idx, &(rdma_mg->res->my_gid));
    if (rc) {
      fprintf(stderr, "could not get gid for port %d, index %d\n",
              rdma_mg->rdma_config.ib_port, rdma_mg->rdma_config.gid_idx);
      return;
    }
  } else
    memset(&(rdma_mg->res->my_gid), 0, sizeof(rdma_mg->res->my_gid));
  /* exchange using TCP sockets info required to connect QPs */
  send_pointer->content.qp_config.qp_num =
      rdma_mg->res->qp_map[new_qp_id]->qp_num;
  send_pointer->content.qp_config.lid = rdma_mg->res->port_attr.lid;
  memcpy(send_pointer->content.qp_config.gid, &(rdma_mg->res->my_gid), 16);
  send_pointer->received = true;
  registered_qp_config* remote_con_data = new registered_qp_config(request.content.qp_config);
  std::shared_lock<std::shared_mutex> l1(rdma_mg->qp_cq_map_mutex);
  if (new_qp_id == "read_local" )
    rdma_mg->local_read_qp_info->Reset(remote_con_data);
  else if(new_qp_id == "write_local_compact")
    rdma_mg->local_write_compact_qp_info->Reset(remote_con_data);
  else if(new_qp_id == "write_local_flush")
    rdma_mg->local_write_flush_qp_info->Reset(remote_con_data);
  else
    rdma_mg->res->qp_connection_info.insert({new_qp_id,remote_con_data});
  rdma_mg->connect_qp(qp, new_qp_id);

  rdma_mg->RDMA_Write(request.reply_buffer, request.rkey,
                       &send_mr, sizeof(RDMA_Reply),client_ip, IBV_SEND_SIGNALED,1);
  rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr, "message");
  }
  void Memory_Node_Keeper::install_version_edit_handler(RDMA_Request request,
                                                        std::string& client_ip) {
  DEBUG("install version\n");
  ibv_mr send_mr;
  rdma_mg->Allocate_Local_RDMA_Slot(send_mr, "message");
  RDMA_Reply* send_pointer = (RDMA_Reply*)send_mr.addr;
  send_pointer->content.ive = {};
  ibv_mr edit_recv_mr;
  rdma_mg->Allocate_Local_RDMA_Slot(edit_recv_mr, "version_edit");
  send_pointer->reply_buffer = edit_recv_mr.addr;
  send_pointer->rkey = edit_recv_mr.rkey;
  send_pointer->received = true;
  //TODO: how to check whether the version edit message is ready, we need to know the size of the
  // version edit in the first REQUEST from compute node.
  volatile char* polling_byte = (char*)edit_recv_mr.addr + request.content.ive.buffer_size;
  memset((void*)polling_byte, 0, 1);
  asm volatile ("sfence\n" : : );
  asm volatile ("lfence\n" : : );
  asm volatile ("mfence\n" : : );
  rdma_mg->RDMA_Write(request.reply_buffer, request.rkey,
                       &send_mr, sizeof(RDMA_Reply),client_ip, IBV_SEND_SIGNALED,1);

  while (*(unsigned char*)polling_byte == 0){
    _mm_clflush(polling_byte);
    asm volatile ("sfence\n" : : );
    asm volatile ("lfence\n" : : );
    asm volatile ("mfence\n" : : );
    std::fprintf(stderr, "Polling install version handler\r");
    std::fflush(stderr);
  }
  VersionEdit version_edit;
  version_edit.DecodeFrom(
      Slice((char*)edit_recv_mr.addr, request.content.ive.buffer_size), 1);
  DEBUG_arg("Version edit decoded, new file number is %zu", version_edit.GetNewFilesNum());
  std::unique_lock<std::mutex> lck(versionset_mtx);
  versions_->LogAndApply(&version_edit, 0);
  lck.unlock();
  MaybeScheduleCompaction(client_ip);

  rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr, "message");
  rdma_mg->Deallocate_Local_RDMA_Slot(edit_recv_mr.addr, "version_edit");
  }
  void Memory_Node_Keeper::version_unpin_handler(RDMA_Request request,
                                                 std::string& client_ip) {
    std::unique_lock<std::mutex> lck(versionset_mtx);
    versions_->Unpin_Version_For_Compute(request.content.unpinned_version_id);
  }
  void Memory_Node_Keeper::Edit_sync_to_remote(VersionEdit* edit,
                                               std::string& client_ip) {
  //  std::unique_lock<std::shared_mutex> l(main_qp_mutex);

//  std::shared_ptr<RDMA_Manager> rdma_mg = env_->rdma_mg;
  // register the memory block from the remote memory
  RDMA_Request* send_pointer;
  ibv_mr send_mr = {};
  ibv_mr send_mr_ve = {};
  ibv_mr receive_mr = {};
  rdma_mg->Allocate_Local_RDMA_Slot(send_mr, "message");

  if (edit->IsTrival()){
    send_pointer = (RDMA_Request*)send_mr.addr;
    send_pointer->command = install_version_edit;
    send_pointer->content.ive.trival = true;
//    send_pointer->content.ive.buffer_size = serilized_ve.size();
    int level;
    uint64_t file_number;
    uint8_t node_id;
    edit->GetTrivalFile(level, file_number,
                        node_id);
    send_pointer->content.ive.level = level;
    send_pointer->content.ive.file_number = file_number;
    send_pointer->content.ive.node_id = node_id;
    send_pointer->content.ive.version_id = versions_->version_id;
    rdma_mg->post_send<RDMA_Request>(&send_mr, client_ip);
    ibv_wc wc[2] = {};
    if (rdma_mg->poll_completion(wc, 1, client_ip,true)){
      fprintf(stderr, "failed to poll send for remote memory register\n");
      return;
    }
  }else{
    rdma_mg->Allocate_Local_RDMA_Slot(send_mr_ve, "version_edit");

    rdma_mg->Allocate_Local_RDMA_Slot(receive_mr, "message");
    std::string serilized_ve;
    edit->EncodeTo(&serilized_ve);
    assert(serilized_ve.size() <= send_mr_ve.length-1);
    uint8_t check_byte = rand()%256;
    memcpy(send_mr_ve.addr, serilized_ve.c_str(), serilized_ve.size());
    memset((char*)send_mr_ve.addr + serilized_ve.size(), check_byte, 1);

    send_pointer = (RDMA_Request*)send_mr.addr;
    send_pointer->command = install_version_edit;
    send_pointer->content.ive.trival = false;
    send_pointer->content.ive.buffer_size = serilized_ve.size();
    send_pointer->content.ive.version_id = versions_->version_id;
    send_pointer->content.ive.check_byte = check_byte;
    send_pointer->reply_buffer = receive_mr.addr;
    send_pointer->rkey = receive_mr.rkey;
    RDMA_Reply* receive_pointer;
    receive_pointer = (RDMA_Reply*)receive_mr.addr;
    //Clear the reply buffer for the polling.
    *receive_pointer = {};
    asm volatile ("sfence\n" : : );
    asm volatile ("lfence\n" : : );
    asm volatile ("mfence\n" : : );
    rdma_mg->post_send<RDMA_Request>(&send_mr, client_ip);
    ibv_wc wc[2] = {};
    if (rdma_mg->poll_completion(wc, 1, client_ip,true)){
      fprintf(stderr, "failed to poll send for remote memory register\n");
      return;
    }
    printf("Request was sent, sub version id is %lu, polled buffer address is %p, checkbyte is %d\n",
           versions_->version_id, &receive_pointer->received, check_byte);
    asm volatile ("sfence\n" : : );
    asm volatile ("lfence\n" : : );
    asm volatile ("mfence\n" : : );
    if(!rdma_mg->poll_reply_buffer(receive_pointer)) // poll the receive for 2 entires
    {
      printf("Reply buffer is %p", receive_pointer->reply_buffer);
      printf("Received is %d", receive_pointer->received);
      printf("receive structure size is %lu", sizeof(RDMA_Reply));
      printf("version id is %lu", versions_->version_id);
      exit(0);
    }

    //Note: here multiple threads will RDMA_Write the "main" qp at the same time,
    // which means the polling result may not belongs to this thread, but it does not
    // matter in our case because we do not care when will the message arrive at the other side.
    asm volatile ("sfence\n" : : );
    asm volatile ("lfence\n" : : );
    asm volatile ("mfence\n" : : );
    rdma_mg->RDMA_Write(receive_pointer->reply_buffer, receive_pointer->rkey,
                        &send_mr_ve, serilized_ve.size() + 1, client_ip, IBV_SEND_SIGNALED,1);
    rdma_mg->Deallocate_Local_RDMA_Slot(send_mr_ve.addr,"version_edit");
    rdma_mg->Deallocate_Local_RDMA_Slot(receive_mr.addr,"message");
  }
  rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr,"message");

  }


  }


