//
// Created by ruihong on 7/29/21.
//
#include "memory_node/memory_node_keeper.h"
#include "table/table_builder_memoryside.h"
#include "db/table_cache.h"
#define R_SIZE 32
namespace leveldb{
std::shared_ptr<RDMA_Manager> Memory_Node_Keeper::rdma_mg = std::shared_ptr<RDMA_Manager>();
leveldb::Memory_Node_Keeper::Memory_Node_Keeper(bool use_sub_compaction): opts(std::make_shared<Options>(true)),
usesubcompaction(use_sub_compaction), table_cache_(new TableCache("home_node", *opts, opts->max_open_files)), internal_comparator_(BytewiseComparator()),
versions_(new VersionSet("home_node", opts.get(), table_cache_, &internal_comparator_, &dummy_superversions_mtx)){
    struct leveldb::config_t config = {
        NULL,  /* dev_name */
        NULL,  /* server_name */
        19801, /* tcp_port */
        1,	 /* ib_port */
        1, /* gid_idx */
        0};
    //  size_t write_block_size = 4*1024*1024;
    //  size_t read_block_size = 4*1024;
    size_t table_size = 10*1024*1024;
    rdma_mg = std::make_shared<RDMA_Manager>(config, table_size, 1); //set memory server node id as 1.
    rdma_mg->Mempool_initialize(std::string("FlushBuffer"), RDMA_WRITE_BLOCK);
    //TODO: add a handle function for the option value to get the non-default bloombits.
    opts->filter_policy = NewBloomFilterPolicy(opts->bloom_bits);

  }
  void leveldb::Memory_Node_Keeper::Schedule(void (*background_work_function)(void*),
                                             void* background_work_arg,
                                             ThreadPoolType type) {
    message_handler_pool_.Schedule(background_work_function, background_work_arg);
  }
  void Memory_Node_Keeper::SetBackgroundThreads(int num, ThreadPoolType type) {
    message_handler_pool_.SetBackgroundThreads(num);
  }
  void Memory_Node_Keeper::MaybeScheduleCompaction() {
    if (versions_->NeedsCompaction()) {
      //    background_compaction_scheduled_ = true;
      void* function_args = nullptr;
      BGThreadMetadata* thread_pool_args = new BGThreadMetadata{.db = this, .func_args = function_args};
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
      c->edit()->RemoveFile(c->level(), f->number, rdma_mg->node_id);
      c->edit()->AddFile(c->level() + 1, f);
      {
//        std::unique_lock<std::mutex> l(versions_mtx);// TODO(ruihong): remove all the superversion mutex usage.
        c->ReleaseInputs();
        status = versions_->LogAndApply(c->edit());
//        InstallSuperVersion();
      }

      DEBUG("Trival compaction\n");
    } else {
      CompactionState* compact = new CompactionState(c);

      auto start = std::chrono::high_resolution_clock::now();
      //      write_stall_mutex_.AssertNotHeld();
      // Only when there is enough input level files and output level files will the subcompaction triggered
      if (usesubcompaction && c->num_input_files(0)>=4 && c->num_input_files(1)>1){
//        status = DoCompactionWorkWithSubcompaction(compact);
      }else{
        status = DoCompactionWork(compact);
      }

      auto stop = std::chrono::high_resolution_clock::now();
      auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
      printf("Table compaction time elapse (%ld) us, compaction level is %d, first level file number %d, the second level file number %d \n",
             duration.count(), compact->compaction->level(), compact->compaction->num_input_files(0),compact->compaction->num_input_files(1) );
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
  MaybeScheduleCompaction();

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
Status Memory_Node_Keeper::DoCompactionWork(CompactionState* compact) {
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
    status = InstallCompactionResults(compact);
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
        *opts, Compact, std::shared_ptr<RDMA_Manager>());
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
Status Memory_Node_Keeper::InstallCompactionResults(CompactionState* compact) {
//  Log(options_.info_log, "Compacted %d@%d + %d@%d files => %lld bytes",
//      compact->compaction->num_input_files(0), compact->compaction->level(),
//      compact->compaction->num_input_files(1), compact->compaction->level() + 1,
//      static_cast<long long>(compact->total_bytes));

  // Add compaction outputs
compact->compaction->AddInputDeletions(compact->compaction->edit(), rdma_mg->node_id);
  const int level = compact->compaction->level();
  if (compact->sub_compact_states.size() == 0){
    for (size_t i = 0; i < compact->outputs.size(); i++) {
      const CompactionOutput& out = compact->outputs[i];
      std::shared_ptr<RemoteMemTableMetaData> meta = std::make_shared<RemoteMemTableMetaData>(1);
      //TODO make all the metadata written into out
      meta->number = out.number;
      meta->file_size = out.file_size;
      meta->smallest = out.smallest;
      meta->largest = out.largest;
      meta->remote_data_mrs = out.remote_data_mrs;
      meta->remote_dataindex_mrs = out.remote_dataindex_mrs;
      meta->remote_filter_mrs = out.remote_filter_mrs;
      compact->compaction->edit()->AddFile(level + 1, meta);
      assert(!meta->UnderCompaction);
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
  return versions_->LogAndApply(compact->compaction->edit());
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
    ibv_mr send_mr[32] = {};
    for(int i = 0; i<32; i++){
      rdma_mg->Allocate_Local_RDMA_Slot(send_mr[i], "message");
    }

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
        main_comm_threads.push_back(std::thread(
            [this](std::string client_ip, int socketfd) {
              this->server_communication_thread(client_ip, socketfd);
              },
              std::string(address.sa_data), sockfd));
        //        thread_pool.back().detach();
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
  if (!rdma_mg->Local_Memory_Register(&buff, &mr, request.content.mem_size,
                                       std::string())) {
    fprintf(stderr, "memory registering failed by size of 0x%x\n",
            static_cast<unsigned>(request.content.mem_size));
  }
  printf("Now the total Registered memory is %zu GB",
         rdma_mg->local_mem_pool.size());
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
  rdma_mg->connect_qp(request.content.qp_config, qp);

  rdma_mg->RDMA_Write(request.reply_buffer, request.rkey,
                       &send_mr, sizeof(RDMA_Reply),client_ip, IBV_SEND_SIGNALED,1);
  rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr, "message");
  }
  void Memory_Node_Keeper::install_version_edit_handler(RDMA_Request request,
                                                        std::string& client_ip) {
  ibv_mr send_mr;
  rdma_mg->Allocate_Local_RDMA_Slot(send_mr, "message");
  RDMA_Reply* send_pointer = (RDMA_Reply*)send_mr.addr;
  send_pointer->content.ive = {};
  ibv_mr edit_recv_mr;
  rdma_mg->Allocate_Local_RDMA_Slot(edit_recv_mr, "version_edit");
  send_pointer->reply_buffer = edit_recv_mr.addr;
  send_pointer->rkey = edit_recv_mr.rkey;
  //TODO: how to check whether the version edit message is ready, we need to know the size of the
  // version edit in the first REQUEST from compute node.
  char* polling_bit = (char*)edit_recv_mr.addr + request.content.ive.buffer_size;
  memset(polling_bit, 0, 1);
  rdma_mg->RDMA_Write(request.reply_buffer, request.rkey,
                       &send_mr, sizeof(RDMA_Reply),client_ip, IBV_SEND_SIGNALED,1);
  while (*polling_bit == 0){}
  VersionEdit version_edit;
  version_edit.DecodeFrom(
      Slice((char*)edit_recv_mr.addr, request.content.ive.buffer_size), 1,
      std::shared_ptr<RDMA_Manager>());
  DEBUG_arg("Version edit decoded, new file number is %zu", version_edit.GetNewFilesNum());
//  std::unique_lock<std::mutex> lck(versions_mtx);
  versions_->LogAndApply(&version_edit);
//  lck.unlock();
  MaybeScheduleCompaction();

  rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr, "message");
  rdma_mg->Deallocate_Local_RDMA_Slot(edit_recv_mr.addr, "version_edit");
  }

  }


