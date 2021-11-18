//
// Created by ruihong on 7/29/21.
//

#ifndef TimberSaw_HOME_NODE_KEEPER_H
#define TimberSaw_HOME_NODE_KEEPER_H


#include <queue>
#include "util/rdma.h"
#include "util/ThreadPool.h"
#include "db/version_set.h"

namespace TimberSaw {
class Memory_Node_Keeper {
 public:
//  friend class RDMA_Manager;
  Memory_Node_Keeper(bool use_sub_compaction);
  ~Memory_Node_Keeper();
//  void Schedule(
//      void (*background_work_function)(void* background_work_arg),
//      void* background_work_arg, ThreadPoolType type);
  void JoinAllThreads(bool wait_for_jobs_to_complete);

  // this function is for the server.
  void Server_to_Client_Communication();
  void SetBackgroundThreads(int num,  ThreadPoolType type);
  void MaybeScheduleCompaction(std::string& client_ip);
  static void BGWork_Compaction(void* thread_args);
  void BackgroundCompaction(void* p);
  void CleanupCompaction(CompactionState* compact);
  void PersistSSTables(VersionEdit& ve);
  Status DoCompactionWork(CompactionState* compact, std::string& client_ip);
  void ProcessKeyValueCompaction(SubcompactionState* sub_compact);
  Status DoCompactionWorkWithSubcompaction(CompactionState* compact,
                                           std::string& client_ip);
  Status OpenCompactionOutputFile(SubcompactionState* compact);
  Status OpenCompactionOutputFile(CompactionState* compact);
  Status FinishCompactionOutputFile(SubcompactionState* compact,
                                    Iterator* input);
  Status FinishCompactionOutputFile(CompactionState* compact, Iterator* input);
  static std::shared_ptr<RDMA_Manager> rdma_mg;
 private:
  std::shared_ptr<Options> opts;
  const InternalKeyComparator internal_comparator_;
//  const InternalFilterPolicy internal_filter_policy_;
  bool usesubcompaction;
  TableCache* const table_cache_;
  std::vector<std::thread> main_comm_threads;
  ThreadPool Compactor_pool_;
  ThreadPool Message_handler_pool_;
  std::mutex versionset_mtx;
  VersionSet* versions_;


  Status InstallCompactionResults(CompactionState* compact,
                                  std::string& client_ip);
  int server_sock_connect(const char* servername, int port);
  void server_communication_thread(std::string client_ip, int socket_fd);
  void create_mr_handler(RDMA_Request request, std::string& client_ip);
  void create_qp_handler(RDMA_Request request, std::string& client_ip);
  const Comparator* user_comparator() const {
    return internal_comparator_.user_comparator();
  }
  void install_version_edit_handler(RDMA_Request request, std::string& client_ip);
  void qp_reset_handler(RDMA_Request request, std::string& client_ip,
                        int socket_fd);
  void sync_option_handler(RDMA_Request request, std::string& client_ip);
  void version_unpin_handler(RDMA_Request request, std::string& client_ip);
  void Edit_sync_to_remote(VersionEdit* edit, std::string& client_ip,
                           std::unique_lock<std::mutex>* version_mtx);
};
}
#endif  // TimberSaw_HOME_NODE_KEEPER_H