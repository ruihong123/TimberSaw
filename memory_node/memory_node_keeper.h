//
// Created by ruihong on 7/29/21.
//

#ifndef LEVELDB_HOME_NODE_KEEPER_H
#define LEVELDB_HOME_NODE_KEEPER_H

#endif  // LEVELDB_HOME_NODE_KEEPER_H
#include <queue>
#include "util/rdma.h"
#include "util/ThreadPool.h"
#include "db/version_set.h"
namespace leveldb{
class Memory_Node_Keeper {
 public:
//  friend class RDMA_Manager;
  Memory_Node_Keeper();
  void Schedule(
      void (*background_work_function)(void* background_work_arg),
      void* background_work_arg, ThreadPoolType type);
  void JoinAllThreads(bool wait_for_jobs_to_complete);

  // this function is for the server.
  void Server_to_Client_Communication();
  void SetBackgroundThreads(int num,  ThreadPoolType type);

 private:
  std::shared_ptr<RDMA_Manager> rdma_mg_;
  std::vector<std::thread> main_comm_threads;
  ThreadPool message_handler_pool_;
  VersionSet* versions_;
  int server_sock_connect(const char* servername, int port);
  void server_communication_thread(std::string client_ip, int socket_fd);
};
}
