#include <iostream>
#include <memory_node/memory_node_keeper.h>

#include "util/rdma.h"

//namespace TimberSaw{
int main()
{ uint32_t tcp_port;
  std::cin >> tcp_port;

  TimberSaw::Memory_Node_Keeper mn_keeper(true, tcp_port);
  mn_keeper.SetBackgroundThreads(12, TimberSaw::ThreadPoolType::CompactionThreadPool);
  mn_keeper.Server_to_Client_Communication();

  return 0;
}