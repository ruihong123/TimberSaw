#include <iostream>
#include <memory_node/memory_node_keeper.h>

#include "util/rdma.h"

//namespace TimberSaw{
int main(int argc,char* argv[])
{
  uint32_t tcp_port;
  int pr_size;
  char* value = argv[1];
  std::stringstream strValue;
  strValue << value;
  strValue >> tcp_port;
  value = argv[2];
  strValue.str("");
  strValue << value;
  strValue >> pr_size;
  TimberSaw::Memory_Node_Keeper mn_keeper(true, tcp_port, pr_size);
  mn_keeper.SetBackgroundThreads(12, TimberSaw::ThreadPoolType::CompactionThreadPool);
  mn_keeper.Server_to_Client_Communication();

  return 0;
}