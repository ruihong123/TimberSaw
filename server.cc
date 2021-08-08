#include <iostream>
#include <memory_node/memory_node_keeper.h>

#include "util/rdma.h"

//namespace leveldb{
int main()
{

  leveldb::Memory_Node_Keeper mn_keeper(true);
  mn_keeper.SetBackgroundThreads(4, leveldb::ThreadPoolType::CompactionThreadPool);
  mn_keeper.Server_to_Client_Communication();

  return 0;
}