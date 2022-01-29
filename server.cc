#include <iostream>
#include <memory_node/memory_node_keeper.h>

#include "util/rdma.h"

//namespace TimberSaw{
int main(int argc,char* argv[])
{
  TimberSaw::Memory_Node_Keeper* mn_keeper;
  if (argc == 3){
    uint32_t tcp_port;
    int pr_size;
    char* value = argv[1];
    std::stringstream strValue1;
    strValue1 << value;
    strValue1 >> tcp_port;
    value = argv[2];
    std::stringstream strValue2;
    //  strValue.str("");
    strValue2 << value;
    strValue2 >> pr_size;
     mn_keeper = new TimberSaw::Memory_Node_Keeper(true, tcp_port, pr_size);
  }else{
    mn_keeper = new TimberSaw::Memory_Node_Keeper(true, 19843, 88);
  }

  mn_keeper->SetBackgroundThreads(12, TimberSaw::ThreadPoolType::CompactionThreadPool);
  mn_keeper->Server_to_Client_Communication();
  delete mn_keeper;

  return 0;
}