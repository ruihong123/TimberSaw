#include <iostream>
#include "util/rdma.h"

//namespace leveldb{
int main()
{
  auto Remote_Bitmap = new std::map<void*, leveldb::In_Use_Array>;
  auto Read_Bitmap = new std::map<void*, leveldb::In_Use_Array>;
  auto Write_Bitmap = new std::map<void*, leveldb::In_Use_Array>;
  std::string db_name;
  std::unordered_map<std::string, leveldb::SST_Metadata*> file_to_sst_meta;
  std::shared_mutex fs_mutex;
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
  leveldb::RDMA_Manager RDMA_manager(config, table_size, 1);

  RDMA_manager.Server_to_Client_Communication();


  return 0;
}