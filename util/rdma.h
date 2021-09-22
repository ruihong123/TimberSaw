#ifndef RDMA_H
#define RDMA_H



#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <inttypes.h>
#include <endian.h>
#include <byteswap.h>
#include <cassert>
#include <algorithm>

#include <thread>
#include <chrono>
#include <memory>
#include <sstream>
#include <arpa/inet.h>
#include <infiniband/verbs.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include "util/thread_local.h"
#include <atomic>
#include <chrono>
#include <iostream>
#include <map>
#include <unordered_map>
#include <shared_mutex>
#include <vector>
#include <list>
#define _mm_clflush(addr)\
	asm volatile("clflush %0" : "+m" (*(volatile char *)(addr)))
#if __BYTE_ORDER == __LITTLE_ENDIAN
//template <typename T>
//  static inline T hton(T u) {
//  static_assert (CHAR_BIT == 8, "CHAR_BIT != 8");
//
//  union
//  {
//    T u;
//    unsigned char u8[sizeof(T)];
//  } source, dest;
//
//  source.u = u;
//
//  for (size_t k = 0; k < sizeof(T); k++)
//    dest.u8[k] = source.u8[sizeof(T) - k - 1];
//
//  return dest.u;
//}
//  static inline uint64_t htonll(uint64_t x) { return bswap_64(x); }
//  static inline uint64_t ntohll(uint64_t x) { return bswap_64(x); }
#elif __BYTE_ORDER == __BIG_ENDIAN
  static inline uint64_t htonll(uint64_t x) { return x; }
  static inline uint64_t ntohll(uint64_t x) { return x; }
#else
#error __BYTE_ORDER is neither __LITTLE_ENDIAN nor __BIG_ENDIAN
#endif
namespace TimberSaw {
struct config_t {
  const char* dev_name;    /* IB device name */
  const char* server_name; /* server host name */
  u_int32_t tcp_port;      /* server TCP port */
  int ib_port; /* local IB port to work with, or physically port number */
  int gid_idx; /* gid index to use */
  int init_local_buffer_size; /*initial local SST buffer size*/
};
/* structure to exchange data which is needed to connect the QPs */
struct registered_qp_config {
  uint32_t qp_num; /* QP number */
  uint16_t lid;    /* LID of the IB port */
  uint8_t gid[16]; /* gid */
} __attribute__((packed));
struct install_versionedit {
  bool trival;
  size_t buffer_size;
  size_t version_id;
  uint8_t check_byte;
  int level;
  uint64_t file_number;
  uint8_t node_id;
} __attribute__((packed));
enum RDMA_Command_Type {
  create_qp_,
  create_mr_,
  install_version_edit,
  version_unpin_,
  sync_option,
  save_fs_serialized_data,
  retrieve_fs_serialized_data,
  save_log_serialized_data,
  retrieve_log_serialized_data
};
enum file_type { log_type, others };
struct fs_sync_command {
  int data_size;
  file_type type;
};
//TODO (ruihong): add the reply message address to avoid request&response conflict for the same queue pair.
// In other word, the threads will not need to figure out whether this message is a reply or response,
// when receive a message from the main queue pair.
union RDMA_Request_Content {
  size_t mem_size;
  registered_qp_config qp_config;
  fs_sync_command fs_sync_cmd;
  install_versionedit ive;
  size_t unpinned_version_id;
};
union RDMA_Reply_Content {
  ibv_mr mr;
  registered_qp_config qp_config;
  install_versionedit ive;
};
struct RDMA_Request {
  RDMA_Command_Type command;
  RDMA_Request_Content content;
  void* reply_buffer;
  uint32_t rkey;
} __attribute__((packed));

struct RDMA_Reply {
//  RDMA_Command_Type command;
  RDMA_Reply_Content content;
  void* reply_buffer;
  uint32_t rkey;
  bool received;
} __attribute__((packed));
// Structure for the file handle in RDMA file system. it could be a link list
// for large files
struct SST_Metadata {
  std::shared_mutex file_lock;
  std::string fname;
  ibv_mr* mr;
  ibv_mr* map_pointer;
  SST_Metadata* last_ptr = nullptr;
  SST_Metadata* next_ptr = nullptr;
  unsigned int file_size = 0;
};
template <typename T>
struct atomwrapper {
  std::atomic<T> _a;

  atomwrapper() : _a() {}

  atomwrapper(const std::atomic<T>& a) : _a(a.load()) {}

  atomwrapper(const atomwrapper& other) : _a(other._a.load()) {}

  atomwrapper& operator=(const atomwrapper& other) {
    _a.store(other._a.load());
  }
};

class In_Use_Array {
 public:
  In_Use_Array(size_t size, size_t chunk_size, ibv_mr* mr_ori)
      : element_size_(size), chunk_size_(chunk_size), mr_ori_(mr_ori) {
    in_use_ = new std::atomic<bool>[element_size_];
    for (size_t i = 0; i < element_size_; ++i) {
      in_use_[i] = false;
    }
  }
  In_Use_Array(size_t size, size_t chunk_size, ibv_mr* mr_ori,
               std::atomic<bool>* in_use)
      : element_size_(size),
        chunk_size_(chunk_size),
        in_use_(in_use),
        mr_ori_(mr_ori) {}
  int allocate_memory_slot() {
    for (int i = 0; i < static_cast<int>(element_size_); ++i) {
      //      auto start = std::chrono::high_resolution_clock::now();
      bool temp = in_use_[i];
      if (temp == false) {
        //        auto stop = std::chrono::high_resolution_clock::now();
        //        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start); std::printf("Compare and swap time duration is %ld \n", duration.count());
        if (in_use_[i].compare_exchange_strong(temp, true)) {
          //          std::cout << "chunk" <<i << "was changed to true" << std::endl;

          return i;  // find the empty slot then return the index for the slot
        }
        //        else
        //          std::cout << "Compare and swap fail" << "i equals" << i  << "type is" << type_ << std::endl;
      }
    }
    return -1;  // Not find the empty memory chunk.
  }
  bool deallocate_memory_slot(int index) {
    bool temp = true;
    assert(in_use_[index] == true);
    assert(index < element_size_);
    //    std::cout << "chunk" <<index << "was changed to false" << std::endl;
    while (!in_use_[index].compare_exchange_strong(temp, false));
    return true;
  }
  size_t get_chunk_size() { return chunk_size_; }
  ibv_mr* get_mr_ori() { return mr_ori_; }
  size_t get_element_size() { return element_size_; }
  std::atomic<bool>* get_inuse_table() { return in_use_; }
  //  void deserialization(char*& temp, int& size){
  //
  //
  //  }
 private:
  size_t element_size_;
  size_t chunk_size_;
  std::atomic<bool>* in_use_;
  ibv_mr* mr_ori_;
  //  int type_;
};
/* structure of system resources */
struct resources {
  union ibv_gid my_gid;
  struct ibv_device_attr device_attr;
  /* Device attributes */
  struct ibv_sge* sge = nullptr;
  struct ibv_recv_wr* rr = nullptr;
  struct ibv_port_attr port_attr; /* IB port attributes */
  //  std::vector<registered_qp_config> remote_mem_regions; /* memory buffers for RDMA */
  struct ibv_context* ib_ctx = nullptr;  /* device handle */
  struct ibv_pd* pd = nullptr;           /* PD handle */
  std::map<std::string, std::pair<ibv_cq*, ibv_cq*>> cq_map; /* CQ Map */
  std::map<std::string, ibv_qp*> qp_map; /* QP Map */
  std::map<std::string, registered_qp_config*> qp_connection_info;
  struct ibv_mr* mr_receive = nullptr;   /* MR handle for receive_buf */
  struct ibv_mr* mr_send = nullptr;      /* MR handle for send_buf */
  //  struct ibv_mr* mr_SST = nullptr;                        /* MR handle for SST_buf */ struct ibv_mr* mr_remote;                     /* remote MR handle for computing node */
  char* SST_buf = nullptr;     /* SSTable buffer pools pointer, it could contain
                                  multiple SSTbuffers */
  char* send_buf = nullptr;    /* SEND buffer pools pointer, it could contain
                                  multiple SEND buffers */
  char* receive_buf = nullptr; /* receive buffer pool pointer,  it could contain
                                  multiple acturall receive buffers */
  std::map<std::string, int> sock_map; /* TCP socket file descriptor */
  std::map<std::string, ibv_mr*> mr_receive_map;
  std::map<std::string, ibv_mr*> mr_send_map;
};
struct IBV_Deleter {
  // Called by unique_ptr to destroy/free the Resource
  void operator()(ibv_mr* r) {
    if (r) {
      void* pointer = r->addr;
      ibv_dereg_mr(r);
      free(pointer);
    }
  }
};

// class QP_Deleter{
// public:
//    void operator()(ibv_qp* ptr){
//        if (ptr == nullptr)
//            return;
//        else if (ibv_destroy_qp(static_cast<ibv_qp*>(ptr))) {
//            fprintf(stderr, "Thread local qp failed to destroy QP\n");
//        }
//        else{
//            printf("thread local qp destroy successfully!");
//        }
//    }
//};
// class CQ_Deleter {
// public:
//    void operator()(ibv_cq* ptr) {
//        if (ptr == nullptr)
//            return;
//        if (ibv_destroy_cq(static_cast<ibv_cq *>(ptr))) {
//            fprintf(stderr, "Thread local cq failed to destroy QP\n");
//        } else {
//            printf("thread local cq destroy successfully!");
//        }
//    }
//};
// QP_Deleter qpdeleter;
// CQ_Deleter cqdeleter;
class Memory_Node_Keeper;
class RDMA_Manager {

 public:
  friend class Memory_Node_Keeper;
  friend class DBImpl;
  RDMA_Manager(config_t config, size_t remote_block_size, uint8_t nodeid);
  //  RDMA_Manager(config_t config) : rdma_config(config){
  //    res = new resources();
  //    res->sock = -1;
  //  }
  //  RDMA_Manager()=delete;
  ~RDMA_Manager();
  // RDMA set up create all the resources, and create one query pair for RDMA send & Receive.
  void Client_Set_Up_Resources();
  // Set up the socket connection to remote shared memory.
  bool Get_Remote_qp_Info_Then_Connect();
  // client function to retrieve serialized data.
  //  bool client_retrieve_serialized_data(const std::string& db_name, char*& buff,
  //                                       size_t& buff_size, ibv_mr*& local_data_mr,
  //                                       file_type type);
  //  // client function to save serialized data.
  //  bool client_save_serialized_data(const std::string& db_name, char* buff,
  //                                   size_t buff_size, file_type type,
  //                                   ibv_mr* local_data_mr);
  void client_message_polling_thread();
  void ConnectQPThroughSocket(std::string client_ip, int socket_fd);
  // Local memory register will register RDMA memory in local machine,
  // Both Computing node and share memory will call this function.
  // it also push the new block bit map to the Remote_Mem_Bitmap

  // Set the type of the memory pool. the mempool can be access by the pool name
  bool Mempool_initialize(std::string pool_name, size_t size);
  //Allocate memory as "size", then slice the whole region into small chunks according to the pool name
  bool Local_Memory_Register(
      char** p2buffpointer, ibv_mr** p2mrpointer, size_t size,
      std::string pool_name);  // register the memory on the local side

  bool Preregister_Memory(int gb_number); //Pre register the memroy do not allocate bit map
  // Remote Memory registering will call RDMA send and receive to the remote memory it also push the new SST bit map to the Remote_Mem_Bitmap
  bool Remote_Memory_Register(size_t size);
  int Remote_Memory_Deregister();
  // new query pair creation and connection to remote Memory by RDMA send and receive
  bool Remote_Query_Pair_Connection(
      std::string& qp_id);  // Only called by client.

  int RDMA_Read(ibv_mr* remote_mr, ibv_mr* local_mr, size_t msg_size,
                std::string q_id, size_t send_flag, int poll_num);
  int RDMA_Write(ibv_mr* remote_mr, ibv_mr* local_mr, size_t msg_size,
                 std::string q_id, size_t send_flag, int poll_num);
  int RDMA_Write(void* addr, uint32_t rkey, ibv_mr* local_mr, size_t msg_size,
                 std::string q_id, size_t send_flag, int poll_num);
  // the coder need to figure out whether the queue pair has two seperated queue,
  // if not, only send_cq==true is a valid option.
  // For a thread-local queue pair, the send_cq does not matter.
  int poll_completion(ibv_wc* wc_p, int num_entries, std::string q_id,
                      bool send_cq);
  bool Deallocate_Local_RDMA_Slot(ibv_mr* mr, ibv_mr* map_pointer,
                                  std::string buffer_type);
  bool Deallocate_Local_RDMA_Slot(void* p, const std::string& buff_type);
  //  bool Deallocate_Remote_RDMA_Slot(SST_Metadata* sst_meta);
  bool Deallocate_Remote_RDMA_Slot(void* p);
  //TOFIX: There will be memory leak for the remote_mr and mr_input for local/remote memory
  // allocation.
  void Allocate_Remote_RDMA_Slot(ibv_mr& remote_mr);
  void Allocate_Local_RDMA_Slot(ibv_mr& mr_input, std::string pool_name);
  // this function will determine whether the pointer is with in the registered memory
  bool CheckInsideLocalBuff(
      void* p,
      std::_Rb_tree_iterator<std::pair<void* const, In_Use_Array>>& mr_iter,
      std::map<void*, In_Use_Array>* Bitmap);
  bool CheckInsideRemoteBuff(void* p);
  void mr_serialization(char*& temp, size_t& size, ibv_mr* mr);
  void mr_deserialization(char*& temp, size_t& size, ibv_mr*& mr);
  int try_poll_this_thread_completions(ibv_wc* wc_p, int num_entries,
                                       std::string q_id, bool send_cq);
  void fs_serialization(
      char*& buff, size_t& size, std::string& db_name,
      std::unordered_map<std::string, SST_Metadata*>& file_to_sst_meta,
      std::map<void*, In_Use_Array>& remote_mem_bitmap);
  // Deserialization for linked file is problematic because different file may link to the same SSTdata
  void fs_deserilization(
      char*& buff, size_t& size, std::string& db_name,
      std::unordered_map<std::string, SST_Metadata*>& file_to_sst_meta,
      std::map<void*, In_Use_Array>& remote_mem_bitmap, ibv_mr* local_mr);
  //  void mem_pool_serialization
  bool poll_reply_buffer(RDMA_Reply* rdma_reply);
  // TODO: Make all the variable more smart pointers.
  resources* res = nullptr;
  std::vector<ibv_mr*>
      remote_mem_pool; /* a vector for all the remote memory regions*/
  std::vector<ibv_mr*>
      local_mem_pool; /* a vector for all the local memory regions.*/
  std::list<ibv_mr*> pre_allocated_pool;
  std::map<void*, In_Use_Array>* Remote_Mem_Bitmap;
  size_t total_registered_size;
  //  std::shared_mutex remote_pool_mutex;
  //  std::map<void*, In_Use_Array>* Write_Local_Mem_Bitmap = nullptr;
  ////  std::shared_mutex write_pool_mutex;
  //  std::map<void*, In_Use_Array>* Read_Local_Mem_Bitmap = nullptr;
  //  std::shared_mutex read_pool_mutex;
  //  size_t Read_Block_Size;
  //  size_t Write_Block_Size;
  uint64_t Table_Size;
  std::shared_mutex remote_mem_mutex;

  std::shared_mutex rw_mutex;
//  std::shared_mutex main_qp_mutex;
  std::shared_mutex qp_cq_map_mutex;
  //  ThreadLocalPtr* t_local_1;
  ThreadLocalPtr* qp_local_write_flush;
  ThreadLocalPtr* cq_local_write_flush;
  ThreadLocalPtr* local_write_flush_qp_info;
  ThreadLocalPtr* qp_local_write_compact;
  ThreadLocalPtr* cq_local_write_compact;
  ThreadLocalPtr* local_write_compact_qp_info;
  ThreadLocalPtr* qp_local_read;
  ThreadLocalPtr* cq_local_read;
  ThreadLocalPtr* local_read_qp_info;
  //  thread_local static std::unique_ptr<ibv_qp, QP_Deleter> qp_local_write_flush;
  //  thread_local static std::unique_ptr<ibv_cq, CQ_Deleter> cq_local_write_flush;
  std::unordered_map<std::string, std::map<void*, In_Use_Array>>
      name_to_mem_pool;
  std::unordered_map<std::string, size_t> name_to_size;
  std::shared_mutex local_mem_mutex;
  uint8_t node_id;

#ifdef PROCESSANALYSIS
  static std::atomic<uint64_t> RDMAReadTimeElapseSum;
  static std::atomic<uint64_t> ReadCount;
#endif
  //  std::unordered_map<std::string, ibv_mr*> fs_image;
  //  std::unordered_map<std::string, ibv_mr*> log_image;
  //  std::unique_ptr<ibv_mr, IBV_Deleter> log_image_mr;
  //  std::shared_mutex log_image_mutex;
  //  std::shared_mutex fs_image_mutex;
  // use thread local qp and cq instead of map, this could be lock free.
  //  static __thread std::string thread_id;
  template <typename T>
  int post_send(ibv_mr* mr, std::string qp_id = "main"){
    struct ibv_send_wr sr;
    struct ibv_sge sge;
    struct ibv_send_wr* bad_wr = NULL;
    int rc;
    //  if (!rdma_config.server_name) {
    // server side.
    /* prepare the scatter/gather entry */
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)mr->addr;
    sge.length = sizeof(T);
    sge.lkey = mr->lkey;
    //  }
    //  else {
    //    //client side
    //    /* prepare the scatter/gather entry */
    //    memset(&sge, 0, sizeof(sge));
    //    sge.addr = (uintptr_t)res->send_buf;
    //    sge.length = sizeof(T);
    //    sge.lkey = res->mr_send->lkey;
    //  }

    /* prepare the send work request */
    memset(&sr, 0, sizeof(sr));
    sr.next = NULL;
    sr.wr_id = 0;
    sr.sg_list = &sge;
    sr.num_sge = 1;
    sr.opcode = static_cast<ibv_wr_opcode>(IBV_WR_SEND);
    sr.send_flags = IBV_SEND_SIGNALED;

    /* there is a Receive Request in the responder side, so we won't get any into RNR flow */
    //*(start) = std::chrono::steady_clock::now();
    // start = std::chrono::steady_clock::now();

    if (rdma_config.server_name)
      rc = ibv_post_send(res->qp_map["main"], &sr, &bad_wr);
    else
      rc = ibv_post_send(res->qp_map[qp_id], &sr, &bad_wr);
//    if (rc)
//      fprintf(stderr, "failed to post SR\n");
//    else {
//      fprintf(stdout, "Send Request was posted\n");
//    }
    return rc;}

  // three variables below are from rdma file system.
  //  std::string* db_name_;
  //  std::unordered_map<std::string, SST_Metadata*>* file_to_sst_meta_;
  //  std::shared_mutex* fs_mutex_;


 private:
  config_t rdma_config;
  int client_sock_connect(const char* servername, int port);

  int sock_sync_data(int sock, int xfer_size, char* local_data,
                     char* remote_data);

  int post_send(ibv_mr* mr, std::string q_id = "main", size_t size = 0);
  //  int post_receives(int len);

  int post_receive(ibv_mr* mr, std::string q_id = "main", size_t size = 0);

  int resources_create();
  int modify_qp_to_reset(ibv_qp* qp);
  int modify_qp_to_init(struct ibv_qp* qp);
  int modify_qp_to_rtr(struct ibv_qp* qp, uint32_t remote_qpn, uint16_t dlid,
                       uint8_t* dgid);
  int modify_qp_to_rts(struct ibv_qp* qp);
  ibv_qp* create_qp(std::string& id, bool seperated_cq);

  //q_id is for the remote qp informantion fetching
  int connect_qp(ibv_qp* qp, std::string& q_id);
  int connect_qp(ibv_qp* qp, registered_qp_config* remote_con_data);
  int resources_destroy();
  void print_config(void);
  void usage(const char* argv0);

  int post_receive(ibv_mr** mr_list, size_t sge_size, std::string q_id);
  int post_send(ibv_mr** mr_list, size_t sge_size, std::string q_id);
  template <typename T>
  int post_receive(ibv_mr* mr, std::string qp_id = "main"){
    struct ibv_recv_wr rr;
    struct ibv_sge sge;
    struct ibv_recv_wr* bad_wr;
    int rc;
    //  if (!rdma_config.server_name) {
    //    /* prepare the scatter/gather entry */

    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)mr->addr;
    sge.length = sizeof(T);
    sge.lkey = mr->lkey;

    //  }
    //  else {
    //    /* prepare the scatter/gather entry */
    //    memset(&sge, 0, sizeof(sge));
    //    sge.addr = (uintptr_t)res->receive_buf;
    //    sge.length = sizeof(T);
    //    sge.lkey = res->mr_receive->lkey;
    //  }

    /* prepare the receive work request */
    memset(&rr, 0, sizeof(rr));
    rr.next = NULL;
    rr.wr_id = 0;
    rr.sg_list = &sge;
    rr.num_sge = 1;
    /* post the Receive Request to the RQ */
    if (rdma_config.server_name)
      rc = ibv_post_recv(res->qp_map["main"], &rr, &bad_wr);
    else
      rc = ibv_post_recv(res->qp_map[qp_id], &rr, &bad_wr);
//    if (rc)
//#ifndef NDEBUG
//      fprintf(stderr, "failed to post RR\n");
//#endif
//    else
//#ifndef NDEBUG
//      fprintf(stdout, "Receive Request was posted\n");
//#endif
    return rc;
  }  // For a non-thread-local queue pair, send_cq==true poll the cq of send queue, send_cq==false poll the cq of receive queue
};

}
#endif