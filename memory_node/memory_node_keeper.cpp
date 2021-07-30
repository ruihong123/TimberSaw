//
// Created by ruihong on 7/29/21.
//
#include "memory_node_keeper.h"
namespace leveldb{
leveldb::Memory_Node_Keeper::Memory_Node_Keeper() {
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
    rdma_mg_ = std::make_shared<RDMA_Manager>(config, table_size);

  }
  void leveldb::Memory_Node_Keeper::Schedule(void (*background_work_function)(void*),
                                             void* background_work_arg,
                                             ThreadPoolType type) {
    message_handler_pool_.Schedule(background_work_function, background_work_arg);
  }
  void Memory_Node_Keeper::SetBackgroundThreads(int num, ThreadPoolType type) {
    message_handler_pool_.SetBackgroundThreads(num);
  }
  void Memory_Node_Keeper::server_communication_thread(std::string client_ip,
                                                 int socket_fd) {
    printf("A new shared memory thread start\n");
    printf("checkpoint1");
    char temp_receive[2];
    char temp_send[] = "Q";
    int rc = 0;
    ibv_mr* send_mr;
    char* send_buff;
    if (!rdma_mg_->Local_Memory_Register(&send_buff, &send_mr, 1000, std::string())) {
      fprintf(stderr, "memory registering failed by size of 0x%x\n", 1000);
    }
    ibv_mr* recv_mr;
    char* recv_buff;
    if (!rdma_mg_->Local_Memory_Register(&recv_buff, &recv_mr, 1000, std::string())) {
      fprintf(stderr, "memory registering failed by size of 0x%x\n", 1000);
    }
    //  post_receive<int>(recv_mr, client_ip);

    rdma_mg_->post_receive<computing_to_memory_msg>(recv_mr, client_ip);

    // sync after send & recv buffer creation and receive request posting.
    if (rdma_mg_->sock_sync_data(socket_fd, 1, temp_send,
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
    computing_to_memory_msg receive_msg_buf;

    //  receive_msg_buf = (computing_to_memory_msg*)recv_buff;
    //  receive_msg_buf->command = ntohl(receive_msg_buf->command);
    //  receive_msg_buf->content.qp_config.qp_num = ntohl(receive_msg_buf->content.qp_config.qp_num);
    //  receive_msg_buf->content.qp_config.lid = ntohs(receive_msg_buf->content.qp_config.lid);
    //  ibv_wc wc[3] = {};
    // TODO: implement a heart beat mechanism.
    while (true) {
      rdma_mg_->poll_completion(wc, 1, client_ip);
      memcpy(&receive_msg_buf, recv_buff, sizeof(computing_to_memory_msg));
      // copy the pointer of receive buf to a new place because
      // it is the same with send buff pointer.
      if (receive_msg_buf.command == create_mr_) {
        std::cout << "create memory region command receive for" << client_ip
        << std::endl;
        ibv_mr* send_pointer = (ibv_mr*)send_buff;
        ibv_mr* mr;
        char* buff;
        if (!rdma_mg_->Local_Memory_Register(&buff, &mr, receive_msg_buf.content.mem_size,
                                   std::string())) {
          fprintf(stderr, "memory registering failed by size of 0x%x\n",
                  static_cast<unsigned>(receive_msg_buf.content.mem_size));
        }
        printf("Now the total Registered memory is %zu GB", rdma_mg_->local_mem_pool.size());
        *send_pointer = *mr;
        rdma_mg_->post_receive<computing_to_memory_msg>(recv_mr, client_ip);
        rdma_mg_->post_send<ibv_mr>(send_mr,client_ip);  // note here should be the mr point to the send buffer.
        rdma_mg_->poll_completion(wc, 1, client_ip);
      } else if (receive_msg_buf.command == create_qp_) {
        char gid_str[17];
        memset(gid_str, 0, 17);
        memcpy(gid_str, receive_msg_buf.content.qp_config.gid, 16);
        std::string new_qp_id =
            std::string(gid_str) +
            std::to_string(receive_msg_buf.content.qp_config.lid) +
            std::to_string(receive_msg_buf.content.qp_config.qp_num);
        std::cout << "create query pair command receive for" << client_ip
        << std::endl;
        fprintf(stdout, "Remote QP number=0x%x\n",
                receive_msg_buf.content.qp_config.qp_num);
        fprintf(stdout, "Remote LID = 0x%x\n",
                receive_msg_buf.content.qp_config.lid);
        registered_qp_config* send_pointer = (registered_qp_config*)send_buff;
        ibv_qp* qp = rdma_mg_->create_qp(new_qp_id);
        if (rdma_mg_->rdma_config.gid_idx >= 0) {
          rc = ibv_query_gid(rdma_mg_->res->ib_ctx, rdma_mg_->rdma_config.ib_port,
                             rdma_mg_->rdma_config.gid_idx, &(rdma_mg_->res->my_gid));
          if (rc) {
            fprintf(stderr, "could not get gid for port %d, index %d\n",
                    rdma_mg_->rdma_config.ib_port, rdma_mg_->rdma_config.gid_idx);
            return;
          }
        } else
          memset(&(rdma_mg_->res->my_gid), 0, sizeof(rdma_mg_->res->my_gid));
        /* exchange using TCP sockets info required to connect QPs */
        send_pointer->qp_num = rdma_mg_->res->qp_map[new_qp_id]->qp_num;
        send_pointer->lid = rdma_mg_->res->port_attr.lid;
        memcpy(send_pointer->gid, &(rdma_mg_->res->my_gid), 16);
        rdma_mg_->connect_qp(receive_msg_buf.content.qp_config, qp);
        rdma_mg_->post_receive<computing_to_memory_msg>(recv_mr, client_ip);
        rdma_mg_->post_send<registered_qp_config>(send_mr, client_ip);
        rdma_mg_->poll_completion(wc, 1, client_ip);
      } else {
        printf("corrupt message from client.");
      }
    }
    return;
    // TODO: Build up a exit method for shared memory side, don't forget to destroy all the RDMA resourses.
  }
  void Memory_Node_Keeper::Server_to_Client_Communication() {
  if (rdma_mg_->resources_create()) {
    fprintf(stderr, "failed to create resources\n");
  }
  int rc;
  if (rdma_mg_->rdma_config.gid_idx >= 0) {
    printf("checkpoint0");
    rc = ibv_query_gid(rdma_mg_->res->ib_ctx, rdma_mg_->rdma_config.ib_port, rdma_mg_->rdma_config.gid_idx,
                       &(rdma_mg_->res->my_gid));
    if (rc) {
      fprintf(stderr, "could not get gid for port %d, index %d\n",
              rdma_mg_->rdma_config.ib_port, rdma_mg_->rdma_config.gid_idx);
      return;
    }
  } else
    memset(&(rdma_mg_->res->my_gid), 0, sizeof rdma_mg_->res->my_gid);
  server_sock_connect(rdma_mg_->rdma_config.server_name,
                      rdma_mg_->rdma_config.tcp_port);
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
  }


