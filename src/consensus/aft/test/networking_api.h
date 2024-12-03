#pragma once

#include "authentication_layer.h"
#include "config.hpp"
#include "loggin_stub_mermaid.h"
#include "node/node_to_node.h"

#include <arpa/inet.h>
#include <cstring>
#include <fmt/printf.h>
#include <iostream>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdint.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <unordered_map>

namespace socket_layer
{
  // these are stats we keep track of
  uint64_t nb_sends = 0;
  uint64_t nb_recvs = 0;
  uint64_t bytes_sent = 0;
  uint64_t bytes_received = 0;
  uint64_t nb_syscalls_writes = 0;
  uint64_t nb_syscalls_reads = 0;

  static void print_data(const uint8_t* ptr, size_t msg_size)
  {
#if 0
   fmt::print(
      "=*=*=*==*=*=*==*=*=*==*=*=*= {} #1 "
      "=*=*=*==*=*=*==*=*=*==*=*=*=\n",
      __func__);
    for (auto i = 0ULL; i < msg_size; i++) {
      fmt::print("{}", static_cast<char>(ptr[i]));
    }
    fmt::print(
      "\n=*=*=*==*=*=*==*=*=*==*=*=*= {} #2 "
      "=*=*=*==*=*=*==*=*=*==*=*=*=\n",
      __func__);
#endif
  }

  void send_to_socket(
    const int& socket, std::unique_ptr<uint8_t[]> msg, size_t msg_sz)
  {
    
    nb_sends++;
    bytes_sent += msg_sz;
    int len = 0, offset = 0;
    int remaining = msg_sz;
    for (;;)
    {
      fmt::print("{} nb_sents={} ** START **\n", __func__, nb_sends);
      len = write(socket, msg.get() + offset, remaining);
      nb_syscalls_writes++;
      offset += len;
      remaining -= len;
      
      if (remaining == 0) {
        fmt::print("{} nb_sents={} ** END **\n", __func__, nb_sends);
        break;
      }
    }
  }

  std::tuple<std::unique_ptr<uint8_t[]>, size_t> read_from_socket(
    const int& socket, size_t sz)
  {
    int len = 0, offset = 0;
    int remaining = sz;
    bytes_received += sz;
    static int count = 0;
    std::unique_ptr<uint8_t[]> data_buf =
      std::make_unique<uint8_t[]>(remaining);
    for (;;)
    {
      count++;
      fmt::print("{} count={} ** START **\n", __func__, count);
      len = read(socket, data_buf.get() + offset, remaining);
      nb_syscalls_reads++;
      offset += len;
      remaining -= len;
      fmt::print("{} count={} len={} remaining={} ** START **\n", __func__, count, len, remaining);

      if (remaining == 0) {
        fmt::print("{} count={} ** END **\n", __func__, count);
        return {std::move(data_buf), sz};

      }
    }
    return {std::move(std::make_unique<uint8_t[]>(0)), 0};
  }

  std::tuple<std::unique_ptr<uint8_t[]>, size_t> get_from_socket(
    const int& socket, size_t sz)
  {
    nb_recvs++;
    auto [header_buf, header_sz] = read_from_socket(socket, sz);
    if (header_sz != sz)
    {
      fmt::print(
        "{} ---> error header_sz={} and sz={}\n", __func__, header_sz, sz);
      exit(-1);
    }

    int total_bytes = header_sz;
    auto [payload_sz_buf, pload_sz] = read_from_socket(socket, payload_sz);
    if (pload_sz != payload_sz)
    {
      fmt::print(
        "{} ---> error payload_sz={} and pload_sz={}\n",
        __func__,
        payload_sz,
        pload_sz);
      exit(-1);
    }
    size_t sz_val;
    ::memcpy(&sz_val, payload_sz_buf.get(), payload_sz);

    auto [entry_buf, entry_sz] = read_from_socket(socket, sz_val);
    if (entry_sz != sz_val)
    {
      fmt::print(
        "{} ---> error entry_sz={} and sz_val={}\n",
        __func__,
        entry_sz,
        sz_val);
      exit(-1);
    }
    total_bytes += sz_val; // we exclude the sizeof(payload_sz) on purpose
    std::unique_ptr<uint8_t[]> serialized_msg =
      std::make_unique<uint8_t[]>(total_bytes);
    ::memcpy(serialized_msg.get(), header_buf.get(), header_sz);
    if (entry_sz > 0)
    {
      ::memcpy(serialized_msg.get() + header_sz, entry_buf.get(), entry_sz);
    }

    if (authentication::is_enabled())
    {
      auto [hash, hash_len] =
        read_from_socket(socket, authentication::get_hash_len());
      std::unique_ptr<uint8_t[]> original_msg =
        std::make_unique<uint8_t[]>(total_bytes + payload_sz);
      ::memcpy(original_msg.get(), header_buf.get(), header_sz);
      ::memcpy(
        original_msg.get() + header_sz, payload_sz_buf.get(), payload_sz);
      ::memcpy(
        original_msg.get() + header_sz + payload_sz, entry_buf.get(), entry_sz);

      if (!authentication::verify_hash(
            original_msg.get(), (total_bytes + payload_sz), hash.get()))
        fmt::print("{} ---> error in verifying the hash\n", __func__);
    }
    print_data(serialized_msg.get(), total_bytes);
    return {std::move(serialized_msg), total_bytes};
  }
};

class network_stack : public ccf::NodeToNode
{
  using node_id = ccf::NodeId;
  using conn_handle = int; // this is a socket

  struct connections
  {
    connections() : listening_handle(0), sending_handle(0){};

    connections(const connections& other)
    {
      listening_handle = other.listening_handle;
      sending_handle = other.sending_handle;
    }

    connections(connections&& other)
    {
      listening_handle = other.listening_handle;
      sending_handle = other.sending_handle;
    }

    conn_handle listening_handle;
    conn_handle sending_handle;
  };

public:
  network_stack() {}

  // this is a weak pointer over raft object such that we have access to the
  // in-memory ledger for constructing the message-to-be-sent
  std::weak_ptr<TRaft> raft_copy;

  struct connectivity_description
  {
    connectivity_description() = default;
    node_id nid;
    std::string ip;
    int base_listening_port;
    int base_sending_port;
  };

public:
  std::map<node_id, std::unique_ptr<connections>> node_connections_map;

  void register_ledger_getter(std::shared_ptr<TRaft> raft)
  {
    raft_copy = raft;
  }

  bool connect_to_peer(
    const std::string& host_,
    const std::string& port_,
    const ccf::NodeId& peer_id,
    const std::string& peer_ip,
    const int& peer_port)
  {
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd == -1)
    {
      fmt::print("{} error creating the socket\n", __func__);
      return -1;
    }
    int flag = 1;
    int result =
      setsockopt(sockfd, IPPROTO_TCP, TCP_NODELAY, (char*)&flag, sizeof(int));
    if (result < 0)
    {
      fmt::print("{} error setting up the socket\n", __func__);
      return -1;
    }
    int flags = fcntl(sockfd, F_GETFL, 0);
    if (flags < 0) {
        perror("fcntl(F_GETFL)");
        exit(EXIT_FAILURE);
    }

  #if 0
    flags |= O_NONBLOCK;
    if (fcntl(sockfd, F_SETFL, flags) < 0) {
        perror("fcntl(F_SETFL)");
        exit(EXIT_FAILURE);
    }
  #endif


    // Define the server address
    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(peer_port); // Specify the port number
    server_addr.sin_addr.s_addr =
      inet_addr(peer_ip.c_str()); // Replace with the specific IP address

    // Connect to the server
    int count = 0;
    while (connect(
             sockfd, (struct sockaddr*)&server_addr, sizeof(server_addr)) == -1)
    {
      count++;
      sleep(1);
      fmt::print(
        "{} ---> trying to connect to {}:{} ---> {}\n",
        __func__,
        peer_ip,
        peer_port,
        std::strerror(errno));
      if (count == 10000)
      {
        fmt::print(
          "{} ---> error in connecting to {}:{}\n",
          __func__,
          peer_ip,
          peer_port);
        close(sockfd);
        return -1;
      }
    }

    if (node_connections_map.find(peer_id) == node_connections_map.end())
    {
      fmt::print(
        "{}: cannot find connection entry with {} so we add one\n",
        __func__,
        peer_id);
      node_connections_map.insert(
        std::make_pair(peer_id, std::make_unique<connections>()));
    }
    auto& sending_handle = node_connections_map[peer_id]->sending_handle;
    sending_handle = sockfd;
    fmt::print(
      "{} ---> ({}) {}:{} at sending socket={}\n",
      __func__,
      peer_id,
      peer_ip,
      peer_port,
      sending_handle);

    return 1;
  }

  void accept_connection(const ccf::NodeId& peer_id)
  {
    if (node_connections_map.find(peer_id) == node_connections_map.end())
    {
      fmt::print(
        "{} cannot find listening socket on this connection (with {})\n",
        __func__,
        peer_id);
      exit(-1);
    }
    fmt::print(
      "{} ---> with peer={} and we will update the listening_handle\n",
      __func__,
      peer_id);
    auto& listening_handle =
      node_connections_map[peer_id]
        ->listening_handle; // TODO: the peer_id is my_id -> should be the
                            // other's node id

    if (listen(listening_handle, 5) == -1)
    {
      fmt::print(
        "{} error in listening on socket {}\n", __func__, listening_handle);
      close(listening_handle);
    }

    struct sockaddr_in client_addr;
    socklen_t client_len = sizeof(client_addr);
    int client_sock =
      accept(listening_handle, (struct sockaddr*)&client_addr, &client_len);
    if (client_sock == -1)
    {
      fmt::print("{} error in accepting a connection\n", __func__);
      close(listening_handle);
    }

    fmt::print(
      "{} ---> connection accepted on {} from {}:{} ({})\n",
      __func__,
      client_sock,
      inet_ntoa(client_addr.sin_addr),
      ntohs(client_addr.sin_port),
      peer_id);
    listening_handle = client_sock;
  }

  void associate_node_address(
    const ccf::NodeId& peer_id,
    const std::string& peer_hostname,
    const std::string& peer_service) override
  {
    const int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd == -1)
    {
      fmt::print("{} ---> error creating the socket\n", __func__);
      return;
    }
    int flag = 1;
    int result =
      setsockopt(sockfd, IPPROTO_TCP, TCP_NODELAY, (char*)&flag, sizeof(int));
    if (result < 0)
    {
      fmt::print("{} ---> error setting up the socket\n", __func__);
      return;
    }

     int flags = fcntl(sockfd, F_GETFL, 0);
    if (flags < 0) {
        perror("fcntl(F_GETFL)");
        exit(EXIT_FAILURE);
    }
#if 0
    flags |= O_NONBLOCK;
    if (fcntl(sockfd, F_SETFL, flags) < 0) {
        perror("fcntl(F_SETFL)");
        exit(EXIT_FAILURE);
    }
  #endif


    const int port = std::stoi(peer_service);
    // define the server address
    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = inet_addr(peer_hostname.c_str());
    server_addr.sin_port = htons(port); // Specify the port number

    // bind the socket to the address
    if (bind(sockfd, (struct sockaddr*)&server_addr, sizeof(server_addr)) == -1)
    {
      fmt::print(
        "{} error(={}) ---> binding the socket in {}:{}\n",
        __func__,
        std::strerror(errno),
        peer_hostname,
        peer_service);
      close(sockfd);
      return;
    }
    fmt::print(
      "{} listening socket={} (bound at {}:{})\n",
      __func__,
      sockfd,
      peer_hostname.c_str(),
      port);
    node_connections_map.insert(std::make_pair(
      ccf::NodeId(peer_id.value()), std::make_unique<connections>()));
    node_connections_map[peer_id]->listening_handle = sockfd;
  }

  void close_channel(const ccf::NodeId& peer_id) override
  {
    if (node_connections_map.find(peer_id) == node_connections_map.end()) {
      fmt::print("{} not connections found with peer {}\n", __func__, peer_id);
      return;
    }

    if (node_connections_map[peer_id]->listening_handle > 0)
      close(node_connections_map[peer_id]->listening_handle);

    if (node_connections_map[peer_id]->sending_handle > 0)
      close(node_connections_map[peer_id]->sending_handle);
  }

  bool have_channel(const ccf::NodeId& nid) override
  {
    if (auto s_ptr = raft_copy.lock())
    {
      fmt::print(
        "{} --> current node {} with node {} \n", __func__, s_ptr->id(), nid);
    }
    else
    {
      fmt::print(
        "{} --> with node {} (raft_copy not initialized ...)\n",
        __func__,
        s_ptr->id(),
        nid);
    }

    return (node_connections_map.find(nid) != node_connections_map.end());
  }

  template <class T>
  T read(const uint8_t* data, size_t size)
  {
    if (size < sizeof(T))
    {
      fmt::print(
        "{} --> error: Insufficient space (read<T>: {} < {})",
        __func__,
        size,
        sizeof(T));
    }

    T v;
    std::memcpy(reinterpret_cast<uint8_t*>(&v), data, sizeof(T));
    return v;
  }

  std::tuple<uint64_t, size_t> get_msg_type_from_header_sz(
    const uint8_t* serialized_data, size_t sz)
  {
    if (
      read<aft::RaftMsgType>(serialized_data, sz) ==
      aft::RaftMsgType::raft_append_entries)
    {
#if 0
        auto ae = *(aft::AppendEntries*)serialized_data;
        fmt::print("{}:aft::AppendEntries --> .idx={}, .prev_idx={}, .term={},
        .leader_commit_idx={}, .term_of_idx={}\n",
        __func__, ae.idx, ae.prev_idx, ae.term, ae.prev_term,
        ae.leader_commit_idx, ae.term_of_idx);
#endif
      return {
        aft::RaftMsgType::raft_append_entries, sizeof(aft::AppendEntries)};
    }
    else if (
      read<aft::RaftMsgType>(serialized_data, sz) ==
      aft::RaftMsgType::raft_append_entries_response)
    {
      return {
        aft::RaftMsgType::raft_append_entries_response,
        sizeof(aft::AppendEntriesResponse)};
    }
    return {-1, -1};
  }

  bool send_authenticated(
    const ccf::NodeId& to,
    ccf::NodeMsgType type,
    const uint8_t* data,
    size_t size) override
  {
    auto [msg_type, specific_msg_type_sz] =
      get_msg_type_from_header_sz(data, size);

    auto header_msg_size = size;

    std::unique_ptr<uint8_t[]> msg_ptr;

    if (msg_type == aft::RaftMsgType::raft_append_entries)
    {
      auto ae = *(aft::AppendEntries*)data;

#if 0
      fmt::print(
        "{}:aft::AppendEntries --> .idx={}, .prev_idx={}, .term={}, "
        ".leader_commit_idx={}, .term_of_idx={}\n",
        __func__,
        ae.idx,
        ae.prev_idx,
        ae.term,
        ae.prev_term,
        ae.leader_commit_idx,
        ae.term_of_idx);
#endif
      if (auto s_ptr = raft_copy.lock())
      {
        auto entry = s_ptr->ledger->get_entry_by_idx(ae.idx);
        if (entry.has_value())
        {
          msg_ptr = std::make_unique<uint8_t[]>(
            header_msg_size + payload_sz + entry.value().size());
          size_t size_of_payload = entry.value().size();
          ::memcpy(msg_ptr.get(), data, header_msg_size);
          ::memcpy(msg_ptr.get() + size, &(size_of_payload), payload_sz);
          ::memcpy(
            msg_ptr.get() + header_msg_size + payload_sz,
            entry.value().data(),
            entry.value().size());
          send_msg(
            to,
            std::move(msg_ptr),
            header_msg_size + entry.value().size() + payload_sz);
          return true;
        }
        else
        {
          msg_ptr = std::make_unique<uint8_t[]>(header_msg_size + payload_sz);
          ::memcpy(msg_ptr.get(), data, size);
          size_t empty_payload = 0;
          ::memcpy(msg_ptr.get() + header_msg_size, &empty_payload, payload_sz);
          send_msg(to, std::move(msg_ptr), header_msg_size + payload_sz);
          return true;
        }
      }
      else
      {
        fmt::print("{} --> raft_copy is not initialized ..\n", __func__);
        exit(-1);
        return false;
      }
    }
    else
    {
      msg_ptr = std::make_unique<uint8_t[]>(header_msg_size + payload_sz);
    }
    ::memcpy(msg_ptr.get(), data, header_msg_size);
    size_t empty_payload = 0;
    ::memcpy(msg_ptr.get() + header_msg_size, &empty_payload, payload_sz);
    send_msg(to, std::move(msg_ptr), header_msg_size + payload_sz);
    return true;
  }

  bool recv_authenticated_with_load(
    const ccf::NodeId& from, const uint8_t*& data, size_t& size) override
  {
    fmt::print("{} ---> from={} \n", __func__, from);
    // TODO: implement me!
    return true;
  }

  bool recv_authenticated(
    const ccf::NodeId& from,
    std::span<const uint8_t> header,
    const uint8_t*& data,
    size_t& size) override
  {
#if 0
    auto [msg_type, specific_msg_type_sz] =
      get_msg_type_from_header_sz(header.data(), header.size());
    if (msg_type == aft::RaftMsgType::raft_append_entries)
    {
      auto ae = *(aft::AppendEntries*)(header.data());

      fmt::print(
        "{}:aft::AppendEntries --> .idx={}, .prev_idx={}, .term={}, "
        ".prev_term={}, .leader_commit_idx = {}, .term_of_idx = {}, "
        "header.data()={},  sizeof(aft::AppendEntries)={}, data.size()={}\n",
        __func__,
        ae.idx,
        ae.prev_idx,
        ae.term,
        ae.prev_term,
        ae.leader_commit_idx,
        ae.term_of_idx,
        header.size(),
        sizeof(aft::AppendEntries),
        size);
    }
#endif

    return true;
  }

  bool recv_channel_message(
    const ccf::NodeId& from, const uint8_t* data, size_t size) override
  {
    fmt::print("{} ---> from={} \n", __func__, from);
    // TODO: implement me!
    return true;
  }

  void initialize(
    const ccf::NodeId& self_id,
    const ccf::crypto::Pem& service_cert,
    ccf::crypto::KeyPairPtr node_kp,
    const std::optional<ccf::crypto::Pem>& node_cert = std::nullopt) override
  {
    fmt::print("{} ---> self_id={} \n", __func__, self_id);
    // TODO: implement me!
  }

  void set_endorsed_node_cert(
    const ccf::crypto::Pem& endorsed_node_cert) override
  {
    fmt::print("{}\n", __func__);
    // TODO: implement me!
  }

  bool send_encrypted(
    const ccf::NodeId& to,
    ccf::NodeMsgType type,
    std::span<const uint8_t> header,
    const std::vector<uint8_t>& data) override
  {
    fmt::print("{} ---> to={} \n", __func__, to);
    // TODO: implement me!
    return true;
  }

  std::vector<uint8_t> recv_encrypted(
    const ccf::NodeId& from,
    std::span<const uint8_t> header,
    const uint8_t* data,
    size_t size) override
  {
    fmt::print("{} ---> from={} \n", __func__, from);
    // TODO: implement me!
    return std::vector<uint8_t>{};
  }

  void set_message_limit(size_t message_limit) override
  {
    fmt::print("{}, limit={} \n", __func__, message_limit);
    // TODO: implement me!
  }

  void set_idle_timeout(std::chrono::milliseconds idle_timeout) override
  {
    fmt::print("{} for {} milliseconds \n", __func__, idle_timeout);
    // TODO: implement me!
  }

  void tick(std::chrono::milliseconds elapsed) override
  {
    fmt::print("{} for {} milliseconds \n", __func__, elapsed);
    // TODO:: implement me
  }

private:
  void send_msg(node_id to, std::unique_ptr<uint8_t[]> msg, size_t msg_sz)
  {
    auto& socket = node_connections_map[to]->sending_handle;

    if (authentication::is_enabled())
    {
      auto [hash, hash_len] = authentication::get_hash(msg.get(), msg_sz);
      auto hashed_msg = std::make_unique<uint8_t[]>(hash_len + msg_sz);
      ::memcpy(hashed_msg.get(), msg.get(), msg_sz);
      ::memcpy(hashed_msg.get() + msg_sz, hash.get(), hash_len);
      socket_layer::send_to_socket(
        socket, std::move(hashed_msg), msg_sz + hash_len);
    }
    else
      socket_layer::send_to_socket(socket, std::move(msg), msg_sz);
  }
};
