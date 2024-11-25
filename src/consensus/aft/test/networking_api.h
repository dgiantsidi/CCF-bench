#pragma once


#include <arpa/inet.h>
#include <cstring>
#include <fmt/printf.h>
#include <iostream>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdint.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <unordered_map>
#include "loggin_stub_mermaid.h"
#include "node/node_to_node.h"

namespace socket_layer
{
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
    // fmt::print("{}: --> msg_size={} @ socket={}\n", __func__, msg_sz,
    // socket);

#if 0
    fmt::print(
      "=*=*=*==*=*=*==*=*=*==*=*=*= {} #1 "
      "=*=*=*==*=*=*==*=*=*==*=*=*=\n",
      __func__);
#endif
    print_data(msg.get(), msg_sz);
    int len = 0, offset = 0;
    int remaining = msg_sz;
    for (;;)
    {
      len = write(socket, msg.get() + offset, remaining);
      offset += len;
      remaining -= offset;
      if (remaining == 0)
        break;
    }
#if 0
    fmt::print(
      "=*=*=*==*=*=*==*=*=*==*=*=*= {} #2 "
      "=*=*=*==*=*=*==*=*=*==*=*=*=\n",
      __func__);
#endif
  }

  std::tuple<std::unique_ptr<uint8_t[]>, size_t> get_from_socket(
    const int& socket, size_t sz)
  {
    // fmt::print("{}: --> socket={} sz={}\n", __func__, socket, sz);
    int len = 0, offset = 0;
    int remaining = sz;
    std::unique_ptr<uint8_t[]> data = std::make_unique<uint8_t[]>(remaining);
    for (;;)
    {
      len = read(socket, data.get() + offset, remaining);
      offset += len;
      remaining -= offset;
      if (remaining == 0)
        break;
    }
    print_data(data.get(), offset);
    return {std::move(data), offset};
  }
};

#if 0
static inline uint16_t __bswap_16(uint16_t x) {
    return (x >> 8) | (x << 8);
}
#endif

class network_stack : public ccf::NodeToNode
{
  using node_id = ccf::NodeId;
  using conn_handle = int;

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
  using MessageList = std::deque<std::pair<ccf::NodeId, std::vector<uint8_t>>>;
  MessageList messages;

  void register_ledger_getter(std::shared_ptr<TRaft> raft)
  {
    raft_copy = raft;
  }
  void print() {}

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
      fmt::print("{}: error creating the socket\n", __func__);
      return -1;
    }
    int flag = 1;
    int result =
      setsockopt(sockfd, IPPROTO_TCP, TCP_NODELAY, (char*)&flag, sizeof(int));
    if (result < 0)
    {
      fmt::print("{}: error setting up the socket\n", __func__);
      return -1;
    }

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
        "{} --> trying to connect to {}:{}\n", __func__, peer_ip, peer_port);
      if (count == 10000)
      {
        fmt::print(
          "{}: error in connecting to {}:{}\n", __func__, peer_ip, peer_port);
        close(sockfd);
        return -1;
      }
    }

    if (node_connections_map.find(peer_id) == node_connections_map.end())
    {
      fmt::print("{}: cannot find connection entry w/ {}\n", __func__, peer_id);
      node_connections_map.insert(
        std::make_pair(peer_id, std::make_unique<connections>()));
    }
    auto& sending_handle = node_connections_map[peer_id]->sending_handle;
    sending_handle = sockfd;
    fmt::print(
      "{}  --> ({}) {}:{} @ socket={}\n",
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
        "{} cannot find listening socket on this connection (w/ {})\n",
        __func__,
        peer_id);
    }
    auto& listening_handle = node_connections_map[peer_id]->listening_handle;

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
      "{}: connection accepted on {} from {}:{} ({})\n",
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
      fmt::print("{}: error creating the socket\n", __func__);
      return;
    }
    int flag = 1;
    int result =
      setsockopt(sockfd, IPPROTO_TCP, TCP_NODELAY, (char*)&flag, sizeof(int));
    if (result < 0)
    {
      fmt::print("{}: error setting up the socket\n", __func__);
      return;
    }

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
        "{}: error binding the socket --> {} {}:{}\n",
        __func__,
        std::strerror(errno),
        peer_hostname,
        peer_service);
      close(sockfd);
      return;
    }
    fmt::print(
      "{} created listening socket={} (bound @ {}:{})\n",
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
    if (node_connections_map.find(peer_id) == node_connections_map.end())
      fmt::print("{} not connections foudn for peer={}\n", __func__, peer_id);
    return;

    if (node_connections_map[peer_id]->listening_handle > 0)
      close(node_connections_map[peer_id]->listening_handle);

    if (node_connections_map[peer_id]->sending_handle > 0)
      close(node_connections_map[peer_id]->sending_handle);
  }

  bool have_channel(const ccf::NodeId& nid) override
  {
    fmt::print("{} --> node {} \n", __func__, nid);

    return (node_connections_map.find(nid) != node_connections_map.end());
#if 0
        if (node_connections_map.find(nid) != node_connections_map.end()) {
            return true;
        }
        return false;
#endif
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
      /*  auto ae = *(aft::AppendEntries*)serialized_data;
        fmt::print("{}:aft::AppendEntries --> .idx={}, .prev_idx={}, .term={},
        .leader_commit_idx={}, .term_of_idx={}\n",
        __func__, ae.idx, ae.prev_idx, ae.term, ae.prev_term,
        ae.leader_commit_idx, ae.term_of_idx);
        */
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
#if 0
    if (sz == sizeof(aft::AppendEntries))
        return aft::RaftMsgType::raft_append_entries;
      if (sz == sizeof(aft::AppendEntriesResponse))
        return aft::RaftMsgType::raft_append_entries_response;
#endif
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

    if (specific_msg_type_sz != size)
    {
      fmt::print(
        "{} --> specific_msg_type_sz={} and size={}\n",
        __func__,
        specific_msg_type_sz,
        size);
        exit(-1);
    }
    size_t msg_type_sz = sizeof(ccf::NodeMsgType::consensus_msg);
    if (sizeof(type) != msg_type_sz) {
      fmt::print(
        "{} --> msg_type_sz={} and sizeof(type)={}\n",
        __func__,
        msg_type_sz,
        sizeof(type));
        exit(-1);
    }

    auto msg_size = size; // + msg_type_sz;
    auto data_sz =  64;

    std::unique_ptr<uint8_t[]> msg_ptr;

    if (msg_type == aft::RaftMsgType::raft_append_entries)
    {
#if 1
      auto ae = *(aft::AppendEntries*)data;
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
        msg_ptr = std::make_unique<uint8_t[]>(msg_size+data_sz);

        ::memset(msg_ptr.get() + size, 'd', data_sz);
#endif
    }
    ::memcpy(msg_ptr.get(), data, size);
    // ::memcpy(msg_ptr.get() , &type, sizeof(type));
    // ::memcpy(msg_ptr.get() + sizeof(type), data, size);

    if (msg_type == aft::RaftMsgType::raft_append_entries)
    {
      send_msg(to, std::move(msg_ptr), msg_size+data_sz);
    }
    else {
      send_msg(to, std::move(msg_ptr), msg_size);
    }
    return true;
  }

  bool recv_authenticated_with_load(
    const ccf::NodeId& from, const uint8_t*& data, size_t& size) override
  {
    fmt::print("{}: from={} \n", __func__, from);

    // TODO: implement me!
    return true;
  }

  bool recv_authenticated(
    const ccf::NodeId& from,
    std::span<const uint8_t> header,
    const uint8_t*& data,
    size_t& size) override
  {
    if (
      read<aft::RaftMsgType>(header.data(), sizeof(aft::AppendEntries)) ==
      aft::RaftMsgType::raft_append_entries)
    {
      auto ae = *(aft::AppendEntries*)(header.data());
#if 1
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
    }
   
    return true;
  }

  bool recv_channel_message(
    const ccf::NodeId& from, const uint8_t* data, size_t size) override
  {
    fmt::print("{}: from={} \n", __func__, from);

    // TODO: implement me!
    return true;
  }

  void initialize(
    const ccf::NodeId& self_id,
    const ccf::crypto::Pem& service_cert,
    ccf::crypto::KeyPairPtr node_kp,
    const std::optional<ccf::crypto::Pem>& node_cert = std::nullopt) override
  {
    fmt::print("{}: self_id={} \n", __func__, self_id);

    // TODO: implement me!
  }

  void set_endorsed_node_cert(
    const ccf::crypto::Pem& endorsed_node_cert) override
  {
    // TODO: implement me!
  }

  bool send_encrypted(
    const ccf::NodeId& to,
    ccf::NodeMsgType type,
    std::span<const uint8_t> header,
    const std::vector<uint8_t>& data) override
  {
    fmt::print("{}: to={} \n", __func__, to);

    // TODO: implement me!
    return true;
  }

  std::vector<uint8_t> recv_encrypted(
    const ccf::NodeId& from,
    std::span<const uint8_t> header,
    const uint8_t* data,
    size_t size) override
  {
    fmt::print("{}: from={} \n", __func__, from);

    // TODO: implement me!
    return std::vector<uint8_t>{};
  }

  void set_message_limit(size_t message_limit) override
  {
    fmt::print("{}: --> limit={} \n", __func__, message_limit);

    // TODO: implement me!
  }

  void set_idle_timeout(std::chrono::milliseconds idle_timeout) override
  {
    fmt::print("{}: --> {} milliseconds \n", __func__, idle_timeout);

    // TODO: implement me!
  }

  void tick(std::chrono::milliseconds elapsed) override
  {
    fmt::print("{}: --> {} milliseconds \n", __func__, elapsed);

    // todo:: implement me
  }

private:
  void send_msg(node_id to, std::unique_ptr<uint8_t[]> msg, size_t msg_sz)
  {
    // fmt::print("{}: --> to {} {} bytes (sizeof(aft::RaftMsgType)={})\n",
    // __func__, to, msg_sz, sizeof(aft::RaftMsgType));
    const uint8_t* data = msg.get();
    aft::RaftMsgType type = serialized::peek<aft::RaftMsgType>(data, msg_sz);

    auto& socket = node_connections_map[to]->sending_handle;
    socket_layer::send_to_socket(socket, std::move(msg), msg_sz);
  }
};
