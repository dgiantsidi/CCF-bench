
#include "ccf/ds/logger.h"
#include "consensus/aft/raft.h"
#include "networking_api.h"

#include <cassert>
#include <chrono>
#include <fstream>
#include <iostream>
#include <map>
#include <regex>
#include <string>

constexpr int kreqs = 1e6;
constexpr int k_print = 10000;
constexpr int k_msg_sz = 64;

inline static std::tuple<std::unique_ptr<uint8_t[]>, size_t> authenticate_msg(
  std::unique_ptr<uint8_t[]> msg, size_t msg_sz)
{
  if (authentication::is_enabled())
  {
    auto [hash, hash_sz] = authentication::get_hash(msg.get(), msg_sz);
    auto authenticated_msg =
      std::make_unique<uint8_t[]>(msg_sz + authentication::get_hash_len());
    ::memcpy(authenticated_msg.get(), msg.get(), msg_sz);
    ::memcpy(authenticated_msg.get() + msg_sz, hash.get(), hash_sz);
    return {std::move(authenticated_msg), (hash_sz + msg_sz)};
  }
  return {std::move(msg), msg_sz};
}

inline static bool verify_authentication(uint8_t* msg, size_t msg_sz)
{
  if (authentication::is_enabled())
  {
    auto [hash, hash_sz] = authentication::get_hash(msg, msg_sz);
    return (::memcmp(hash.get(), msg + msg_sz, hash_sz) == 0);
  }
  return true;
}

namespace config_parser
{
  void initialize_with_data(
    std::map<ccf::NodeId, network_stack::connectivity_description>&
      my_connections)
  {
    my_connections.insert(std::make_pair(
      ccf::NodeId("0"), network_stack::connectivity_description()));
    my_connections.insert(std::make_pair(
      ccf::NodeId("1"), network_stack::connectivity_description()));

    my_connections[ccf::NodeId(std::to_string(primary_node))].nid =
      ccf::NodeId(std::to_string(primary_node));
    my_connections[ccf::NodeId(std::to_string(primary_node))].ip =
      primary_ip; // CVM
    // my_connections[ccf::NodeId(std::to_string(primary_node))].ip =
    // "10.5.0.6"; // regural VM IP
    my_connections[ccf::NodeId(std::to_string(primary_node))]
      .base_listening_port = primary_listening_port;
    my_connections[ccf::NodeId(std::to_string(primary_node))]
      .base_sending_port = primary_sending_port;

    my_connections[ccf::NodeId(std::to_string(follower_1))].nid =
      ccf::NodeId(std::to_string(follower_1));
    my_connections[ccf::NodeId(std::to_string(follower_1))].ip =
      follower_1_ip; // CVM
    // my_connections[ccf::NodeId(std::to_string(follower_1))].ip = "10.5.0.7";
    // // regural VM IP
    my_connections[ccf::NodeId(std::to_string(follower_1))]
      .base_listening_port = follower_1_listening_port;
    my_connections[ccf::NodeId(std::to_string(follower_1))].base_sending_port =
      follower_1_sending_port;
  }
}

int main(int argc, char* argv[])
{
  authentication::init();
  std::map<ccf::NodeId, network_stack::connectivity_description> my_connections;
  config_parser::initialize_with_data(my_connections);

  std::string node_id;
  std::cin >> node_id;
  std::shared_ptr<network_stack> net = std::make_shared<network_stack>();
  if (node_id == "0")
  {
    fmt::print("{} ---> server\n", __func__);
    net->associate_node_address(
      ccf::NodeId("0"),
      my_connections[ccf::NodeId("0")].ip,
      std::to_string(my_connections[ccf::NodeId("0")].base_listening_port));
    net->connect_to_peer(
      my_connections[ccf::NodeId("0")].ip,
      std::to_string(my_connections[ccf::NodeId("0")].base_listening_port),
      ccf::NodeId("1"),
      my_connections[ccf::NodeId("1")].ip,
      my_connections[ccf::NodeId("1")].base_listening_port);

    net->accept_connection(ccf::NodeId("0"));

    sleep(1);
    auto now = std::chrono::high_resolution_clock::now();
    fmt::print("{} ---> Starting ...\n", __func__);
    for (auto i = 0ULL; i < kreqs; i++)
    {
      auto ptr = std::make_unique<uint8_t[]>(k_msg_sz);
      if (authentication::is_enabled())
      {
        auto [msg_to_send, msg_to_send_sz] =
          authenticate_msg(std::move(ptr), k_msg_sz);
        socket_layer::send_to_socket(
          net->node_connections_map[ccf::NodeId("1")]->sending_handle,
          std::move(msg_to_send),
          msg_to_send_sz);
      }
      else
      {
        socket_layer::send_to_socket(
          net->node_connections_map[ccf::NodeId("1")]->sending_handle,
          std::move(ptr),
          k_msg_sz);
      }

      auto [data, data_sz] = socket_layer::read_from_socket(
        net->node_connections_map[ccf::NodeId("0")]->listening_handle,
        k_msg_sz + authentication::get_hash_len());
      if (!verify_authentication(
            data.get(), data_sz - authentication::get_hash_len()))
      {
        fmt::print("{} ---> tx_id={} failed in authentication\n", __func__, i);
      }
      if (i % k_print == 0)
        fmt::print("{} ---> tx_id={}\n", __func__, i);
    }
    net->close_channel(ccf::NodeId("0"));
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> duration = end - now;
    fmt::print(
      "{} ---> experiment took = {} s, tput={} op/s, avg_latency={} ms\n",
      __func__,
      duration.count(),
      (1.0 * kreqs) / duration.count(),
      (duration.count() * 1.0 * 1000) / (kreqs * 1.0));
  }
  else
  {
    fmt::print("{} ---> client\n", __func__);
    net->associate_node_address(
      ccf::NodeId("1"),
      my_connections[ccf::NodeId("1")].ip,
      std::to_string(my_connections[ccf::NodeId("1")].base_listening_port));
    net->accept_connection(ccf::NodeId("1"));

    net->connect_to_peer(
      my_connections[ccf::NodeId("1")].ip,
      std::to_string(my_connections[ccf::NodeId("1")].base_listening_port),
      ccf::NodeId("0"),
      my_connections[ccf::NodeId("0")].ip,
      my_connections[ccf::NodeId("0")].base_listening_port);

    sleep(1);
    auto now = std::chrono::high_resolution_clock::now();

    for (auto i = 0ULL; i < kreqs; i++)
    {
      auto [data, data_sz] = socket_layer::read_from_socket(
        net->node_connections_map[ccf::NodeId("1")]->listening_handle,
        k_msg_sz + authentication::get_hash_len());
      if (!verify_authentication(
            data.get(), data_sz - authentication::get_hash_len()))
      {
        fmt::print("{} ---> tx_id={} failed in authentication\n", __func__, i);
      }
      auto ptr = std::make_unique<uint8_t[]>(k_msg_sz);
      if (authentication::is_enabled())
      {
        auto [msg_to_send, msg_to_send_sz] =
          authenticate_msg(std::move(ptr), k_msg_sz);

        socket_layer::send_to_socket(
          net->node_connections_map[ccf::NodeId("0")]->sending_handle,
          std::move(msg_to_send),
          msg_to_send_sz);
      }
      else
      {
        socket_layer::send_to_socket(
          net->node_connections_map[ccf::NodeId("0")]->sending_handle,
          std::move(ptr),
          k_msg_sz);
      }
    }
    net->close_channel(ccf::NodeId("1"));
    auto end = std::chrono::high_resolution_clock::now();

    std::chrono::duration<double> duration = end - now;
    fmt::print(
      "{} ---> experiment took = {} s, tput={} op/s, avg_latency={} ms\n",
      __func__,
      duration.count(),
      (1.0 * kreqs) / duration.count(),
      (duration.count() * 1.0 * 1000) / (kreqs * 1.0));
  }
}