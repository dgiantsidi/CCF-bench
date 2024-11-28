// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache 2.0 License.

#define VERBOSE_RAFT_LOGGING

#include "driver.h"

#include "ccf/ds/hash.h"
#include "config.hpp"
#include "networking_api.h"

#include <cassert>
#include <chrono>
#include <fstream>
#include <iostream>
#include <regex>
#include <string>

using namespace std;

static void print_data(uint8_t* ptr, size_t msg_size)
{
  fmt::print(
    "=*=*=*==*=*=*==*=*=*==*=*=*= {} #1 "
    "=*=*=*==*=*=*==*=*=*==*=*=*=\n",
    __func__);
  for (auto i = 0ULL; i < msg_size; i++)
  {
    fmt::print("{}", static_cast<int>(ptr[i]));
  }
  fmt::print(
    "=*=*=*==*=*=*==*=*=*==*=*=*= {} #2 "
    "=*=*=*==*=*=*==*=*=*==*=*=*=\n",
    __func__);
}

std::unique_ptr<threading::ThreadMessaging>
  threading::ThreadMessaging::singleton = nullptr;

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
  threading::ThreadMessaging::init(1);
  authentication::init();
  // std::map<ccf::NodeId, network_stack::connectivity_description> my_connections;
  
  #if 0
  my_connections.insert(std::make_pair(
    ccf::NodeId("0"), network_stack::connectivity_description()));
  my_connections.insert(std::make_pair(
    ccf::NodeId("1"), network_stack::connectivity_description()));
  my_connections.insert(std::make_pair(
    ccf::NodeId("2"), network_stack::connectivity_description()));

  my_connections[ccf::NodeId("0")].nid = ccf::NodeId("0");
  my_connections[ccf::NodeId("0")].ip = "10.1.0.7";
  // my_connections[ccf::NodeId("0")].ip = "10.5.0.6"; // regural VM IP
  my_connections[ccf::NodeId("0")].base_listening_port = 1800;
  my_connections[ccf::NodeId("0")].base_sending_port = 1900;

  my_connections[ccf::NodeId("1")].nid = ccf::NodeId("1");
  my_connections[ccf::NodeId("1")].ip = "10.1.0.4";
  // my_connections[ccf::NodeId("1")].ip = "10.5.0.7"; // regural VM IP
  my_connections[ccf::NodeId("1")].base_listening_port = 2800;
  my_connections[ccf::NodeId("1")].base_sending_port = 2900;

  my_connections[ccf::NodeId("2")].nid = ccf::NodeId("2");
  my_connections[ccf::NodeId("2")].ip = "10.5.0.6";
  my_connections[ccf::NodeId("2")].base_listening_port = 3800;
  my_connections[ccf::NodeId("2")].base_sending_port = 3900;
#endif
  std::string node_id;
  std::cin >> node_id;

  auto driver = make_shared<RaftDriver>(node_id);
  config_parser::initialize_with_data(driver->my_connections);
  auto start = std::chrono::high_resolution_clock::now();
  if (ccf::NodeId(node_id) == ccf::NodeId(std::to_string(primary_node)))
  {
    driver->make_primary(
      ccf::NodeId(node_id),
      driver->my_connections[std::to_string(primary_node)].ip,
      driver->my_connections[std::to_string(primary_node)].base_listening_port);
    driver->become_primary();
    driver->create_new_nodes(std::vector<std::string>{std::to_string(follower_1)});
    auto data = std::make_shared<std::vector<uint8_t>>();
    auto& vec = *(data.get());

    int acks = 0;
    for (auto i = 0ULL; i < k_num_requests; i++)
    {
      driver->replicate_commitable("2", data, 0);
      acks += driver->periodic_listening_acks(std::to_string(follower_1));
      if (acks % 50000 == 0)
        fmt::print("{} acks={}\n", __func__, acks);
    }

    driver->close_connections(ccf::NodeId("0"));
  }
  else
  {
    driver->make_follower(
      ccf::NodeId(node_id),
      driver->my_connections[std::to_string(follower_1)].ip,
      driver->my_connections[std::to_string(follower_1)].base_listening_port);
    int count = 0;
    for (auto i = 0ULL; i < k_num_requests; i++)
    {
      count += driver->periodic_listening(std::to_string(primary_node));
    }
    count += driver->periodic_listening(std::to_string(primary_node));
    fmt::print("{} count={}\n", __func__, count);
    driver->close_connections(std::to_string(follower_1));
  }
  auto end = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double> duration = end - start;

  fmt::print(
    "{}: time elapsed={}s, tput={} op/s, avg latency={} ms, nb_sends={}, "
    "nb_recvs={}, raft_committed_seqno={}\n",
    __func__,
    duration.count(),
    (1.0 * k_num_requests) / (1.0 * duration.count()),
    ((1000.0 * duration.count()) / (1.0 * k_num_requests)),
    socket_layer::nb_sends,
    socket_layer::nb_recvs,
    driver->get_committed_seqno());

  return 0;
}
