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

int main(int argc, char* argv[])
{
  threading::ThreadMessaging::init(1);
  authentication::init();
  std::map<ccf::NodeId, network_stack::connectivity_description> my_connections;
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

  std::string node_id;
  std::cin >> node_id;

  auto driver = make_shared<RaftDriver>(node_id);
  auto start = std::chrono::high_resolution_clock::now();
  if (ccf::NodeId(node_id) == ccf::NodeId("0"))
  {
    driver->make_primary(
      ccf::NodeId(node_id),
      my_connections[ccf::NodeId("0")].ip,
      my_connections[ccf::NodeId("0")].base_listening_port);
    driver->become_primary();
    driver->create_new_nodes(std::vector<std::string>{"1"});
    auto data = std::make_shared<std::vector<uint8_t>>();
    auto& vec = *(data.get());

    int acks = 0;
    for (auto i = 0ULL; i < k_num_requests; i++)
    {
      driver->replicate_commitable("2", data, 0);
      acks += driver->periodic_listening_acks(ccf::NodeId("1"));
      if (acks % 50000 == 0)
        fmt::print("{} acks={}\n", __func__, acks);
    }

    driver->close_connections(ccf::NodeId("0"));
  }
  else
  {
    driver->make_follower(
      ccf::NodeId(node_id),
      my_connections[ccf::NodeId("1")].ip,
      my_connections[ccf::NodeId("1")].base_listening_port);
    int count = 0;
    for (auto i = 0ULL; i < k_num_requests; i++)
    {
      count += driver->periodic_listening(ccf::NodeId("0"));
    }
    count += driver->periodic_listening(ccf::NodeId("0"));
    fmt::print("{} count={}\n", __func__, count);
    driver->close_connections(ccf::NodeId("1"));
  }
  auto end = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double> duration = end - start;

  fmt::print(
    "{}: time elapsed={}s, tput={} op/s, avg latency={} ms, nb_sends={}, "
    "nb_recvs={}\n",
    __func__,
    duration.count(),
    (1.0 * k_num_requests) / (1.0 * duration.count()),
    ((1000.0 * duration.count()) / (1.0 * k_num_requests)),
    socket_layer::nb_sends,
    socket_layer::nb_recvs);

  return 0;
}
