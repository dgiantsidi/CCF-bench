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
std::mutex leader_mtx;
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
static void apply_cmds(std::shared_ptr<RaftDriver> driver)
{
  for (;;)
  {
    auto [src_node, data, data_sz] = driver->message_queue.pop();
    if (data_sz > 0)
      driver->periodic_applying(src_node, data.get(), data_sz);
  }
}

static void listen_for_acks(std::shared_ptr<RaftDriver> driver)
{
  fmt::print("{}:{}\n", __func__, socket_layer::get_thread_id());
  int acks = 0;
  for (;;)
  {
    {
      // std::unique_lock<std::mutex> tmp_l(leader_mtx);
      acks += driver->periodic_listening_acks(std::to_string(follower_1));
    }
    if (acks % 1 == 0)
      fmt::print("{} acks={}\n", __func__, acks);
    if (acks == k_num_requests)
      return;
  }
}

int main(int argc, char* argv[])
{
  threading::ThreadMessaging::init(
    1); // @dimitra:TODO -> this is not used actually
  authentication::init();

  std::string node_id;
  std::cin >> node_id;
  std::vector<std::thread> threads_leader;
  auto driver = make_shared<RaftDriver>(node_id);
  config_parser::initialize_with_data(driver->my_connections);
  auto start = std::chrono::high_resolution_clock::now();
  if (ccf::NodeId(node_id) == ccf::NodeId(std::to_string(primary_node)))
  {
    std::vector<std::thread> threads_leader;

    driver->make_primary(
      ccf::NodeId(node_id),
      driver->my_connections[std::to_string(primary_node)].ip,
      driver->my_connections[std::to_string(primary_node)].base_listening_port);
    driver->become_primary();
    driver->create_new_nodes(
      std::vector<std::string>{std::to_string(follower_1)});
    auto data = std::make_shared<std::vector<uint8_t>>();
    auto& vec = *(data.get());

    int acks = 0;
    acks += driver->periodic_listening_acks(std::to_string(follower_1));
    // this is because we send an AppendEntries message every time we
    // send a new_configuration
    for (auto i = 0ULL; i < k_num_requests; i++)
    {
      // std::unique_lock<std::mutex> tmp_l(leader_mtx);

      // fmt::print("{}:{} ---> replicate_commitable()\n", __func__,
      // socket_layer::get_thread_id());
      driver->replicate_commitable("2", data, 0);
      if (i == 0)
      {
        threads_leader.emplace_back(std::thread(listen_for_acks, driver));
      }
#if 0
      acks += driver->periodic_listening_acks(std::to_string(follower_1));
      if (acks % 50000 == 0)
        fmt::print("{} acks={}\n", __func__, acks);
#endif
    }
    fmt::print("{} --> finished, 1\n", __func__);
    for (;;)
    {
      fmt::print(
        "{} --> get_committed_seqno()={}\n",
        __func__,
        driver->get_committed_seqno());
      if (driver->get_committed_seqno() == k_num_requests)
        break;
    }

    threads_leader[0].join();
    driver->close_connections(std::to_string(primary_node));
    driver->close_connections(std::to_string(follower_1));
  }
  else
  {
    std::vector<std::thread> threads_follower;
    driver->make_follower(
      ccf::NodeId(node_id),
      driver->my_connections[std::to_string(follower_1)].ip,
      driver->my_connections[std::to_string(follower_1)].base_listening_port);
    int count = 0;
    for (auto i = 0ULL; i < k_num_requests; i++)
    {
      count += driver->periodic_listening(std::to_string(primary_node));
      if (i == 0)
      {
        threads_follower.emplace_back(std::thread(apply_cmds, driver));
      }
    }
    count += driver->periodic_listening(std::to_string(primary_node));
    driver->close_connections(std::to_string(follower_1));
    // driver->close_connections(std::to_string(primary_node));
  }
  auto end = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double> duration = end - start;

  fmt::print(
    "{}: time elapsed={}s, tput={} op/s, avg latency={} ms, nb_sends={}, "
    "nb_syscalls_writes={} "
    "nb_recvs={}, nb_syscalls_reads={}, bytes_sent={}, bytes_received={}, "
    "raft_committed_seqno={}, ledger_size={}\n",
    __func__,
    duration.count(),
    ((1.0 * k_num_requests) / (1.0 * duration.count())),
    ((1000.0 * duration.count()) / (1.0 * k_num_requests)),
    socket_layer::nb_sends,
    socket_layer::nb_syscalls_writes,
    socket_layer::nb_recvs,
    socket_layer::nb_syscalls_reads,
    socket_layer::bytes_sent,
    socket_layer::bytes_received,
    driver->get_committed_seqno(),
    driver->get_ledger_size());

  return 0;
}
