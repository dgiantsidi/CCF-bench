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

namespace threading
{
  std::map<std::thread::id, uint16_t> thread_ids;
}

constexpr auto shash = ccf::ds::fnv_1a<size_t>;

int main(int argc, char* argv[])
{
  threading::ThreadMessaging::init(1);

  std::map<ccf::NodeId, network_stack::connectivity_description> my_connections;
  my_connections.insert(std::make_pair(
    ccf::NodeId("0"), network_stack::connectivity_description()));
  my_connections.insert(std::make_pair(
    ccf::NodeId("1"), network_stack::connectivity_description()));
  my_connections.insert(std::make_pair(
    ccf::NodeId("2"), network_stack::connectivity_description()));

  my_connections[ccf::NodeId("0")].nid = ccf::NodeId("0");
  my_connections[ccf::NodeId("0")].ip = "10.5.0.6";
  my_connections[ccf::NodeId("0")].base_listening_port = 1800;
  my_connections[ccf::NodeId("0")].base_sending_port = 1900;

  my_connections[ccf::NodeId("1")].nid = ccf::NodeId("1");
  my_connections[ccf::NodeId("1")].ip = "10.5.0.7";
  my_connections[ccf::NodeId("1")].base_listening_port = 2800;
  my_connections[ccf::NodeId("1")].base_sending_port = 2900;

  my_connections[ccf::NodeId("2")].nid = ccf::NodeId("2");
  my_connections[ccf::NodeId("2")].ip = "10.5.0.6";
  my_connections[ccf::NodeId("2")].base_listening_port = 3800;
  my_connections[ccf::NodeId("2")].base_sending_port = 3900;

  std::string node_id;
  std::cin >> node_id;

  uint64_t lineno = 0;
#if 0
  fmt::print(
    "=*=*=*==*=*=*==*=*=*==*=*=*= make_shared<RaftDriver>() "
    "=*=*=*==*=*=*==*=*=*==*=*=*=\n");
#endif

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
    // driver->create_start_node(ccf::NodeId("0"), 0);
    auto data = std::make_shared<std::vector<uint8_t>>();
    auto& vec = *(data.get());
#if 0
    fmt::print("{}: data to be sent -> ", __func__);
    for (auto i = 0ULL; i < 64; i++) {
      vec.push_back('a');
      fmt::print("{}", vec[i]);
    }
#endif
    // fmt::print("\n");

    int acks = 0;
    for (auto i = 0ULL; i < k_num_requests; i++)
    {
      driver->replicate_commitable("2", data, 0);
      acks += driver->periodic_listening_acks(ccf::NodeId("1"));
      fmt::print("{} acks={}\n", __func__, acks);
    }

    //    driver->periodic_listening(ccf::NodeId("1"));

    driver->close_connections(ccf::NodeId("0"));
    //    driver->close_connections(ccf::NodeId("0"));
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
      for (;;)
      {
        count += driver->periodic_listening(ccf::NodeId("0"));
        // fmt::print("{} recv_msg_count={}\n", __func__, count);
      }
    }
    driver->periodic_listening(ccf::NodeId("0"));
    count++;
    fmt::print("{} count={}\n", __func__, count);
    //    driver->close_connections(ccf::NodeId("0"));
    driver->close_connections(ccf::NodeId("1"));
  }
  auto end = std::chrono::high_resolution_clock::now();
  // Calculate the duration
  std::chrono::duration<double> duration = end - start;

  fmt::print(
    "{}: time elapsed={}s, tput={} op/s, avg latency={} ms\n",
    __func__,
    duration.count(),
    (1.0 * k_num_requests) / (1.0 * duration.count()),
    ((1000.0 * duration.count()) / (1.0 * k_num_requests)));

#if 0
  threading::ThreadMessaging::init(1);
  fmt::print(
    "=*=*=*==*=*=*==*=*=*==*=*=*= driver->create_start_node(0, 0); "
    "=*=*=*==*=*=*==*=*=*==*=*=*=\n");

  driver->create_start_node("0", lineno++);
  fmt::print(
    "=*=*=*==*=*=*==*=*=*==*=*=*= driver->trust_nodes(2, (1, 2), 1); "
    "=*=*=*==*=*=*==*=*=*==*=*=*=\n");

  driver->trust_nodes("2", {"1", "2"}, lineno++);
  fmt::print(
    "=*=*=*==*=*=*==*=*=*==*=*=*= start workload "
    "=*=*=*==*=*=*==*=*=*==*=*=*=\n");
  auto nb_cmds = 0;
  std::unique_ptr<uint8_t[]> data = std::make_unique<uint8_t[]>(64);
  for (;;)
  {
    if (nb_cmds == 1)
      break;
    auto data_v = std::make_shared<std::vector<uint8_t>>();
    // std::copy(data.get(), data.get() + 64, data_v->begin());
    auto term = driver->get_term(ccf::NodeId("0"));
    fmt::print(
      "=*=*=*==*=*=*==*=*=*==*=*=*= replicate_commitable lineno={}"
      "=*=*=*==*=*=*==*=*=*==*=*=*=\n",
      lineno);
    driver->replicate_commitable(std::to_string(term), data_v, lineno++);
    fmt::print(
      "=*=*=*==*=*=*==*=*=*==*=*=*= loop_until_sync lineno={}"
      "=*=*=*==*=*=*==*=*=*==*=*=*=\n",
      (lineno - 1));
    fmt::print(
      "=*=*=*==*=*=*==*=*=*==*=*=*= commit_idxs before sync lineno={}"
      "=*=*=*==*=*=*==*=*=*==*=*=*=\n",
      (lineno - 1));
    driver->print_commit_idx(ccf::NodeId("0"));
    driver->print_commit_idx(ccf::NodeId("1"));
    driver->print_commit_idx(ccf::NodeId("2"));

    driver->loop_until_sync_quorum(lineno++);
    fmt::print(
      "=*=*=*==*=*=*==*=*=*==*=*=*= commit_idxs after sync lineno={} "
      "=*=*=*==*=*=*==*=*=*==*=*=*=\n",
      (lineno - 1));
    driver->print_commit_idx(ccf::NodeId("0"));
    driver->print_commit_idx(ccf::NodeId("1"));
    driver->print_commit_idx(ccf::NodeId("2"));

    nb_cmds++;
  }
#endif

  return 0;
}

#if 0
int main(int argc, char** argv)
{
  const regex delim{","};
  size_t lineno = 1;
  auto driver = make_shared<RaftDriver>();

  if (argc < 2)
  {
    throw std::runtime_error(
      "Too few arguments - first must be path to scenario");
  }

  // Log all raft steps to stdout (python wrapper raft_scenario_runner.py
  // filters them).
#  ifdef CCF_RAFT_TRACING
  ccf::logger::config::add_json_console_logger();
#  else
  ccf::logger::config::add_text_console_logger();
#  endif
  ccf::logger::config::level() = LoggerLevel::DEBUG;

  threading::ThreadMessaging::init(1);

  const std::string filename = argv[1];

  std::ifstream fstream;
  fstream.open(filename);

  if (!fstream.is_open())
  {
    throw std::runtime_error(
      fmt::format("File {} does not exist or could not be opened", filename));
  }

  string line;
  while (getline(fstream, line))
  {
    // Strip off any comments (preceded with #)
    const auto comment_start = line.find_first_of("#");
    if (comment_start != std::string::npos)
    {
      line.erase(comment_start);
    }
    // Strip off any trailing whitespace
    line.erase(line.find_last_not_of(" \t\n\r\f\v") + 1);
    vector<string> items{
      sregex_token_iterator(line.begin(), line.end(), delim, -1),
      std::sregex_token_iterator()};
    std::shared_ptr<std::vector<uint8_t>> data;
    const std::string& in = items[0].c_str();
    if (in.starts_with("===="))
    {
      // Terminate early if four or more '=' appear on a line.
      break;
    }
#  ifdef CCF_RAFT_TRACING
    if (!line.empty())
    {
      std::cout << "{\"tag\": \"raft_trace\", \"cmd\": \"" << line << "\"}"
                << std::endl;
    }
#  endif
    // Steps which don't alter state don't need to recheck invariants
    bool skip_invariants = false;

    switch (shash(in))
    {
      case shash("start_node"):
        assert(items.size() == 2);
        driver->create_start_node(items[1], lineno);
        break;
      case shash("trust_node"):
        assert(items.size() == 3);
        driver->trust_nodes(items[1], {items[2]}, lineno);
        break;
      case shash("trust_nodes"):
        assert(items.size() >= 3);
        items.erase(items.begin());
        driver->trust_nodes(
          items[0], {std::next(items.begin()), items.end()}, lineno);
        break;
      case shash("cleanup_nodes"):
        assert(items.size() >= 3);
        items.erase(items.begin());
        driver->cleanup_nodes(
          items[0], {std::next(items.begin()), items.end()}, lineno);
        break;
      case shash("swap_node"):
        assert(items.size() == 4);
        driver->swap_nodes(items[1], {items[2]}, {items[3]}, lineno);
        break;
      case shash("swap_nodes"):
      {
        // Usage is: swap_nodes,<term>,in,<node1>,...,out,<node3>,...
        // swap_nodes,<term>,in,<node1>,...
        // swap_nodes,<term>,out,<node1>,...
        // are also permitted, and so is
        // swap_nodes,<term>,out,<node1>,...,in,<node3>,...
        assert(items.size() >= 4);
        auto vargs_begin = std::next(std::next(items.begin()));
        auto in_pos = std::find(vargs_begin, items.end(), "in");
        auto out_pos = std::find(vargs_begin, items.end(), "out");
        if (in_pos == vargs_begin)
        {
          driver->swap_nodes(
            items[1],
            {out_pos != items.end() ? std::next(out_pos) : items.end(),
             items.end()}, // out nodes if any
            {std::next(vargs_begin), out_pos}, // in nodes
            lineno);
        }
        else if (out_pos == vargs_begin)
        {
          driver->swap_nodes(
            items[1],
            {std::next(vargs_begin), in_pos}, // out nodes
            {in_pos != items.end() ? std::next(in_pos) : items.end(),
             items.end()}, // in nodes if any
            lineno);
        }
        else
        {
          throw std::runtime_error(fmt::format(
            "swap_nodes: expected 'in' or 'out' after term on line {}",
            lineno));
        }

        break;
      }
      case shash("nodes"):
        assert(items.size() >= 2);
        items.erase(items.begin());
        driver->create_new_nodes(items);
        break;
      case shash("connect"):
        assert(items.size() == 3);
        driver->connect(items[1], items[2]);
        break;
      case shash("periodic_one"):
        assert(items.size() == 3);
        driver->periodic_one(items[1], ms(stoi(items[2])));
        break;
      case shash("periodic_all"):
        assert(items.size() == 2);
        driver->periodic_all(ms(stoi(items[1])));
        break;
      case shash("state_one"):
        assert(items.size() == 2);
        skip_invariants = true;
        driver->state_one(items[1]);
        break;
      case shash("state_all"):
        assert(items.size() == 1);
        skip_invariants = true;
        driver->state_all();
        break;
      case shash("summarise_log"):
        assert(items.size() == 2);
        skip_invariants = true;
        driver->summarise_log(items[1]);
        break;
      case shash("summarise_logs_all"):
        assert(items.size() == 1);
        skip_invariants = true;
        driver->summarise_logs_all();
        break;
      case shash("summarise_messages"):
        assert(items.size() == 3);
        driver->summarise_messages(items[1], items[2]);
        break;
      case shash("shuffle_one"):
        assert(items.size() == 2);
        driver->shuffle_messages_one(items[1]);
        break;
      case shash("shuffle_all"):
        assert(items.size() == 1);
        driver->shuffle_messages_all();
        break;
      case shash("dispatch_one"):
        assert(items.size() == 2);
        driver->dispatch_one(items[1]);
        break;
      case shash("dispatch_all"):
        assert(items.size() == 1);
        driver->dispatch_all();
        break;
      case shash("dispatch_all_once"):
        assert(items.size() == 1);
        driver->dispatch_all_once();
        break;
      case shash("dispatch_single"):
        assert(items.size() == 3);
        driver->dispatch_single(items[1], items[2]);
        break;
      case shash("replicate"):
        static int occurence_replication = 0;
        fmt::print(
          "[{}] replicate start {}\n\n\n\n\n\n\n\n",
          __func__,
          occurence_replication);
        assert(items.size() == 3);
        data = std::make_shared<std::vector<uint8_t>>(
          items[2].begin(), items[2].end());
        driver->replicate(items[1], data, lineno);
        fmt::print(
          "[{}] replicate end {}\n\n\n\n\n\n\n\n\n",
          __func__,
          occurence_replication);
        occurence_replication++;
        break;
      case shash("emit_signature"):
        static int occurence = 0;
        fmt::print(
          "[{}] emit_signature start {}\n\n\n\n\n\n\n\n", __func__, occurence);
        assert(items.size() == 2);
        driver->emit_signature(items[1], lineno);
        fmt::print(
          "[{}] emit_signature end {}\n\n\n\n\n\n\n\n\n", __func__, occurence);
        occurence++;
        break;
      case shash("disconnect"):
        assert(items.size() == 3);
        driver->disconnect(items[1], items[2]);
        break;
      case shash("disconnect_node"):
        assert(items.size() == 2);
        driver->disconnect_node(items[1]);
        break;
      case shash("reconnect"):
        assert(items.size() == 3);
        driver->reconnect(items[1], items[2]);
        break;
      case shash("reconnect_node"):
        assert(items.size() == 2);
        driver->reconnect_node(items[1]);
        break;
      case shash("drop_pending"):
        assert(items.size() == 2);
        driver->drop_pending(items[1]);
        break;
      case shash("drop_pending_to"):
        assert(items.size() == 3);
        driver->drop_pending_to(items[1], items[2]);
        break;
      case shash("assert_state_sync"):
        assert(items.size() == 1);
        skip_invariants = true;
        driver->assert_state_sync(lineno);
        break;
      case shash("assert_commit_safety"):
        assert(items.size() == 2);
        driver->assert_commit_safety(items[1], lineno);
        break;
      case shash("assert_commit_idx"):
        assert(items.size() == 3);
        skip_invariants = true;
        driver->assert_commit_idx(items[1], items[2], lineno);
        break;
      case shash("assert_detail"):
        assert(items.size() == 4);
        driver->assert_detail(items[1], items[2], items[3], true, lineno);
        break;
      case shash("assert_!detail"):
        assert(items.size() == 4);
        driver->assert_detail(items[1], items[2], items[3], false, lineno);
        break;
      case shash("replicate_new_configuration"):
        assert(items.size() >= 3);
        items.erase(items.begin());
        driver->replicate_new_configuration(
          items[0], {std::next(items.begin()), items.end()}, lineno);
        break;
      case shash("create_new_node"):
        assert(items.size() == 2);
        driver->create_new_node(items[1]);
        break;
      case shash("loop_until_sync"):
        assert(items.size() == 1);
        driver->loop_until_sync(lineno);
        break;
      case shash(""):
        // Ignore empty lines
        skip_invariants = true;
        break;
      default:
        throw std::runtime_error(
          fmt::format("Unknown action '{}' at line {}", items[0], lineno));
    }

    if (!skip_invariants)
    {
      driver->assert_invariants(lineno);
    }

    ++lineno;
  }

  // Confirm path to liveness from final state
  driver->loop_until_sync(lineno);

  return 0;
}
#endif