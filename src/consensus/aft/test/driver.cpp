// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache 2.0 License.

#define VERBOSE_RAFT_LOGGING

#include "driver.h"

#include "ccf/ds/hash.h"
#include "config.hpp"
#include "networking_api.h"

#include "ngtcp2-unmodified/examples/ccf_related_work/config.h"
//#warning "Linking with server_req_pipelining.h"
//#include "ngtcp2/examples/server_req_pipelining.h"
#include "ngtcp2-unmodified/examples/ccf_related_work/server_ccf.h"
#include "ngtcp2-unmodified/examples/template.h"
#include "ngtcp2-unmodified/examples/util.h"
#include <arpa/inet.h>
#include <cassert>
#include <chrono>
#include <fstream>
#include <iostream>
#include <regex>
#include <string>

using namespace std;
std::mutex leader_mtx;




struct metadata
{
  uint64_t client_req_id; // req_id assigned by client
  uint64_t latency_ns; // replication latency

  metadata() = delete;
  explicit metadata(uint64_t req, uint64_t lat) :
    client_req_id(req),
    latency_ns(lat){};

  metadata(const metadata& other)
  {
    client_req_id = other.client_req_id;
    latency_ns = other.latency_ns;
  };

  metadata(metadata&& other)
  {
    client_req_id = other.client_req_id;
    latency_ns = other.latency_ns;
  };

  metadata& operator=(const metadata& other)
  {
    client_req_id = other.client_req_id;
    latency_ns = other.latency_ns;
    return *this;
  }

  metadata& operator=(metadata&& other)
  {
    client_req_id = other.client_req_id;
    latency_ns = other.latency_ns;
    return *this;
  }

  friend std::ostream& operator<<(std::ostream& os, const metadata& m)
  {
    os << "(client_req_id=" << m.client_req_id
       << ", latency (ns)=" << m.latency_ns << ")\n";
    return os;
  }
};

std::map<int, metadata> latencies;

template <class K, class V>
std::ostream& operator<<(std::ostream& os, const std::map<K, V>& map)
{
  for (auto& elem : map)
    os << "(" << elem.first << ", latency=" << elem.second << "ns)\n";
  return os;
}
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

static uint64_t get_timestamp_ns()
{
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
           std::chrono::steady_clock::now().time_since_epoch())
    .count();
}

void empty_func(
  std::weak_ptr<void> driver,
  uint64_t client_req_id,
  uint8_t* data = nullptr,
  size_t sz_data = 0
  )
{}


uint64_t ccf_committed_seqno(
  std::weak_ptr<void> driver) {
  std::shared_ptr<void> drv_shared = driver.lock();
  if (drv_shared)
  {
    std::shared_ptr<RaftDriver> raft_drv =
    std::static_pointer_cast<RaftDriver>(drv_shared);
    return raft_drv->get_committed_seqno();
  }
  else
  {
    fmt::print("{} error\n", __PRETTY_FUNCTION__);
    assert(false);
    return 0;
  }
}

void ccf_replication(
  std::weak_ptr<void> driver,
  uint64_t client_req_id,
  uint8_t* data = nullptr,
  size_t sz_data = 0)
{
  static double sum_latency = 0.0;
  static int count = 0;

  static int reqs_no = 0;
  static int log_id = 0;
  std::shared_ptr<void> drv_shared = driver.lock();
  if (drv_shared)
  {
    assert(sz_data > 0);
    auto data_to_replicate = std::make_shared<std::vector<uint8_t>>(sz_data);
    ::memcpy(data_to_replicate->data(), data, sz_data);
    std::shared_ptr<RaftDriver> raft_drv =
    std::static_pointer_cast<RaftDriver>(drv_shared);

    reqs_no++;
    uint64_t zil_blk_id = 2;// std::stoll;
    ::memcpy(&zil_blk_id, data_to_replicate->data(), sizeof(uint64_t));
    auto now_ts = get_timestamp_ns();
    if (client_req_id <= raft_drv->get_committed_seqno()) {
      std::cout << "*====RAFT====* " << __PRETTY_FUNCTION__
                << " client_req_id=" << client_req_id
                << " zil_blk_id=" << zil_blk_id
                << " committed_seqno=" << raft_drv->get_committed_seqno()
                << " ERROR\n";
      exit(-1);
    }
    if (client_req_id % 10000 ==0) {
      std::cout << "*====RAFT====* " << __PRETTY_FUNCTION__
                << " client_req_id=" << client_req_id
                << " zil_blk_id=" << zil_blk_id
                << " size=" << sz_data
                << " committed_seqno=" << raft_drv->get_committed_seqno()
                << "\n";
   }
    raft_drv->replicate_commitable("2", data_to_replicate, 0);
    if (reqs_no % 50000 == 0)
    {
#ifdef KEEP_LATENCIES
      std::string fname = "output_" + std::to_string(log_id) + ".txt";
      log_id++;
      std::ofstream file(fname);

      // check if the file is open
      if (!file.is_open())
      {
        fmt::print("{} failed to open log file={}\n", __func__, fname);
        exit(-1);
      }
      file << latencies << std::endl;
      file.flush();
      file.close();
#endif
      // std::cout << latencies;
      latencies.clear();
    }
    

   #if 0
    // wait until committed
    while (raft_drv->get_committed_seqno() < reqs_no)
    {
      std::cout << __PRETTY_FUNCTION__
                << "After replicate_commitable: reqs_no=" << reqs_no
                << " committed_seqno=" << raft_drv->get_committed_seqno()
                << "\n";
    }
     std::cout << __PRETTY_FUNCTION__
                << "After replicate_commitable: reqs_no=" << reqs_no
                << " committed_seqno=" << raft_drv->get_committed_seqno()
                << "\n";
    #endif
    auto end_ts = get_timestamp_ns();
    latencies.insert(
      std::make_pair(reqs_no, metadata(client_req_id, (end_ts - now_ts))));
    sum_latency += (end_ts - now_ts);
    count ++;
    if (client_req_id % 10000 == 0) {  
      std::cout << "*====RAFT====* " << __func__ 
                << " client_req_id=" << client_req_id
                << " zil_blk_id=" << zil_blk_id
                << " committed_seqno=" << raft_drv->get_committed_seqno()
                << " latency (ns)=" << (end_ts - now_ts) <<  " avg_lat = " << (sum_latency/(1.0*count)) << " ns\n";
    }
  }
  else
  {
    fmt::print("{} error\n", __PRETTY_FUNCTION__);
    assert(false);
  }
}


void ccf_replication_blocking(
  std::weak_ptr<void> driver,
  uint64_t client_req_id,
  uint8_t* data = nullptr,
  size_t sz_data = 0)
{
  static double sum_latency = 0.0;
  static int count = 0;

  static int reqs_no = 0;
  static int log_id = 0;
  // fmt::print("{} here\n", __func__);
  std::shared_ptr<void> drv_shared = driver.lock();
  if (drv_shared)
  {
    assert(sz_data > 0); // FIXME:@dimitra
    // fmt::print("{} sz_data={}B\n", __func__, sz_data);
    // sz_data =0;
    auto data_to_replicate = std::make_shared<std::vector<uint8_t>>(sz_data);
    ::memcpy(data_to_replicate->data(), data, sz_data);
    std::shared_ptr<RaftDriver> raft_drv =
    std::static_pointer_cast<RaftDriver>(drv_shared);

    reqs_no++;
    if (reqs_no % 50000 == 0)
    {
#ifdef KEEP_LATENCIES
      std::string fname = "output_" + std::to_string(log_id) + ".txt";
      log_id++;
      std::ofstream file(fname);

      // Check if the file is open
      if (!file.is_open())
      {
        fmt::print("{} failed to open log file={}\n", __func__, fname);
        exit(-1);
      }

      // write latencies to the file
      file << latencies << std::endl;

      // flush the output buffer to the file
      file.flush();

      // close the file
      file.close();
#endif
      // std::cout << latencies;
      latencies.clear();
    }
    auto now_ts = get_timestamp_ns();

    if (reqs_no % 10000 ==0) {
      std::cout << __PRETTY_FUNCTION__
                << " reqs_no=" << reqs_no
                << " committed_seqno=" << raft_drv->get_committed_seqno()
                << "\n";
   }
   #if 0
   std::cout << __PRETTY_FUNCTION__
                << "Before replicate_commitable: reqs_no=" << reqs_no
                << " committed_seqno=" << raft_drv->get_committed_seqno()
                << "\n";
   
    #endif
     raft_drv->replicate_commitable("2", data_to_replicate, 0);
    // wait until committed
    while (raft_drv->get_committed_seqno() < reqs_no)
    {
#if 0
      std::cout << __PRETTY_FUNCTION__
                << "After replicate_commitable: reqs_no=" << reqs_no
                << " committed_seqno=" << raft_drv->get_committed_seqno()
                << "\n";
#endif
    }
    
    auto end_ts = get_timestamp_ns();
    latencies.insert(
      std::make_pair(reqs_no, metadata(client_req_id, (end_ts - now_ts))));
    sum_latency += (end_ts - now_ts);
    count ++;
    if (count % 1000 == 0) {  
      std::cout << __PRETTY_FUNCTION__ << " reqs_no=" << reqs_no
              << " committed_seqno=" << raft_drv->get_committed_seqno()
              << " latency (ns)=" << (end_ts - now_ts) <<  " avg_lat = " << (sum_latency/(1.0*count)) << "\n";
    }
  }
  else
  {
    fmt::print("{} error\n", __PRETTY_FUNCTION__);
    assert(false);
  }
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
#ifdef SECOND_FOLLOWER
    my_connections.insert(std::make_pair(
      ccf::NodeId("2"), network_stack::connectivity_description()));
#endif
    my_connections[ccf::NodeId(std::to_string(primary_node))].nid =
      ccf::NodeId(std::to_string(primary_node));
    my_connections[ccf::NodeId(std::to_string(primary_node))].ip =
      primary_ip; // CVM

    my_connections[ccf::NodeId(std::to_string(primary_node))]
      .base_listening_port = primary_listening_port;
    my_connections[ccf::NodeId(std::to_string(primary_node))]
      .base_sending_port = primary_sending_port;

    my_connections[ccf::NodeId(std::to_string(follower_1))].nid =
      ccf::NodeId(std::to_string(follower_1));
    my_connections[ccf::NodeId(std::to_string(follower_1))].ip =
      follower_1_ip; // CVM

    my_connections[ccf::NodeId(std::to_string(follower_1))]
      .base_listening_port = follower_1_listening_port;
    my_connections[ccf::NodeId(std::to_string(follower_1))].base_sending_port =
      follower_1_sending_port;
#ifdef SECOND_FOLLOWER
    my_connections[ccf::NodeId(std::to_string(follower_2))].nid =
      ccf::NodeId(std::to_string(follower_2));
    my_connections[ccf::NodeId(std::to_string(follower_2))].ip =
      follower_2_ip; // CVM

    my_connections[ccf::NodeId(std::to_string(follower_2))]
      .base_listening_port = follower_2_listening_port;
    my_connections[ccf::NodeId(std::to_string(follower_2))].base_sending_port =
      follower_2_sending_port;
#endif
  }
}

std::atomic<bool> stop;
std::atomic<int> total_acks;

static void apply_cmds(std::shared_ptr<RaftDriver> driver)
{
  static std::atomic<int> reqs_nb = 0;
  for (;;)
  {
    auto [src_node, data, data_sz] = driver->message_queue.pop();
    if (data_sz > 0)
    {
      reqs_nb.fetch_add(1);
      // fmt::print("{} --> data_sz={}\n", __func__, data_sz);
      auto src_node_str = ccf::NodeId(std::to_string(src_node));
      driver->periodic_applying(src_node_str, data.get(), data_sz);
#if 1
      if (reqs_nb.load() % 100 == 0)
        fmt::print(
          "{} src_node={}, cmt_idx={}, reqs_no={}, data_sz={}\n",
          __func__,
          src_node_str,
          driver->get_committed_seqno(),
          reqs_nb.load(), data_sz);
#endif
    }
    else if (data_sz == 0)
    {
      if (stop.load())
        return;
    }
  }
}

static void listen_for_acks(std::shared_ptr<RaftDriver> driver, int node_id)
{
  fmt::print("{}: thread_id={}\n", __func__, socket_layer::get_thread_id());
  int acks = 0;
  for (;;)
  {
    {
      // std::unique_lock<std::mutex> tmp_l(leader_mtx);
      acks += driver->periodic_listening_acks(std::to_string(node_id));
    }
    total_acks.fetch_add(1);
    if (acks % 10000 == 0)
    {
      fmt::print(
        "{} acks={} from node_id={}, cmt_idx={}\n",
        __func__,
        acks,
        node_id,
        driver->get_committed_seqno());
    }
    /*
    if (acks == k_num_requests)
      return;
    */
  }
}

int main(int argc, char* argv[])
{
  std::string node_id;
  std::cin >> node_id;
  // here starts the original driver_raft logic

  threading::ThreadMessaging::init(
    1); // @dimitra:TODO -> this is not used actually
  authentication::init();
  stop.store(false);
  total_acks.store(0);

  std::vector<std::thread> threads_leader;
  auto driver = make_shared<RaftDriver>(node_id);

  config_parser::initialize_with_data(driver->my_connections);
  auto start = std::chrono::high_resolution_clock::now();
  auto leader_end = std::chrono::high_resolution_clock::now();

  if (ccf::NodeId(node_id) == ccf::NodeId(std::to_string(primary_node)))
  {
    std::cout << __func__ << " primary node_id=" << node_id << "\n";
    config_set_default(config);
    if (argc - optind < 4)
    {
      std::cerr << "Too few arguments" << std::endl;
      print_usage();
      exit(EXIT_FAILURE);
    }

    auto addr = argv[optind++];
    auto port = argv[optind++];
    auto private_key_file = argv[optind++];
    auto cert_file = argv[optind++];

    if (auto n = util::parse_uint(port); !n)
    {
      std::cerr << "port: invalid port number" << std::endl;
      exit(EXIT_FAILURE);
    }
    else if (*n > 65535)
    {
      std::cerr << "port: must not exceed 65535" << std::endl;
      exit(EXIT_FAILURE);
    }
    else
    {
      config.port = *n;
    }

    TLSServerContext tls_ctx;

    if (tls_ctx.init(private_key_file, cert_file, AppProtocol::H3) != 0)
    {
      exit(EXIT_FAILURE);
    }

    if (config.htdocs.back() != '/')
    {
      config.htdocs += '/';
    }

    fmt::print("{} using document root:{}\n", __func__, config.htdocs);

    auto ev_loop_d = defer(ev_loop_destroy, EV_DEFAULT);
    if (util::generate_secret(config.static_secret) != 0)
    {
      fmt::print("{} unable to generate static secret\n", __func__);
      exit(EXIT_FAILURE);
    }

    std::vector<std::thread> threads_leader;

    driver->make_primary(
      ccf::NodeId(node_id),
      driver->my_connections[std::to_string(primary_node)].ip,
      driver->my_connections[std::to_string(primary_node)].base_listening_port);
    driver->become_primary();
    fmt::print("{} created primary\n", __func__);
    driver->create_new_nodes(
      std::map<std::string, ccf::kv::Configuration::NodeInfo>{
        std::make_pair(
          std::to_string(primary_node),
          ccf::kv::Configuration::NodeInfo(primary_ip, std::to_string(primary_listening_port))),
        std::make_pair(
          std::to_string(follower_1),
          ccf::kv::Configuration::NodeInfo(
            follower_1_ip, std::to_string(follower_1_listening_port)))
#ifdef SECOND_FOLLOWER
          ,
        std::make_pair(
          std::to_string(follower_2),
          ccf::kv::Configuration::NodeInfo(
            follower_2_ip, std::to_string(follower_2_listening_port)))
#endif
      }); //
    auto data = std::make_shared<std::vector<uint8_t>>();
    auto& vec = *(data.get());
    fmt::print("{} #1\n", __func__);
    int acks = 0;
    acks += driver->periodic_listening_acks(std::to_string(follower_1));
    fmt::print("{} #2\n", __func__);
#ifdef SECOND_FOLLOWER
    acks += driver->periodic_listening_acks(std::to_string(follower_2));
    fmt::print("{} #3\n", __func__);
#endif

    threads_leader.emplace_back(
      std::thread(listen_for_acks, driver, follower_1));
#ifdef SECOND_FOLLOWER
    threads_leader.emplace_back(
      std::thread(listen_for_acks, driver, follower_2));
#endif
    fmt::print("{} QUIC server\n", __PRETTY_FUNCTION__);
    // =============== QUIC

    ccf_monitor_ptr = std::make_unique<ccf_monitor>();
    Server s(EV_DEFAULT, tls_ctx);
   
    s.init(addr, port);
    s.assign_server_id(5);

#ifdef STANDALONE
#  warning "STANDALONE BENCHMARK"
    std::shared_ptr<ccf_callbacks_set> ptr_callable =
      std::make_shared<ccf_callbacks_set>(
        driver, empty_func);
    s.register_ccf_functions(ptr_callable);
#else
#ifdef BLOCKING_REPLICATION
#warning "BLOCKING REPLICATION"
    std::shared_ptr<ccf_callbacks_set> ptr_callable =
      std::make_shared<ccf_callbacks_set>(driver, ccf_replication_blocking);
      s.register_ccf_functions(ptr_callable);

#else
#warning "NON BLOCKING REPLICATION"
    std::shared_ptr<ccf_callbacks_set> ptr_callable =
      std::make_shared<ccf_callbacks_set>(driver, ccf_replication, ccf_committed_seqno);
    s.register_ccf_functions(ptr_callable);
#endif
#endif
    ccf_monitor_ptr->set_quic_server(&s);
    ccf_monitor_ptr->started.store(true);

    ev_run(EV_DEFAULT, 0);

    s.disconnect();
    s.close();
    // =============== QUIC end

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
        threads_leader.emplace_back(
          std::thread(listen_for_acks, driver, follower_1));
        threads_leader.emplace_back(
          std::thread(listen_for_acks, driver, follower_2));
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
#if 1
      fmt::print(
        "{} --> get_committed_seqno()={}\n",
        __func__,
        driver->get_committed_seqno());
#endif
      if (driver->get_committed_seqno() == k_num_requests)
        break;
      std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }

    leader_end = std::chrono::high_resolution_clock::now();
    fmt::print(
      "{} ---> taken timestamp={}s\n", __func__, driver->get_committed_seqno());

    threads_leader[0].join();
    threads_leader[1].join();

    driver->replicate_commitable("2", data, 0);

    acks += driver->periodic_listening_acks(std::to_string(follower_1));
    acks += driver->periodic_listening_acks(std::to_string(follower_2));

    // threads_leader[0].join();
    driver->close_connections(std::to_string(primary_node));
    driver->close_connections(std::to_string(follower_1));
    driver->close_connections(std::to_string(follower_2));
  }
  else
  {
    std::cout << __func__ << " secondary node_id=" << node_id << "\n";
    std::vector<std::thread> threads_follower;
    driver->make_follower(
      ccf::NodeId(node_id),
      driver->my_connections[ccf::NodeId(node_id)].ip,
      driver->my_connections[ccf::NodeId(node_id)].base_listening_port);
    int count = 0;
    count += driver->establish_state(std::to_string(primary_node));
    for (auto i = 0ULL; i < k_num_requests; i++)
    {
      count += driver->periodic_listening(std::to_string(primary_node));
      if (i == 0)
      {
        threads_follower.emplace_back(std::thread(apply_cmds, driver));
      }
    }
    for (;;)
    {
      stop.store(true);
      fmt::print(
        "{} --> get_committed_seqno()={}\n",
        __func__,
        driver->get_committed_seqno());
      if (driver->get_committed_seqno() == k_num_requests)
        break;
      std::this_thread::sleep_for(std::chrono::milliseconds(1000));

      count += driver->establish_state(std::to_string(primary_node));
    }
    // count += driver->periodic_listening(std::to_string(primary_node));
    driver->close_connections(node_id);

    // driver->close_connections(std::to_string(primary_node));
    threads_follower[0].join();
  }
  auto end = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double> duration = end - start;
  std::chrono::duration<double> leader_duration = leader_end - start;

  fmt::print(
    "{}: total time elapsed={}s, time elapsed in leader={}s, tput={} op/s, avg "
    "latency={} ms, nb_sends={}, "
    "nb_syscalls_writes={} "
    "nb_recvs={}, nb_syscalls_reads={}, bytes_sent={}, bytes_received={}, "
    "raft_committed_seqno={}, ledger_size={}\n",
    __func__,
    duration.count(),
    leader_duration.count(),
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
