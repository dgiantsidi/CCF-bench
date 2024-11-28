#pragma once
#include "ccf/entity_id.h"
#include "consensus/aft/raft.h"
#include "consensus/aft/raft_types.h"

constexpr int k_num_requests = 100000; // 500000;
constexpr int payload_sz = sizeof(size_t);
constexpr int payload_sz_entry = sizeof(size_t) + sizeof(bool) +
  sizeof(ccf::kv::Term) +
  sizeof(ccf::kv::Version); //<payload_size><term><kv_version>;

constexpr int primary_node = 0;
std::string primary_ip = "10.1.0.7";
constexpr int primary_listening_port = 1800;
constexpr int primary_sending_port = 1900;

constexpr int follower_1 = 1;
std::string follower_1_ip = "10.1.0.4";
constexpr int follower_1_listening_port = 2800;
constexpr int follower_1_sending_port = 2900;

constexpr int follower_2 = 2;
std::string follower_2_ip = "0.0.0.0";
constexpr int follower_2_listening_port = 3800;
constexpr int follower_2_sending_port = 3900;