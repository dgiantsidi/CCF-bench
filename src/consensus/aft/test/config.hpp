#pragma once
#include "ccf/entity_id.h"
#include "consensus/aft/raft.h"
#include "consensus/aft/raft_types.h"

constexpr int k_num_requests = 4e6; // 500000;
constexpr int payload_sz = sizeof(size_t);
constexpr int payload_sz_entry = sizeof(size_t) + sizeof(bool)+ sizeof(ccf::kv::Term) + sizeof(ccf::kv::Version); //<payload_size><term><kv_version>;