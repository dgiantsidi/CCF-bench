#pragma once
#include "ccf/ds/logger.h"
#include "consensus/aft/raft.h"
#include "logging_stub.h"
//#include "networking_api.h"

#include <chrono>
#include <random>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>

std::string stringify(const std::vector<uint8_t>& v, size_t max_size = 15ul)
{
  auto size = std::min(v.size(), max_size);
  return fmt::format(
    "[{} bytes] {}", v.size(), std::string(v.begin(), v.begin() + size));
}

std::string stringify(const std::optional<std::vector<uint8_t>>& o)
{
  if (o.has_value())
  {
    return stringify(*o);
  }

  return "MISSING";
}

struct LedgerStubProxy_Mermaid : public aft::LedgerStubProxy
{
  using LedgerStubProxy::LedgerStubProxy;

  void put_entry(
    const std::vector<uint8_t>& data,
    bool globally_committable,
    ccf::kv::Term term,
    ccf::kv::Version index) override
  {
#if 0
    fmt::print(
      "{}->>{}: [ledger] appending: term={} index={} -> data: {}\n",
      _id,
      _id,
      term,
      index,
      stringify(data));
#endif
    aft::LedgerStubProxy::put_entry(data, globally_committable, term, index);
  }

  void truncate(aft::Index idx) override
  {
    // fmt::print("{}->>{}: [ledger] truncating to {}\n", _id, _id, idx);
    aft::LedgerStubProxy::truncate(idx);
  }
};

struct LoggingStubStore_Mermaid : public aft::LoggingStubStoreConfig
{
  using LoggingStubStoreConfig::LoggingStubStoreConfig;

  void compact(aft::Index idx) override
  {
    fmt::print("{}->>{}: [KV] compacting to {}\n", _id, _id, idx);
    aft::LoggingStubStoreConfig::compact(idx);
  }

  void rollback(const ccf::kv::TxID& tx_id, aft::Term t) override
  {
#if 0
    fmt::print(
      "{}->>{}: [KV] rolling back to {}.{}, in term {}\n",
      _id,
      _id,
      tx_id.term,
      tx_id.version,
      t);
#endif
    aft::LoggingStubStoreConfig::rollback(tx_id, t);
  }

  void initialise_term(aft::Term t) override
  {
    fmt::print("{}->>{}: [KV] initialising in term {}\n", _id, _id, t);
    aft::LoggingStubStoreConfig::initialise_term(t);
  }
};

using TRaft = aft::Aft<LedgerStubProxy_Mermaid>;
