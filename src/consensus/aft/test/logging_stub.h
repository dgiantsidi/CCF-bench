// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache 2.0 License.
#pragma once

#include "ccf/entity_id.h"
#include "consensus/aft/raft.h"
#include "consensus/aft/raft_types.h"
#include "ngtcp2-unmodified/examples/ccf_related_work/config.h"

#include <map>
#include <optional>
#include <tuple>
#include <vector>

namespace aft
{
  static std::tuple<int, int, int> deserialize_data_and_print(
    const char* func, uint8_t* data, size_t sz_data)
  {
    // from /home/azureuser/ngtcp2/examples/client.cc
    // ::memcpy(stream->sent_data.data(), &last_cmt->blk_id, sizeof(uint64_t));
    // ::memcpy(stream->sent_data.data() + sizeof(uint64_t), last_cmt->tail_commitment, COMMITMENT_SIZE);
    // ::memcpy(stream->sent_data.data() + sizeof(uint64_t) + COMMITMENT_SIZE, &(last_cmt->blk_type), sizeof(int));
    // ::memcpy(stream->sent_data.data() + sizeof(uint64_t) + COMMITMENT_SIZE + sizeof(int), &client_id /*fs_id*/, sizeof(int));
    // ::memcpy(stream->sent_data.data() + sizeof(uint64_t) + COMMITMENT_SIZE + sizeof(int) + sizeof(int), &attestation_id /*emphemeral_id*/, sizeof(int));

    uint64_t zil_blk_id;
    ::memcpy(&zil_blk_id, data, sizeof(uint64_t));
    uint64_t cmt[4];
    ::memcpy(cmt, data + sizeof(uint64_t), sizeof(cmt));

    int commitment_type = -1;
    ::memcpy(
      &commitment_type, data + sizeof(uint64_t) + sizeof(cmt), sizeof(int));

    // get the filesystem id
    int fs_id = -1;
    ::memcpy(
      &fs_id, data + sizeof(uint64_t) + sizeof(cmt) + sizeof(int), sizeof(int));

    // get the attestation report (denoted with an id), it is emphemeral to
    // distinguish between different mounts of the filesystem
    int attestation_id = -1;
    ::memcpy(
      &attestation_id,
      data + sizeof(uint64_t) + sizeof(cmt) + 2 * sizeof(int),
      sizeof(int));
    char ub_digest[UBERBLOCK_DIGEST_BUF_SIZE];
    uint64_t zil_head_blk_id;
    if (commitment_type == (int)block_type::UB) { 
      ::memcpy(&zil_head_blk_id, data + sizeof(uint64_t) + sizeof(cmt) + 3 * sizeof(int), sizeof(uint64_t));
      ::memcpy(ub_digest, data + sizeof(uint64_t) + sizeof(cmt) + 3 * sizeof(int) + sizeof(uint64_t), (UBERBLOCK_DIGEST_BUF_SIZE-1));
      ub_digest[UBERBLOCK_DIGEST_BUF_SIZE-1] = '\0';
    }

    {
      using u_longlong_t = long long unsigned;
      if (commitment_type == (int)block_type::UB) {
        fmt::print(
          "{}->{} deserialized (size={}) fs_id={}: ub_tx={}, commitment_type={}, , "
          "head_cmt=[{:016x}:{:016x}:{:016x}:{:016x}], zil_head_blk_id={}, ub_digest={}\n",
          func,
          __func__,
          sz_data,
          fs_id,
          zil_blk_id,
          (commitment_type == (int)block_type::TAIL) ? "TAIL" : "UB",
          (u_longlong_t)cmt[0],
          (u_longlong_t)cmt[1],
          (u_longlong_t)cmt[2],
          (u_longlong_t)cmt[3],
          zil_head_blk_id,
          ub_digest);
      } else {
        fmt::print(
          "{}->{} deserialized (size={}) fs_id={}: zil_blk_id={}, commitment_type={}, "
          "cmt=[{:016x}:{:016x}:{:016x}:{:016x}]\n",
          func,
          __func__,
          sz_data,
          fs_id,
          zil_blk_id,
          (commitment_type == (int)block_type::TAIL) ? "TAIL" : "UB",
          (u_longlong_t)cmt[0],
          (u_longlong_t)cmt[1],
          (u_longlong_t)cmt[2],
          (u_longlong_t)cmt[3]);
      }
    }
     
    return {commitment_type, fs_id, attestation_id};
  }

  enum class ReplicatedDataType
  {
    raw = 0,
    reconfiguration = 1,
    retired_committed = 2
  };
  DECLARE_JSON_ENUM(
    ReplicatedDataType,
    {{ReplicatedDataType::raw, "raw"},
     {ReplicatedDataType::reconfiguration, "reconfiguration"},
     {ReplicatedDataType::retired_committed, "retired_committed"}});

  struct ReplicatedData
  {
    ReplicatedDataType type;
    std::vector<uint8_t> data;
  };
  DECLARE_JSON_TYPE(ReplicatedData);
  DECLARE_JSON_REQUIRED_FIELDS(ReplicatedData, type, data);

  class LedgerStubProxy
  {
  protected:
    ccf::NodeId _id;
    std::mutex ledger_access;

  public:
    std::vector<std::vector<uint8_t>> ledger;
    using ledger_by_idx_t = std::map<Index, std::vector<uint8_t>>;
    using filesystem_id = int;
    //ledger_by_idx_t ledger_by_idx;
    std::map<filesystem_id, ledger_by_idx_t> tail_ledger_by_fs_id;
    std::map<filesystem_id, ledger_by_idx_t> ub_ledger_by_fs_id;
    ledger_by_idx_t aux_ledger_by_fs_id;
    Index cur_idx = 0;
    uint64_t skip_count = 0;

    LedgerStubProxy(const ccf::NodeId& id) : _id(id) {}

    virtual void init(Index, Index) {}

    size_t ledger_size()
    {
      fmt::print("{}\n", __func__);
      #if 0
      if (cur_idx != ledger.size()) {
        fmt::print(
          "{}: cur_idx={} is different from ledger.size()={}\n",
          __func__,
          cur_idx,
          ledger.size());
      }
      #endif
      return cur_idx;
      return ledger.size();
    }

    virtual void put_entry(
      const std::vector<uint8_t>& original,
      bool globally_committable,
      ccf::kv::Term term,
      ccf::kv::Version index)
    {
      fmt::print("{}\n", __func__);
      
      std::lock_guard<std::mutex> lock(ledger_access);

      // The payload that we eventually deserialise must include the
      // ledger entry as well as the View and Index that identify it, and
      // whether this is committable. In the real entries, they are nested in
      // the payload and the IV. For test purposes, we just prefix them manually
      // (to mirror the deserialisation in LoggingStubStore::ExecutionWrapper).
      // We also size-prefix, so in a buffer of multiple of these messages we
      // can extract each with get_entry
      const size_t idx = ledger_size() + 1;
      assert(idx == index);
      auto additional_size =
        sizeof(size_t) + sizeof(bool) + sizeof(term) + sizeof(index);
      std::vector<uint8_t> combined(additional_size);
      {
        uint8_t* data = combined.data();
        serialized::write(
          data,
          additional_size,
          (sizeof(bool) + sizeof(term) + sizeof(index) + original.size()));
        serialized::write(data, additional_size, globally_committable);
        serialized::write(data, additional_size, term);
        serialized::write(data, additional_size, index);
      }

      combined.insert(combined.end(), original.begin(), original.end());

      ReplicatedData r =
        nlohmann::json::parse(std::span{original.data(), original.size()});
      if (r.type == ReplicatedDataType::raw)
      {
        [[__maybe_unused__]] auto [commitment_type, fs_id, attestation_id] =
          deserialize_data_and_print(__func__, r.data.data(), r.data.size());
          if (commitment_type == (int)block_type::TAIL) {
            tail_ledger_by_fs_id[fs_id][index] = combined;
          }
          else {
            ub_ledger_by_fs_id[fs_id][index] = combined;
          }
      }
      else {
        aux_ledger_by_fs_id[index] = combined;
      }

#if 0
      fmt::print(
        "{} [{}] ---> globally_committable={}, term={}, index={}, "
        "payload_size={}, combined_size={}\n",
        __func__,
        _id,
        globally_committable,
        term,
        index,
        original.size(),
        combined.size());
#endif
      
      // ledger_by_idx[index] = combined;
      cur_idx = index;
      // ledger.push_back(combined);
    }

    void skip_entry(const uint8_t*& data, size_t& size)
    {
      fmt::print("{}\n", __func__);
      get_entry(data, size);
      ++skip_count;
    }

    static std::vector<uint8_t> get_entry(const uint8_t*& data, size_t& size)
    {
      fmt::print("{}\n", __func__);
#if 1
      // fmt::print("{} --> size={}\n", __func__, size);
      const auto entry_size = serialized::read<size_t>(data, size);
      // fmt::print("{} --> entry_size={}\n", __func__, entry_size);
      std::vector<uint8_t> entry(data, data + entry_size);
      serialized::skip(data, size, entry_size);
      // fmt::print("{} ---> data.size() should be 0={}\n", __func__, size);
#endif
      return entry;
    }

    std::optional<std::vector<uint8_t>> get_entry_by_idx(size_t idx)
    {
      fmt::print("{}\n", __func__);
      #if 1
      std::lock_guard<std::mutex> lock(ledger_access);
      // Ledger indices are 1-based, hence the -1
      if (idx > 0 && idx <= ledger.size())
      {
#if 1
        fmt::print(
          "{} -> idx={} entry_size={}\n",
          __func__,
          idx,
          ledger[idx - 1].size());
#endif
        
        
        //return ledger[idx - 1];
      }
      #endif
      for (auto& fs_ledger : tail_ledger_by_fs_id) {
        auto& ledger_by_idx = fs_ledger.second;
        if (ledger_by_idx.find(idx) != ledger_by_idx.end())
        {
          return ledger_by_idx[idx];
        }
      }
      for (auto& fs_ledger : ub_ledger_by_fs_id) {
        auto& ledger_by_idx = fs_ledger.second;
        if (ledger_by_idx.find(idx) != ledger_by_idx.end())
        {
          return ledger_by_idx[idx];
        }
        
      }
      if (auto it = aux_ledger_by_fs_id.find(idx);
          it != aux_ledger_by_fs_id.end())
      {
        return it->second;
      }

      fmt::print("{} --> no entry found at idx={}\n", __func__, idx);
      return std::nullopt;
    }

    std::optional<std::vector<uint8_t>> get_raw_entry_by_idx(size_t idx)
    {
      fmt::print("{}\n", __func__);
      auto data = get_entry_by_idx(idx);
      #if 1
      if (data.has_value())
      {
        // Remove the View and Index that were written during put_entry
        data->erase(
          data->begin(),
          data->begin() + sizeof(size_t) + sizeof(ccf::kv::Term) +
            sizeof(ccf::kv::Version));
      }
      #endif
      return data;
    }

    std::optional<std::vector<uint8_t>> get_append_entries_payload(
      const aft::AppendEntries& ae)
    {
      fmt::print("{}\n", __func__);
      std::vector<uint8_t> payload;
      #if 1
      for (auto idx = ae.prev_idx + 1; idx <= ae.idx; ++idx)
      {
        auto entry_opt = get_entry_by_idx(idx);
        if (!entry_opt.has_value())
        {
          return std::nullopt;
        }

        const auto& entry = *entry_opt;
        payload.insert(payload.end(), entry.begin(), entry.end());
      }
      #endif
      return payload;
    }

    virtual void truncate(Index idx)
    {
      fmt::print("{}\n", __func__);
      ledger.resize(idx);
    }

    void reset_skip_count()
    {
      skip_count = 0;
    }

    std::ostream& print_ledgers(std::ostream& os) const
    {
       auto additional_size =
        sizeof(size_t) + sizeof(bool) + + sizeof(ccf::kv::Term) +
            sizeof(ccf::kv::Version);
      os << "====== tail_ledger_by_fs_id ======\n";
      for (auto& fs_ledger : tail_ledger_by_fs_id) {
       auto& ledger_by_idx = fs_ledger.second;
        os << ">Filesystem " << fs_ledger.first << " has " << ledger_by_idx.size() << " entries\n";
        for (auto& [index, entry] : ledger_by_idx) {
          ReplicatedData r = nlohmann::json::parse(
            std::span{entry.data() + additional_size, entry.size() - additional_size});
          os << "  Index: " << index << ": ";
          deserialize_data_and_print(__func__, r.data.data(), r.data.size());
        }
      }
      os << "====== ub_ledger_by_fs_id ======\n";
      for (auto& fs_ledger : ub_ledger_by_fs_id) {
        auto& ledger_by_idx = fs_ledger.second;
        os << ">Filesystem " << fs_ledger.first << " has " << ledger_by_idx.size() << " entries\n";
        for (auto& [index, entry] : ledger_by_idx) {
          ReplicatedData r = nlohmann::json::parse(
            std::span{entry.data() + additional_size, entry.size() - additional_size});
          os << "  Index: " << index << ": ";
          auto [cmt_type, fs_id, attestation_id] = deserialize_data_and_print(__func__, r.data.data(), r.data.size());
          os << "    --> commitment_type=" << ((cmt_type == (int)block_type::TAIL) ? "TAIL" : "UB") << "\n";
        }
      }
      os << "====== aux_ledger_by_fs_id ======\n";
      return os;
    }

    void commit(Index idx)
    {
      std::lock_guard<std::mutex> lock(ledger_access);
      fmt::print("{} --> committing up to index={}\n", __func__, idx);

      // In a real ledger, commit would make the entries available for
      // deserialisation. In our stub, they are already available, so we just
      // print the commit and do nothing else.

      for (auto it = tail_ledger_by_fs_id.begin(); it != tail_ledger_by_fs_id.end();)
      {
        auto& [fs_id, commitment_store] = *it;
        auto first_uncommitted_it = commitment_store.upper_bound(idx);
        auto last_committed_it = std::prev(first_uncommitted_it);

        if (last_committed_it != commitment_store.end())
        {
          fmt::print(
            "{} [TAIL] fs_id={} last_committed_index={} "
            "first_uncommitted_index={} (=0 for if there are no uncommitted "
            "entries)\n",
            __PRETTY_FUNCTION__,
            fs_id,
            last_committed_it->first,
            first_uncommitted_it != commitment_store.end() ?
              first_uncommitted_it->first :
              0);
          commitment_store.erase(commitment_store.begin(), last_committed_it);
        }

        if (commitment_store.empty())
        {
          fmt::print(
            "{} --> cmt_tail_store: is empty after compacting up to index={}\n",
            __PRETTY_FUNCTION__,
            fs_id,
            idx);
          it = tail_ledger_by_fs_id.erase(it);
        }
        else
        {
          ++it;
        }
      }

      for (auto it = ub_ledger_by_fs_id.begin(); it != ub_ledger_by_fs_id.end();)
      {
        auto& [fs_id, commitment_store] = *it;
        auto first_uncommitted_it = commitment_store.upper_bound(idx);
        auto last_committed_it = std::prev(first_uncommitted_it);
        auto second_to_last_committed_it = last_committed_it != commitment_store.begin() ?
          std::prev(last_committed_it) :
          commitment_store.end();

        if (second_to_last_committed_it != commitment_store.end())
        {
          fmt::print(
            "{} [TAIL] fs_id={} last_committed_index={} "
            "first_uncommitted_index={} (=0 for if there are no uncommitted "
            "entries)\n",
            __PRETTY_FUNCTION__,
            fs_id,
            second_to_last_committed_it->first,
            first_uncommitted_it != commitment_store.end() ?
              first_uncommitted_it->first :
              0);
          commitment_store.erase(commitment_store.begin(), second_to_last_committed_it);
        }

        if (commitment_store.empty())
        {
          fmt::print(
            "{} --> cmt_ub_store: is empty after compacting up to index={}\n",
            __PRETTY_FUNCTION__,
            fs_id,
            idx);
          it = ub_ledger_by_fs_id.erase(it);
        }
        else
        {
          ++it;
        }
      }

      print_ledgers(std::cout) << std::endl;

    }
  };

  class ConfigurationChangeHook : public ccf::kv::ConsensusHook
  {
    ccf::kv::Configuration::Nodes
      new_configuration; // Absence of node means that node has been retired
    ccf::kv::Version version;

  public:
    ConfigurationChangeHook(
      ccf::kv::Configuration::Nodes new_configuration_,
      ccf::kv::Version version_) :
      new_configuration(new_configuration_),
      version(version_)
    {}

    void call(ccf::kv::ConfigurableConsensus* consensus) override
    {
      auto configuration = consensus->get_latest_configuration_unsafe();
      std::unordered_set<ccf::NodeId> retired_nodes;
      std::list<Configuration::Nodes::const_iterator> itrs;

      // Remove and track retired nodes
      for (auto it = configuration.begin(); it != configuration.end(); ++it)
      {
        if (new_configuration.find(it->first) == new_configuration.end())
        {
          retired_nodes.emplace(it->first);
          itrs.push_back(it);
        }
      }
      for (auto it : itrs)
      {
        configuration.erase(it);
      }

      // Add new node to configuration
      for (const auto& [node_id, _] : new_configuration)
      {
        configuration[node_id] = {};
      }

      consensus->add_configuration(version, configuration, {}, retired_nodes);
    }
  };

  using RCHook =
    std::function<void(Index, const std::vector<ccf::kv::NodeId>&)>;

  class LoggingStubStore
  {
  protected:
    ccf::NodeId _id;
    RCHook set_retired_committed_hook;

  protected:
    std::mutex kvstore_access;
    std::map<std::string, std::vector<uint8_t>>
      kvstore; // key: fs_id.commitment_type, value: data
    using filesystem_id = int;
    using emphemeral_attestation_id = int;
    using commitment_store =
      std::map<Index, std::vector<uint8_t>>; // key: raft log index, value:
                                             // commitment
    std::map<filesystem_id, commitment_store>
      cmt_tail_store; // key: fs_id, value: commitment_store
    std::map<filesystem_id, commitment_store>
      cmt_ub_store; // key: fs_id, value: commitment_store
    std::map<filesystem_id, emphemeral_attestation_id>
      attestation_store; // key: fs_id, value: attestation_id

  public:
    LoggingStubStore(ccf::NodeId id) : _id(id) {}

    virtual void set_set_retired_committed_hook(
      RCHook set_retired_committed_hook_)
    {
      set_retired_committed_hook = set_retired_committed_hook_;
    }

    std::optional<std::vector<uint8_t>> get_entry_by_idx(size_t idx)
    {
      std::lock_guard<std::mutex> lock(kvstore_access);
      for (auto it = cmt_tail_store.begin(); it != cmt_tail_store.end(); ++it)
      {
        auto& [fs_id, commitment_store] = *it;
        auto entry_it = commitment_store.find(idx);
        if (entry_it != commitment_store.end())
        {
          return entry_it->second;
        }
      }
      for (auto it = cmt_ub_store.begin(); it != cmt_ub_store.end(); ++it)
      {
        auto& [fs_id, commitment_store] = *it;
        auto entry_it = commitment_store.find(idx);
        if (entry_it != commitment_store.end())
        {
          return entry_it->second;
        }
      }
      return std::nullopt;
    }

    virtual void compact(Index i)
    {
      std::lock_guard<std::mutex> lock(kvstore_access);
      fmt::print("{} --> compacting up to index={}\n", __PRETTY_FUNCTION__, i);
      for (auto it = cmt_tail_store.begin(); it != cmt_tail_store.end();)
      {
        auto& [fs_id, commitment_store] = *it;
        auto first_uncommitted_it = commitment_store.upper_bound(i);
        auto last_committed_it = std::prev(first_uncommitted_it);

        if (last_committed_it != commitment_store.end())
        {
          fmt::print(
            "{} [TAIL] fs_id={} last_committed_index={} "
            "first_uncommitted_index={} (=0 for if there are no uncommitted "
            "entries)\n",
            __PRETTY_FUNCTION__,
            fs_id,
            last_committed_it->first,
            first_uncommitted_it != commitment_store.end() ?
              first_uncommitted_it->first :
              0);
          commitment_store.erase(commitment_store.begin(), last_committed_it);
        }

        if (commitment_store.empty())
        {
          fmt::print(
            "{} --> cmt_tail_store: is empty after compacting up to index={}\n",
            __PRETTY_FUNCTION__,
            fs_id,
            i);
          it = cmt_tail_store.erase(it);
        }
        else
        {
          ++it;
        }
      }
      for (auto it = cmt_ub_store.begin(); it != cmt_ub_store.end();)
      {
        auto& [fs_id, commitment_store] = *it;
        auto first_uncommitted_it = commitment_store.upper_bound(i);
        auto last_committed_it = std::prev(first_uncommitted_it);
        auto second_to_last_committed_it = std::prev(last_committed_it);

        if (
          last_committed_it != commitment_store.end() &&
          second_to_last_committed_it != commitment_store.end())
        {
          fmt::print(
            "{} [UB] fs_id={} last_committed_index={} "
            "second_to_last_committed_index={} first_uncommitted_index={} (=0 "
            "for if there are no uncommitted entries)\n",
            __PRETTY_FUNCTION__,
            fs_id,
            last_committed_it->first,
            second_to_last_committed_it->first,
            first_uncommitted_it != commitment_store.end() ?
              first_uncommitted_it->first :
              0);
          commitment_store.erase(
            commitment_store.begin(), second_to_last_committed_it);
        }
        else if (last_committed_it != commitment_store.end())
        {
          fmt::print(
            "{} [UB] fs_id={} last_committed_index={} "
            "first_uncommitted_index={} (=0 for if there are no uncommitted "
            "entries)\n",
            __PRETTY_FUNCTION__,
            fs_id,
            last_committed_it->first,
            first_uncommitted_it != commitment_store.end() ?
              first_uncommitted_it->first :
              0);
          commitment_store.erase(commitment_store.begin(), last_committed_it);
        }

        if (commitment_store.empty())
        {
          fmt::print(
            "{} --> cmt_ub_store: is empty after compacting up to index={}\n",
            __PRETTY_FUNCTION__,
            fs_id,
            i);
          it = cmt_ub_store.erase(it);
        }
        else
        {
          ++it;
        }
      }

      print_store(std::cout);
    }

    virtual void rollback(const ccf::kv::TxID& tx_id, Term t)
    {
      std::lock_guard<std::mutex> lock(kvstore_access);

      fmt::print(
        "{} --> rolling back to term={} index={}\n",
        __func__,
        tx_id.term,
        tx_id.version);
    }

    virtual void initialise_term(Term t) {}

    ccf::kv::Version current_version()
    {
      return ccf::kv::NoVersion;
    }

    void apply(const std::vector<uint8_t>& entry, ccf::kv::Version index)
    {
      aft::ReplicatedData r =
        nlohmann::json::parse(std::span{entry.data(), entry.size()});
      if (r.type == aft::ReplicatedDataType::raw)
      {
        auto [cmt_type, fs_id, attestation_id] =
          deserialize_data_and_print(__func__, r.data.data(), r.data.size());
        std::lock_guard<std::mutex> lock(kvstore_access);
        if (attestation_store.find(fs_id) == attestation_store.end())
        {
          attestation_store[fs_id] = attestation_id;
          fmt::print(
            "{} --> new fs_id={} with attestation_id={}\n",
            __PRETTY_FUNCTION__,
            fs_id,
            attestation_id);
        }
        if (attestation_store[fs_id] != attestation_id)
        {
          fmt::print(
            "{} --> WARNING -- OWNERSHIP TRANSFER: attestation_id={} for "
            "fs_id={} does not match "
            "previously stored attestation_id={} for the same fs_id\n",
            __PRETTY_FUNCTION__,
            attestation_id,
            fs_id,
            attestation_store[fs_id]);
        }
        if (cmt_type == (int)block_type::TAIL)
        {
          cmt_tail_store[fs_id][index] = std::vector<uint8_t>(
            entry.begin(),
            entry.end()); // Using 0 as the filesystem_id for simplicity
        }
        else if (cmt_type == (int)block_type::UB)
        {
          cmt_ub_store[fs_id][index] = std::vector<uint8_t>(
            entry.begin(),
            entry.end()); // Using 0 as the filesystem_id for simplicity
        }
        std::cout << "Current state of the store after applying entry:\n";
        print_store(std::cout) << std::endl;
      }
    }

    std::ostream& print_store(std::ostream& os) const
    {
      os << "====== cmt_tail_store ======\n";
      for (const auto& [fs_id, commitment_store] : cmt_tail_store)
      {
        os << "Filesystem " << fs_id << ":\n";
        for (const auto& [index, commitment] : commitment_store)
        {
          ReplicatedData r = nlohmann::json::parse(
            std::span{commitment.data(), commitment.size()});
          os << "  Index: " << index << ": ";
          deserialize_data_and_print(__func__, r.data.data(), r.data.size());
        }
      }
      os << "====== cmt_ub_store ======\n";
      for (const auto& [fs_id, commitment_store] : cmt_ub_store)
      {
        os << "Filesystem " << fs_id << ":\n";
        for (const auto& [index, commitment] : commitment_store)
        {
          ReplicatedData r = nlohmann::json::parse(
            std::span{commitment.data(), commitment.size()});
          os << "  Index: " << index << ": ";
          deserialize_data_and_print(__func__, r.data.data(), r.data.size());
        }
      }
      return os;
    }

    class ExecutionWrapper : public ccf::kv::AbstractExecutionWrapper
    {
    private:
      LoggingStubStore* stub_store_ptr; // key: fs_id, value: commitment_store
      ccf::kv::ConsensusHookPtrs hooks;
      aft::Term term;
      ccf::kv::Version index;
      std::vector<uint8_t> entry;
      ccf::ClaimsDigest claims_digest;
      std::optional<ccf::crypto::Sha256Hash> commit_evidence_digest =
        std::nullopt;
      ccf::kv::ApplyResult result;

    public:
      ExecutionWrapper(
        const std::vector<uint8_t>& data_,
        const std::optional<ccf::kv::TxID>& expected_txid,
        ccf::kv::ConsensusHookPtrs&& hooks_,
        LoggingStubStore* stub_store_ptr_) :
        hooks(std::move(hooks_)),
        stub_store_ptr(stub_store_ptr_)
      {
        fmt::print(
          "{}: deserialising entry of size {}\n", __func__, data_.size());
        const uint8_t* data = data_.data();
        auto size = data_.size();

        const auto committable = serialized::read<bool>(data, size);
        term = serialized::read<aft::Term>(data, size);
        index = serialized::read<ccf::kv::Version>(data, size);
        entry = serialized::read(data, size, size);

        fmt::print(
          "{}: deserialized entry with committable={}, term={}, index={}, "
          "entry_size={}\n",
          __func__,
          committable,
          term,
          index,
          entry.size());
        result = committable ? ccf::kv::ApplyResult::PASS_SIGNATURE :
                               ccf::kv::ApplyResult::PASS;

        if (expected_txid.has_value())
        {
          if (term != expected_txid->term || index != expected_txid->version)
          {
            result = ccf::kv::ApplyResult::FAIL;
          }
        }
      }

      ccf::ClaimsDigest&& consume_claims_digest() override
      {
        return std::move(claims_digest);
      }

      std::optional<ccf::crypto::Sha256Hash>&& consume_commit_evidence_digest()
        override
      {
        return std::move(commit_evidence_digest);
      }

      ccf::kv::ApplyResult apply(bool track_deletes_on_missing_keys) override
      {
        stub_store_ptr->apply(entry, index);
        return result;
      }

      ccf::kv::ConsensusHookPtrs& get_hooks() override
      {
        return hooks;
      }

      const std::vector<uint8_t>& get_entry() override
      {
        return entry;
      }

      Term get_term() override
      {
        return term;
      }

      ccf::kv::Version get_index() override
      {
        return index;
      }

      bool support_async_execution() override
      {
        return false;
      }

      bool is_public_only() override
      {
        return false;
      }

      bool should_rollback_to_last_committed() override
      {
        return false;
      }
    };

    virtual std::unique_ptr<ccf::kv::AbstractExecutionWrapper> deserialize(
      const std::vector<uint8_t>& data,
      bool public_only = false,
      const std::optional<ccf::kv::TxID>& expected_txid = std::nullopt)
    {
      fmt::print("{}: deserialising entry of size {}\n", __func__, data.size());
      ccf::kv::ConsensusHookPtrs hooks = {};
      return std::make_unique<ExecutionWrapper>(
        data, expected_txid, std::move(hooks), this);
    }

    bool flag_enabled(ccf::kv::AbstractStore::StoreFlag)
    {
      return false;
    }

    void unset_flag(ccf::kv::AbstractStore::StoreFlag) {}
  };

  class LoggingStubStoreConfig : public LoggingStubStore
  {
  public:
    std::vector<std::pair<Index, nlohmann::json>> retired_committed_entries =
      {};

    LoggingStubStoreConfig(ccf::NodeId id) : LoggingStubStore(id) {}

    // compact and rollback emulate the behaviour of the retired_committed hook
    // in the real store through the retired_committed_entries vector, see
    // node_state.h, circa line 2147
    virtual void compact(Index i) override
    {
      fmt::print("{} --> compacting up to index={}\n", __func__, i);
      for (auto& [version, configuration] : retired_committed_entries)
      {
        if (version <= i)
        {
          std::vector<ccf::kv::NodeId> retired_committed_node_ids;
          for (auto& [node_id, _] : configuration.items())
          {
            retired_committed_node_ids.push_back(node_id);
          }
          set_retired_committed_hook(i, retired_committed_node_ids);
        }
        else
        {
          break;
        }
      }
      retired_committed_entries.erase(
        std::remove_if(
          retired_committed_entries.begin(),
          retired_committed_entries.end(),
          [i](const auto& entry) { return entry.first < i; }),
        retired_committed_entries.end());
      LoggingStubStore::compact(i);
    }

    virtual void rollback(const ccf::kv::TxID& tx_id, Term t) override
    {
      retired_committed_entries.erase(
        std::remove_if(
          retired_committed_entries.begin(),
          retired_committed_entries.end(),
          [tx_id](const auto& entry) { return entry.first > tx_id.version; }),
        retired_committed_entries.end());
    }

    std::string stringify(
      const std::vector<uint8_t>& v, size_t max_size = 100ul)
    {
      auto size = std::min(v.size(), max_size);
      return fmt::format(
        "[{} bytes] {}", v.size(), std::string(v.begin(), v.begin() + size));
    }

    virtual std::unique_ptr<ccf::kv::AbstractExecutionWrapper> deserialize(
      const std::vector<uint8_t>& data,
      bool public_only = false,
      const std::optional<ccf::kv::TxID>& expected_txid = std::nullopt) override
    {
      // Set reconfiguration hook if there are any new nodes
      // Read wrapping term and version
      auto data_ = data.data();
      auto size = data.size();

      fmt::print("{}: deserialising entry of size {}\n", __func__, data.size());
      const auto committable = serialized::read<bool>(data_, size);
      fmt::print("{}: deserialized committable={}\n", __func__, committable);
      auto term = serialized::read<aft::Term>(data_, size);
      fmt::print("{}: deserialized term={}\n", __func__, term);
      auto version = serialized::read<ccf::kv::Version>(data_, size);
      fmt::print("{}: deserialized version={}\n", __func__, version);
      ReplicatedData r = nlohmann::json::parse(std::span{data_, size});

      ccf::kv::ConsensusHookPtrs hooks = {};
      if (r.type == ReplicatedDataType::reconfiguration)
      {
        ccf::kv::Configuration::Nodes configuration =
          nlohmann::json::parse(r.data);
        auto hook = std::make_unique<aft::ConfigurationChangeHook>(
          configuration, version);
        hooks.push_back(std::move(hook));
      }
      if (r.type == ReplicatedDataType::retired_committed)
      {
        ccf::kv::Configuration::Nodes configuration =
          nlohmann::json::parse(r.data);
        retired_committed_entries.emplace_back(version, configuration);
      }

      return std::make_unique<ExecutionWrapper>(
        data, expected_txid, std::move(hooks), this);
    }
  };

  class StubSnapshotter
  {
  public:
    void update(Index, bool) {}

    void set_last_snapshot_idx(Index idx) {}

    void commit(Index, bool) {}

    void rollback(Index) {}

    void record_serialised_tree(Index version, const std::vector<uint8_t>& tree)
    {}

    void record_signature(
      Index,
      const std::vector<uint8_t>&,
      const ccf::NodeId&,
      const ccf::crypto::Pem&)
    {}
  };
}