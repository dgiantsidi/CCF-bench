// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache 2.0 License.
#pragma once

#include "ccf/entity_id.h"
#include "consensus/aft/raft.h"
#include "consensus/aft/raft_types.h"

#include <map>
#include <optional>
#include <vector>

namespace aft
{
  class LedgerStubProxy
  {
  protected:
    ccf::NodeId _id;

    std::mutex ledger_access;

  public:
    std::vector<std::vector<uint8_t>> ledger;
    uint64_t skip_count = 0;

    LedgerStubProxy(const ccf::NodeId& id) : _id(id) {}

    virtual void init(Index, Index) {}

    virtual void put_entry(
      const std::vector<uint8_t>& original,
      bool globally_committable,
      ccf::kv::Term term,
      ccf::kv::Version index)
    {
      fmt::print(
        "{} ---> globally_committable={}, term={}, index={}\n",
        __func__,
        globally_committable,
        term,
        index);
      std::lock_guard<std::mutex> lock(ledger_access);

      // The payload that we eventually deserialise must include the
      // ledger entry as well as the View and Index that identify it, and
      // whether this is committable. In the real entries, they are nested in
      // the payload and the IV. For test purposes, we just prefix them manually
      // (to mirror the deserialisation in LoggingStubStore::ExecutionWrapper).
      // We also size-prefix, so in a buffer of multiple of these messages we
      // can extract each with get_entry
      const size_t idx = ledger.size() + 1;
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
      fmt::print(
        "{} ---> globally_committable={}, term={}, index={}, "
        "combined_size={}\n",
        __func__,
        globally_committable,
        term,
        index,
        combined.size());
      ledger.push_back(combined);
    }

    void skip_entry(const uint8_t*& data, size_t& size)
    {
      get_entry(data, size);
      ++skip_count;
    }

    static std::vector<uint8_t> get_entry(const uint8_t*& data, size_t& size)
    {
#if 1
      fmt::print("{} --> size={}\n", __func__, size);
      const auto entry_size = serialized::read<size_t>(data, size);
      fmt::print("{} --> entry_size={}\n", __func__, entry_size);
      std::vector<uint8_t> entry(data, data + entry_size);
      serialized::skip(data, size, entry_size);
      fmt::print("{} ---> data.size() should be 0={}\n", __func__, size);
#endif
      return entry;
    }

    std::optional<std::vector<uint8_t>> get_entry_by_idx(size_t idx)
    {
      std::lock_guard<std::mutex> lock(ledger_access);
      // Ledger indices are 1-based, hence the -1
      if (idx > 0 && idx <= ledger.size())
      {
        fmt::print(
          "{} -> idx={} entry_size={}\n",
          __func__,
          idx,
          ledger[idx - 1].size());
        return ledger[idx - 1];
      }

      return std::nullopt;
    }

    std::optional<std::vector<uint8_t>> get_raw_entry_by_idx(size_t idx)
    {
      auto data = get_entry_by_idx(idx);
      if (data.has_value())
      {
        // Remove the View and Index that were written during put_entry
        data->erase(
          data->begin(),
          data->begin() + sizeof(size_t) + sizeof(ccf::kv::Term) +
            sizeof(ccf::kv::Version));
      }

      return data;
    }

    std::optional<std::vector<uint8_t>> get_append_entries_payload(
      const aft::AppendEntries& ae)
    {
      std::vector<uint8_t> payload;

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

      return payload;
    }

    virtual void truncate(Index idx)
    {
      ledger.resize(idx);
    }

    void reset_skip_count()
    {
      skip_count = 0;
    }

    void commit(Index idx) {}
  };

  class ChannelStubProxy : public ccf::NodeToNode
  {
  public:
    // Capture what is being sent out
    // Using a deque so we can both pop from the front and shuffle
    using MessageList =
      std::deque<std::pair<ccf::NodeId, std::vector<uint8_t>>>;
    MessageList messages;

    ChannelStubProxy() {}

    size_t count_messages_with_type(RaftMsgType type)
    {
      size_t count = 0;
      for (const auto& [nid, m] : messages)
      {
        const uint8_t* data = m.data();
        size_t size = m.size();

        if (serialized::peek<RaftMsgType>(data, size) == type)
        {
          ++count;
        }
      }

      return count;
    }

    std::optional<std::vector<uint8_t>> pop_first(
      RaftMsgType type, ccf::NodeId target)
    {
      for (auto it = messages.begin(); it != messages.end(); ++it)
      {
        const auto [nid, m] = *it;
        const uint8_t* data = m.data();
        size_t size = m.size();

        if (serialized::peek<RaftMsgType>(data, size) == type)
        {
          if (target == nid)
          {
            messages.erase(it);
            return m;
          }
        }
      }

      return std::nullopt;
    }

    void associate_node_address(
      const ccf::NodeId& peer_id,
      const std::string& peer_hostname,
      const std::string& peer_service) override
    {}

    void close_channel(const ccf::NodeId& peer_id) override {}

    void set_endorsed_node_cert(const ccf::crypto::Pem&) override {}

    bool have_channel(const ccf::NodeId& nid) override
    {
      return true;
    }

    bool send_authenticated(
      const ccf::NodeId& to,
      ccf::NodeMsgType msg_type,
      const uint8_t* data,
      size_t size) override
    {
      fmt::print("[{}] to {}\n", __func__, to);
      std::vector<uint8_t> m(data, data + size);
      messages.emplace_back(to, std::move(m));
      return true;
    }

    bool recv_authenticated(
      const ccf::NodeId& from_node,
      std::span<const uint8_t> cb,
      const uint8_t*& data,
      size_t& size) override
    {
      fmt::print("[{}] from {}\n", __func__, from_node);
      return true;
    }

    bool recv_channel_message(
      const ccf::NodeId& from, const uint8_t* data, size_t size) override
    {
      fmt::print("[{}] from {}\n", __func__, from);

      return true;
    }

    void initialize(
      const ccf::NodeId& self_id,
      const ccf::crypto::Pem& service_cert,
      ccf::crypto::KeyPairPtr node_kp,
      const std::optional<ccf::crypto::Pem>& node_cert = std::nullopt) override
    {}

    bool send_encrypted(
      const ccf::NodeId& to,
      ccf::NodeMsgType msg_type,
      std::span<const uint8_t> cb,
      const std::vector<uint8_t>& data) override
    {
      fmt::print("[{}] to {}\n", __func__, to);

      return true;
    }

    std::vector<uint8_t> recv_encrypted(
      const ccf::NodeId& fromfpf32,
      std::span<const uint8_t> cb,
      const uint8_t* data,
      size_t size) override
    {
      fmt::print("[{}] from {}\n", __func__, fromfpf32);

      return {};
    }

    void set_message_limit(size_t message_limit) override {}
    void set_idle_timeout(std::chrono::milliseconds idle_timeout) override {}

    void tick(std::chrono::milliseconds elapsed) override {}

    bool recv_authenticated_with_load(
      const ccf::NodeId& from, const uint8_t*& data, size_t& size) override
    {
      fmt::print("[{}] from {}\n", __func__, from);

      return true;
    }
  };

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

  public:
    LoggingStubStore(ccf::NodeId id) : _id(id) {}

    virtual void set_set_retired_committed_hook(
      RCHook set_retired_committed_hook_)
    {
      set_retired_committed_hook = set_retired_committed_hook_;
    }

    virtual void compact(Index i) {}

    virtual void rollback(const ccf::kv::TxID& tx_id, Term t) {}

    virtual void initialise_term(Term t) {}

    ccf::kv::Version current_version()
    {
      return ccf::kv::NoVersion;
    }

    class ExecutionWrapper : public ccf::kv::AbstractExecutionWrapper
    {
    private:
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
        ccf::kv::ConsensusHookPtrs&& hooks_) :
        hooks(std::move(hooks_))
      {
        const uint8_t* data = data_.data();
        auto size = data_.size();

        const auto committable = serialized::read<bool>(data, size);
        term = serialized::read<aft::Term>(data, size);
        index = serialized::read<ccf::kv::Version>(data, size);
        entry = serialized::read(data, size, size);

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
      fmt::print(
        "{}-> data.size()={}, public_only={}\n",
        __PRETTY_FUNCTION__,
        data.size(),
        public_only);

      ccf::kv::ConsensusHookPtrs hooks = {};
      return std::make_unique<ExecutionWrapper>(
        data, expected_txid, std::move(hooks));
    }

    bool flag_enabled(ccf::kv::AbstractStore::Flag)
    {
      return false;
    }

    void unset_flag(ccf::kv::AbstractStore::Flag) {}
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
      fmt::print(
        "{} ->>> data.size()={}, public_only={}\n",
        __PRETTY_FUNCTION__,
        data.size(),
        public_only);

      fmt::print("{}->{}\n", __func__, stringify(data, data.size()));
      // Set reconfiguration hook if there are any new nodes
      // Read wrapping term and version
      auto data_ = data.data();
      auto size = data.size();
      auto r2 = nlohmann::json::parse(std::span{
        data_ + 17, size - 17}); // this is extra (@dimitra should be removed)
      fmt::print(
        "{} (data_+17)={} (size-17)={} r2.type={}\n",
        __func__,
        reinterpret_cast<uintptr_t>(data_ + 17),
        (size - 17),
        r2.dump(4));
      fmt::print("{}->{}\n", __func__, stringify(data, data.size()));

      const auto committable = serialized::read<bool>(data_, size);
      fmt::print(
        "{} ->>> committable={} size={}\n",
        __PRETTY_FUNCTION__,
        committable,
        size);
      serialized::read<aft::Term>(data_, size);
      fmt::print(
        "{} ->>> reading term size_left={}\n", __PRETTY_FUNCTION__, size);

      auto version = serialized::read<ccf::kv::Version>(data_, size);
      fmt::print(
        "{} ->>> version={} size_left={}\n",
        __PRETTY_FUNCTION__,
        version,
        size);

      fmt::print(
        "{} --> committable={}, kv_version={}, size={}\n",
        __func__,
        committable,
        version,
        size);
      ReplicatedData r = {
        .type =
          ReplicatedDataType::raw}; // nlohmann::json::parse(std::span{data_,
                                    // size});
      fmt::print(
        "{} (data_)={} (size)={}\n",
        __func__,
        reinterpret_cast<uintptr_t>(data_),
        (size));
      r = nlohmann::json::parse(std::span{data_, size});
      fmt::print("{} -> passed the dangerous point\n", __func__);

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

      fmt::print("{} -----\n\n\n\n\n", __func__);
      return std::make_unique<ExecutionWrapper>(
        data, expected_txid, std::move(hooks));
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