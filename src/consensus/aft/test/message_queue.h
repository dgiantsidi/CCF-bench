#pragma once
#include "consensus/aft/raft.h"

#include <deque>
#include <memory>
#include <mutex>
#include <tuple>

// thread-safe
class m_queue
{
public:
  void append(ccf::NodeId node_id, std::unique_ptr<uint8_t[]> msg, size_t size)
  {
    std::unique_lock<std::mutex> tmp_lock(dq_mtx);
    dq.push_back(std::make_unique<message>(node_id, std::move(msg), size));
  }

  std::tuple<ccf::NodeId, std::unique_ptr<uint8_t[]>, size_t> pop()
  {
    std::unique_lock<std::mutex> tmp_lock(dq_mtx);
    if (dq.empty())
      return {ccf::NodeId("-1"), std::make_unique<uint8_t[]>(1), 0};
    std::unique_ptr<message>& front = dq.front();
    std::unique_ptr<uint8_t[]> ret_msg = std::move(front->msg);
    size_t ret_sz = front->msg_sz;
    ccf::NodeId node_id = front->node_id.value();
    dq.pop_front();
    return {node_id, std::move(ret_msg), ret_sz};
  }

private:
  struct message
  {
    std::unique_ptr<uint8_t[]> msg;
    size_t msg_sz;
    ccf::NodeId node_id;
    explicit message(
      ccf::NodeId _node_id, std::unique_ptr<uint8_t[]> _msg, size_t _msg_sz)
    {
      node_id = _node_id;
      msg = std::move(_msg);
      msg_sz = _msg_sz;
    }
  };
  std::deque<std::unique_ptr<message>> dq;
  std::mutex dq_mtx;
};