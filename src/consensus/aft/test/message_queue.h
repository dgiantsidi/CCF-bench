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
  m_queue()
  {
    fmt::print("{} \n", __func__);
  }

  ~m_queue()
  {
    fmt::print("{} \n", __func__);
  }

  m_queue(const m_queue& other)
  {
    fmt::print("{} \n", __func__);
  }

  m_queue(m_queue&& other)
  {
    fmt::print("{} \n", __func__);
  }

  void append(int node_id, std::unique_ptr<uint8_t[]> msg, size_t size)
  {
    std::lock_guard<std::mutex> tmp_lock(dq_mtx);
    auto ptr = std::make_unique<message>(node_id, std::move(msg), size);
    dq.push_back(std::move(ptr));
  }

  void append(std::unique_ptr<uint8_t[]> msg, size_t size)
  {
    exit(-1);
  }


  std::tuple<int, std::unique_ptr<uint8_t[]>, size_t> pop()
  {
    //fmt::print("{} \n", __PRETTY_FUNCTION__);

    std::lock_guard<std::mutex> tmp_lock(dq_mtx);
    if (dq.empty())
    {
      //fmt::print("{} --> no elem\n", __func__);
      return {-1, std::make_unique<uint8_t[]>(1), 0};
    }

    std::unique_ptr<message> front = std::move(dq.front());
    dq.pop_front();

    std::unique_ptr<uint8_t[]> ret_msg =
      std::make_unique<uint8_t[]>(front->msg_sz); // std::move(front->msg);

    size_t ret_sz = front->msg_sz;
    int node_id = front->node_id;

    return {node_id, std::move(ret_msg), ret_sz};
  }


private:
  struct message
  {
    std::unique_ptr<uint8_t[]> msg;
    size_t msg_sz;
    int node_id;

    explicit message(
      int _node_id, std::unique_ptr<uint8_t[]> _msg, size_t _msg_sz)
    {
      //fmt::print("{}\n", __PRETTY_FUNCTION__);

      node_id = _node_id;
      msg = std::move(_msg);
      msg_sz = _msg_sz;
    }



    message() = delete;

    message(const message& other)
    {
      fmt::print("{} \n", __PRETTY_FUNCTION__);
      node_id = other.node_id;
      msg_sz = other.msg_sz;
      msg = std::make_unique<uint8_t[]>(other.msg_sz);
      ::memcpy(msg.get(), other.msg.get(), msg_sz);
    }

    message(message&& other)
    {
      fmt::print("{} \n", __PRETTY_FUNCTION__);
      node_id = other.node_id;
      msg_sz = other.msg_sz;
      msg = std::make_unique<uint8_t[]>(other.msg_sz);
      ::memcpy(msg.get(), other.msg.get(), msg_sz);
    }

    message& operator=(const message& other)
    {
      if (this != &other)
      {
        fmt::print("{} \n", __PRETTY_FUNCTION__);
        node_id = other.node_id;
        msg_sz = other.msg_sz;
        msg = std::make_unique<uint8_t[]>(other.msg_sz);
        ::memcpy(msg.get(), other.msg.get(), msg_sz);
      }
      return *this;
    }

    message& operator=(message&& other)
    {
      fmt::print("{} \n", __PRETTY_FUNCTION__);
      node_id = other.node_id;
      msg_sz = other.msg_sz;
      msg = std::make_unique<uint8_t[]>(other.msg_sz);
      ::memcpy(msg.get(), other.msg.get(), msg_sz);
      return *this;
    }
  };

  std::deque<std::unique_ptr<message>> dq;
  mutable std::mutex dq_mtx;
};