#include "message_queue.h"

#include <chrono>
#include <thread>
#include <vector>

constexpr int total_reqs = 5e6;

// static void run_func_append(std::shared_ptr<m_queue> q) {
static void run_func_append(std::shared_ptr<m_queue> Q)
{
  fmt::print("{}\n", __func__);

  for (auto i = 0ULL; i < total_reqs; i++)
  {
    size_t msg_sz = 10;
    auto ptr = std::make_unique<uint8_t[]>(msg_sz);
    //fmt::print("{} i = {}\n", __func__, i);

    Q->append(0, std::move(ptr), msg_sz);
  }
  fmt::print("{}\n", __func__);
  return;
}

// static void run_func_pop(std::shared_ptr<m_queue> q) {
static void run_func_pop(std::shared_ptr<m_queue> Q)
{
  fmt::print("{}\n", __func__);

  int count = 0;
  for (;;)
  {
    auto [node_id, data, data_sz] = Q->pop();
    if (data_sz > 0)
    {
      count++;
      //fmt::print("{} ----> count={}\n", __func__, count);
    }
    
    if (count == total_reqs)
      return;
  }
}

int main()
{
  std::vector<std::thread> vec_threads;
  std::shared_ptr<m_queue> Q = std::make_shared<m_queue>();
  // std::shared_ptr<m_queue> q = std::make_shared<m_queue>();
  // run_func_append();
  std::this_thread::sleep_for(std::chrono::seconds(5));
#if 1
  for (auto i = 0ULL; i < 1; i++)
  {
    vec_threads.emplace_back(std::thread(run_func_append, Q));
  }
#endif
  vec_threads.emplace_back(std::thread(run_func_pop, Q));
  vec_threads[0].join();

  vec_threads[1].join();

  return 0;
}