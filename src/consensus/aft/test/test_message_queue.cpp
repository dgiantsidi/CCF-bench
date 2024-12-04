#include "message_queue.h"

#include <thread>
#include <vector>
std::shared_ptr<m_queue> Q;

// static void run_func_append(std::shared_ptr<m_queue> q) {
static void run_func_append()
{
  fmt::print("{}\n", __func__);

  for (auto i = 0ULL; i < 10000; i++)
  {
    size_t msg_sz = 10;
    auto ptr = std::make_unique<uint8_t[]>(msg_sz);
    fmt::print("{} i = {}\n", __func__, i);

    Q->append(std::move(ptr), msg_sz);
  }
  fmt::print("{}\n", __func__);
  return;
}

// static void run_func_pop(std::shared_ptr<m_queue> q) {
static void run_func_pop()
{
  int count = 0;
  for (;;)
  {
    auto [n_id, data, data_sz] = Q->pop();
    if (data_sz > 0)
    {
      count++;
    }
    if (count == 10e6)
      return;
  }
}

int main()
{
  std::vector<std::thread> vec_threads;
  Q = std::shared_ptr<m_queue>();
  // std::shared_ptr<m_queue> q = std::make_shared<m_queue>();
  // run_func_append();
#if 1
  for (auto i = 0ULL; i < 1; i++)
  {
    vec_threads.emplace_back(std::thread(run_func_append));
  }
#endif
  vec_threads[0].join();
  // vec_threads.emplace_back(std::thread(run_func_pop, q));
  // vec_threads[1].join();

  return 0;
}