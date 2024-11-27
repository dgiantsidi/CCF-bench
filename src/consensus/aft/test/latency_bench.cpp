
#include "ccf/ds/logger.h"
#include "consensus/aft/raft.h"
#include "networking_api.h"
#include <cassert>
#include <chrono>
#include <fstream>
#include <iostream>
#include <regex>
#include <string>
#include <map>

int main(int argc, char* argv[])
{
    authentication::init();
  std::map<ccf::NodeId, network_stack::connectivity_description> my_connections;
  my_connections.insert(std::make_pair(
    ccf::NodeId("0"), network_stack::connectivity_description()));
  my_connections.insert(std::make_pair(
    ccf::NodeId("1"), network_stack::connectivity_description()));
  

  my_connections[ccf::NodeId("0")].nid = ccf::NodeId("0");
  my_connections[ccf::NodeId("0")].ip = "10.1.0.7";
  // my_connections[ccf::NodeId("0")].ip = "10.5.0.6"; // regural VM IP
  my_connections[ccf::NodeId("0")].base_listening_port = 1800;
  my_connections[ccf::NodeId("0")].base_sending_port = 1900;

  my_connections[ccf::NodeId("1")].nid = ccf::NodeId("1");
  my_connections[ccf::NodeId("1")].ip = "10.1.0.4";
  // my_connections[ccf::NodeId("1")].ip = "10.5.0.7"; // regural VM IP
  my_connections[ccf::NodeId("1")].base_listening_port = 2800;
  my_connections[ccf::NodeId("1")].base_sending_port = 2900;



  std::string node_id;
  std::cin >> node_id;
  std::shared_ptr<network_stack> net =
      std::make_shared<network_stack>();
  if (node_id == "0") {
    fmt::print("{} ---> server\n", __func__);
    net->associate_node_address(ccf::NodeId("0"), my_connections[ccf::NodeId("0")].ip, std::to_string(my_connections[ccf::NodeId("0")].base_listening_port));
    net->connect_to_peer(my_connections[ccf::NodeId("0")].ip, std::to_string(my_connections[ccf::NodeId("0")].base_listening_port),
      ccf::NodeId("1"), my_connections[ccf::NodeId("1")].ip, my_connections[ccf::NodeId("1")].base_listening_port);

    net->accept_connection(ccf::NodeId("0"));

    auto now = std::chrono::high_resolution_clock::now();
    fmt::print("{} ---> Starting time={}\n", __func__, now);
    auto ptr = std::make_unique<uint8_t[]>(16);
    socket_layer::send_to_socket(net->node_connections_map[ccf::NodeId("0")]->sending_handle, std::move(ptr), 16);
    net->close_channel(ccf::NodeId("0"));
  }
  else {
    fmt::print("{} ---> client\n", __func__);
    net->associate_node_address(ccf::NodeId("1"), my_connections[ccf::NodeId("1")].ip, std::to_string(my_connections[ccf::NodeId("1")].base_listening_port));
    net->accept_connection(ccf::NodeId("1"));

    net->connect_to_peer(
      my_connections[ccf::NodeId("1")].ip, std::to_string(my_connections[ccf::NodeId("1")].base_listening_port), ccf::NodeId("0"), my_connections[ccf::NodeId("0")].ip, my_connections[ccf::NodeId("0")].base_listening_port);
    fmt::print("{} ---> Starting time=1\n", __func__);
    auto [data, data_sz] = socket_layer::read_from_socket(net->node_connections_map[ccf::NodeId("1")]->listening_handle, 16);
    net->close_channel(ccf::NodeId("1"));

  }
}