/**
 * @brief test application for the opmon publisher
 */

#include <chrono>
#include <thread>
#include <future>        

#include <kafkaopmon/OpMonPublisher.hpp>
#include "opmonlib/Utils.hpp"
#include "opmonlib/info/test.pb.h"

#include <boost/program_options.hpp>

using nlohmann::json;
namespace po = boost::program_options;

using namespace dunedaq::kafkaopmon;
using namespace dunedaq::opmonlib;





int
main(int argc, char const* argv[])
{
  po::options_description desc("Allowed options");
  desc.add_options()
    ("help,h", "produce help message")
    ("bootstrap", po::value<std::string>()->default_value("monkafka.cern.ch:30092"), "kafka bootstrap server")
    ("topic,t", po::value<std::string>(), "Optional specification of a topic" )
    ("n_threads,n", po::value<unsigned int>()->default_value(10), "Number of threads used for test") 
  ;

  po::variables_map input_map;
  po::store(po::parse_command_line(argc, argv, desc), input_map);
  po::notify(input_map);
  
  if ( input_map.count("help") ) {
    std::cout << desc << std::endl;
    return 0;
  }

  json conf;
  conf["bootstrap"] = input_map["bootstrap"].as<std::string>();
  conf["cliend_id"] = "opmon_publisher_test";
  if ( input_map.count("topic") ) {
    conf["default_topic"] =  input_map["topic"].as<std::string>() ;
  }

  OpMonPublisher p(conf);

  auto pub_func = [&](int i){
    auto opmon_id = "test.app.thread_" + std::to_string(i);
    for (auto j = 0; j < 20; ++j ) {
      dunedaq::opmon::TestInfo ti;
      ti.set_int_example( j*1000 + i );
      ti.set_string_example( "test" );
      auto e = to_entry( ti );
      e.set_opmon_id(opmon_id);
      p.publish( std::move(e) );
    }
  };

  auto n = input_map["n_threads"].as<unsigned int>() ;
  std::vector<std::future<void>> threads(n);
  
  for( auto i = 0 ; i < n; ++i ) {
    threads[i] = async(std::launch::async, pub_func, i);
  }

  for ( auto & t : threads ) {
    t.get();
  }
  
  std::this_thread::sleep_for(std::chrono::seconds(2));
  
  return 0;
}
