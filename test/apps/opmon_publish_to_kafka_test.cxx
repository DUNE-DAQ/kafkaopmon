/**
 * @brief test application for the the whole chain from MonitorableObject to kafka
 */

#include <chrono>
#include <thread>
#include <future>        

#include "opmonlib/opmon/test.pb.h"

#include <boost/program_options.hpp>

#include "opmonlib/OpMonManager.hpp"

namespace po = boost::program_options;

using namespace dunedaq::opmonlib;


class TestObject : public MonitorableObject {

  public:
    using MonitorableObject::register_node;
    using MonitorableObject::publish;
    TestObject() : MonitorableObject() {;}
};



int
main(int argc, char const* argv[])
{
  po::options_description desc("Allowed options");
  desc.add_options()
    ("help,h", "produce help message")
    ("bootstrap", po::value<std::string>()->default_value("monkafka.cern.ch:30092"), "kafka bootstrap server")
    ("topic,t", po::value<std::string>()->default_value("opmon_stream"), "Optional specification of a topic" )
    ("n_threads,n", po::value<unsigned int>()->default_value(10), "Number of threads used for test") 
  ;

  po::variables_map input_map;
  po::store(po::parse_command_line(argc, argv, desc), input_map);
  po::notify(input_map);
  
  if ( input_map.count("help") ) {
    std::cout << desc << std::endl;
    return 0;
  }

  std::string uri = "stream://";
  uri += input_map["bootstrap"].as<std::string>();
  uri += '/';
  uri += input_map["topic"].as<std::string>();
  
  OpMonManager man( "test",
		    "application",
		    uri );

  const auto n = input_map["n_threads"].as<unsigned int>() ;
  std::vector<std::shared_ptr<TestObject>> objs(n);
  for ( size_t i = 0; i < n; ++i ) {
    auto p = objs[i] = std::make_shared<TestObject>();
    man.register_node( "element_" + std::to_string(i), p );
  }

  auto pub_func = [&](int i, std::shared_ptr<TestObject> p){
    
    for (auto j = 0; j < 20; ++j ) {
      dunedaq::opmon::TestInfo ti;
      ti.set_int_example( j*1000 + i );
      ti.set_string_example( "test" );
      p->publish( std::move(ti) );
    }
  };

  std::vector<std::future<void>> threads(n);
  
  for( size_t i = 0 ; i < n; ++i ) {
    threads[i] = async(std::launch::async, pub_func, i, objs[i]);
  }

  for ( auto & t : threads ) {
    t.get();
  }
  
  std::this_thread::sleep_for(std::chrono::seconds(2));
  
  return 0;
}
