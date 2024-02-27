/**
 * @brief test application for the opmon publisher
 */

#include <chrono>
#include <thread>

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

  
  
  for( auto i = 0 ; i < 50; ++i ) {
    dunedaq::opmon::TestInfo ti;
    ti.set_int_example( 10*i );
    ti.set_string_example( "test" );
    p.publish( to_entry( ti ) );

    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  
  return 0;
}
