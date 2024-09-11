/**
 * @file streamOpMonFacility.cpp kafkaopmon class implementation
 *
 * This is part of the DUNE DAQ software, copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#include "streamOpMonFacility.hpp"

using json = nlohmann::json;

namespace dunedaq::kafkaopmon { // namespace dunedaq

  streamOpMonFacility::streamOpMonFacility(std::string uri, dunedaq::opmonlib::OptionalOrigin o)
    : dunedaq::opmonlib::OpMonFacility(uri) {
    
    std::regex uri_re(R"(([a-zA-Z]+):\/\/([^:\/?#\s]+):(\d+)\/([^:\/?#\s]+))");
    //* 1st Capturing Group `([a-zA-Z])`: Matches protocol
    //* 2nd Capturing Group `([^:\/?#\s])+`: Matches hostname
    //* 3rd Capturing Group `(\d)`: Matches port
    //* 4th Capturing Group `([^\/?#]+)?`: Matches kafka topic

    std::smatch uri_match;
    if (!std::regex_match(uri, uri_match, uri_re)) {
      throw WrongURI(ERS_HERE, uri);
    }

    json config;
    std::string bootstrap = uri_match[2];
    bootstrap += ':' ;
    bootstrap += uri_match[3];
    config["bootstrap"] = bootstrap;

    // optionally set the ID of the application
    // But really this is temporary, and in the future we should avoid env variables
    if ( o ) {
      config["cliend_id"] = dunedaq::opmonlib::to_string( o.value() ) ;
    }

    config["default_topic"] = uri_match[4];

    m_publisher.reset( new OpMonPublisher(config) );
  }
  
} // namespace dunedaq::kafkaopmon


DEFINE_DUNE_OPMON_FACILITY(dunedaq::kafkaopmon::streamOpMonFacility)

