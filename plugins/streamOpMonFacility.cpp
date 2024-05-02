/**
 * @file streamOpMonFacility.cpp kafkaopmon class implementation
 *
 * This is part of the DUNE DAQ software, copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#include "opmonlib/OpMonFacility.hpp"
#include "kafkaopmon/OpMonPublisher.hpp"

#include <memory>
#include <nlohmann/json.hpp>
#include <regex>
#include <string>

using json = nlohmann::json;

namespace dunedaq { // namespace dunedaq

  ERS_DECLARE_ISSUE(kafkaopmon, WrongURI, "Incorrect URI: " << uri, ((std::string)uri))

} // namespace dunedaq

namespace dunedaq::kafkaopmon { // namespace dunedaq

class streamOpMonFacility : public dunedaq::opmonlib::OpMonFacility
{
  std::unique_ptr<OpMonPublisher> m_publisher;

public:
  explicit streamOpMonFacility(std::string uri)
    : dunedaq::opmonlib::OpMonFacility(uri)
  {
    
    std::regex uri_re(R"(([a-zA-Z]+):\/\/([^:\/?#\s]+):(\d+)\/([^:\/?#\s]+))");
    //* 1st Capturing Group `([a-zA-Z])`: Matches protocol
    //* 2nd Capturing Group `([^:\/?#\s])+`: Matches hostname
    //* 3rd Capturing Group `(\d)`: Matches port
    //* 4th Capturing Group `([^\/?#]+)?`: Matches kafka topic

    std::smatch uri_match;
    if (!std::regex_match(uri, uri_match, uri_re)) {
      ers::fatal(WrongURI(ERS_HERE, uri));
    }

    json config;
    std::string bootstrap = uri_match[2];
    bootstrap += ':' ;
    bootstrap += uri_match[3];
    config["bootstrap"] = bootstrap;

    // optionally set the ID of the application
    // But really this is temporary, and in the future we should avoid env variables
    if(auto env_p = std::getenv("DUNEDAQ_PARTITION")) {
      if (auto app_p = std::getenv("DUNEDAQ_APPLICATION_NAME")) {
	config["cliend_id"] = std::string(env_p) + '.' + std::string(app_p);
      }
    }

    config["default_topic"] = uri_match[4];

    m_publisher.reset( new OpMonPublisher(config) );
  }

  void publish( opmon::OpMonEntry && e ) const override {
    m_publisher -> publish(std::move(e));
  }

};

} // namespace dunedaq::kafkaopmon


DEFINE_DUNE_OPMON_FACILITY(dunedaq::kafkaopmon::streamOpMonFacility)

