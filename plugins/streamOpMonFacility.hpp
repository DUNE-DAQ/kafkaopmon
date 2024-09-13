#ifndef KAFKAOPMON_PLUGIN_STREAMOPMONFACILITY_HPP_
#define KAFKAOPMON_PLUGIN_STREAMOPMONFACILITY_HPP_

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
  explicit streamOpMonFacility(std::string uri, dunedaq::opmonlib::OptionalOrigin );
  void publish( opmon::OpMonEntry && e ) const override {
    m_publisher -> publish(std::move(e));
  }

};

} // namespace dunedaq::kafkaopmon

#endif // KAFKAOPMON_PLUGIN_STREAMOPMONFACILITY_HPP_

