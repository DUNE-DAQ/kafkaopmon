/**
 * @file OpMonPublisher.hpp
 *
 * This is the interface to broadcast OpMon entries object in our DAQ system
 *
 * This is part of the DUNE DAQ Application Framework, copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */


#ifndef KAFKAOPMON_INCLUDE_KAFKAOPMON_OPMONPUBLISHER_HPP_
#define KAFKAOPMON_INCLUDE_KAFKAOPMON_OPMONPUBLISHER_HPP_

#include <librdkafka/rdkafkacpp.h>
#include <nlohmann/json.hpp>

#include <string>
#include <memory>

#include "opmonlib/info/test.pb.h"

namespace dunedaq::kafkaopmon {

  class OpMonPublihser {

    OpMonPublihser( const nlohmann::json& conf );

    OpMonPublihser() = delete;
    OpMonPublihser( const OpMonPublihser & ) = delete;
    OpMonPublihser & operator = ( const OpMonPublihser & ) = delete;
    OpMonPublihser( OpMonPublihser && ) = delete;
    OpMonPublihser & operator = ( OpMonPublihser && ) = delete;

    ~OpMonPublihser() {;}

    bool publish( dunedaq::opmon::OpMonEntry && );

  protected:
    std::string topic( const dunedaq::opmon::OpMonEntry & ) const;
    std::string key( const dunedaq::opmon::OpMonEntry & ) const;

  private:
    std::unique_ptr<RdKafka::Producer> m_producer;
    std::string m_default_topic = "monitoring.opmon_stream";
    
  };
  
}

#endif  //KAFKAOPMON_INCLUDE_KAFKAOPMON_OPMONPUBLISHER_HPP_
