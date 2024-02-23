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
#include <ers/ers.hpp>

#include <string>
#include <memory>

#include "opmonlib/opmon_entry.pb.h"

namespace dunedaq {

  ERS_DECLARE_ISSUE( kafkaopmon,
		     MissingParameter,
		     "No " << parameter << " in " << conf,
		     ((std::string)parameter)((std::string)conf)
		   )

  ERS_DECLARE_ISSUE( kafkaopmon,
		     FailedConfiguration,
		     "Invalid " << parameter << ", cause: " << reason,
		     ((std::string)parameter)((std::string)reason)
		   )

  ERS_DECLARE_ISSUE( kafkaopmon,
		     FailedProducerCreation,
		     "Failed creation of a Kafka producer, cause: " << reason,
		     ((std::string)reason)
		   )

  ERS_DECLARE_ISSUE( kafkaopmon,
		     FailedProduce,
		     "Failed produce of message with key " << key << ", cause: " << reason,
		     ((std::string)key)((std::string)reason)
		   )
  
  ERS_DECLARE_ISSUE( kafkaopmon,
                     TimeoutReachedWhileFlushing,
		     "Publisher destroyed before all messages were completed, timeout: " << timeout << " ms",
		     ((int)timeout)
                   )

  
} // dunedaq namespace




namespace dunedaq::kafkaopmon {

  class OpMonPublisher {

    OpMonPublisher( const nlohmann::json& conf );

    OpMonPublisher() = delete;
    OpMonPublisher( const OpMonPublisher & ) = delete;
    OpMonPublisher & operator = ( const OpMonPublisher & ) = delete;
    OpMonPublisher( OpMonPublisher && ) = delete;
    OpMonPublisher & operator = ( OpMonPublisher && ) = delete;

    ~OpMonPublisher();

    bool publish( dunedaq::opmon::OpMonEntry && ) noexcept ;

  protected:
    std::string extract_topic( const dunedaq::opmon::OpMonEntry & e) const noexcept {
      return e.opmon_id() + '/' + e.measurement() ;
    }
    std::string extract_key( const dunedaq::opmon::OpMonEntry & ) const noexcept {
      return m_default_topic;
    }

  private:
    std::unique_ptr<RdKafka::Producer> m_producer;
    std::string m_default_topic = "monitoring.opmon_stream";
    
  };
  
} // namespace dunedaq::kafkaopmon

#endif  //KAFKAOPMON_INCLUDE_KAFKAOPMON_OPMONPUBLISHER_HPP_
