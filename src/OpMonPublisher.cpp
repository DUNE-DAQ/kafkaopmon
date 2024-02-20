/**
 * @file OpMonPublisher.cpp OpMonPublisher Class Implementation
 *  
 * This is part of the DUNE DAQ Software Suite, copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */


#include "kafkaopmon/OpMonPublisher.hpp"

using namespace dunedaq::kafkaopmon;

OpMonPublisher::OpMonPublisher( const nlohmann::json& conf) {

  RdKafka::Conf * k_conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
  std::string errstr;

  // Good observations here https://www.confluent.io/blog/modern-cpp-kafka-api-for-safe-easy-messaging/
  
  // auto it = conf.find("bootstrap");
  // if ( it == conf.end() ) {
  //   std::cerr << "Missing bootstrap from json file";
  //   throw std::runtime_error( "Missing bootstrap from json file" );
  // }
  
  // k_conf->set("bootstrap.servers", *it, errstr);
  // if(errstr != ""){
  //   throw std::runtime_error( errstr );
  // }
  
  // std::string client_id;
  // it = conf.find( "cliend_id" );
  // if ( it != conf.end() )
  //   client_id = *it;
  // else if(const char* env_p = std::getenv("DUNEDAQ_APPLICATION_NAME")) 
  //   client_id = env_p;
  // else
  //   client_id = "erskafkaproducerdefault";
  
  // k_conf->set("client.id", client_id, errstr);    
  // if(errstr != ""){
  //   throw std::runtime_error( errstr );
  // }
  
  // //Create producer instance
  // m_producer.reset(RdKafka::Producer::create(k_conf, errstr));
  
  // if(errstr != ""){
  //   throw std::runtime_error( errstr );
  // }
  
  // it = conf.find("default_topic");
  // if (it != conf.end()) m_default_topic = *it;

}


bool OpMonPublisher::publish( dunedaq::opmon::OpMonEntry && entry ) {

  std::string binary;
  entry.SerializeToString( & binary );

  auto topic = extract_topic( entry );
  auto key   = extractkey( entry );

  

  
}
