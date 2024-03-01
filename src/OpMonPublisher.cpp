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

  // Good observations on threadsafety here
  // https://www.confluent.io/blog/modern-cpp-kafka-api-for-safe-easy-messaging/
  
  auto it = conf.find("bootstrap");
  if ( it == conf.end() ) {
    throw MissingParameter(ERS_HERE,
			   "bootstrap",
			   nlohmann::to_string(conf) );
  }
  
  k_conf->set("bootstrap.servers", *it, errstr);
  if( ! errstr.empty() ) {
    throw FailedConfiguration(ERS_HERE,
			      "bootstrap.servers",
			      errstr);
  }
  
  std::string client_id;
  it = conf.find( "cliend_id" );
  if ( it != conf.end() )
    client_id = *it;
  else
    client_id = "kafkaopmon_default_producer";
  
  k_conf->set("client.id", client_id, errstr);    
  if ( ! errstr.empty() ) {
    ers::error( FailedConfiguration(ERS_HERE, "client.id", errstr ) );
  }
  
  // Create producer instance
  m_producer.reset(RdKafka::Producer::create(k_conf, errstr));
  
  if( ! m_producer ){
    throw FailedProducerCreation(ERS_HERE, errstr);
  }
    
  it = conf.find("default_topic");
  if (it != conf.end()) m_default_topic = *it;
  
}


OpMonPublisher::~OpMonPublisher() {

  int timeout_ms = 500;
  RdKafka::ErrorCode err = m_producer -> flush( timeout_ms );

  if ( err == RdKafka::ERR__TIMED_OUT ) {
    ers::warning( TimeoutReachedWhileFlushing( ERS_HERE, timeout_ms ) );
  }
  
}


void OpMonPublisher::publish( dunedaq::opmon::OpMonEntry && entry ) const {

  std::string binary;
  entry.SerializeToString( & binary );

  auto topic = extract_topic( entry );
  auto key   = extract_key( entry );

  RdKafka::ErrorCode err = m_producer -> produce( topic,
						  RdKafka::Topic::PARTITION_UA,
						  RdKafka::Producer::RK_MSG_COPY,
						  const_cast<char *>(binary.c_str()), binary.size(),
						  key.c_str(), key.size(),
						  0,
						  nullptr
						  );

  if ( err == RdKafka::ERR_NO_ERROR ) return ;

  std::string err_cause;
  
  switch( err ) {
  case RdKafka::ERR__QUEUE_FULL :
    err_cause = "maximum number of outstanding messages reached";
    break;
  case RdKafka::ERR_MSG_SIZE_TOO_LARGE :
    err_cause = "message too large";
    break;
  case RdKafka::ERR__UNKNOWN_PARTITION :
    err_cause = "Unknown partition";
    break;
  case RdKafka::ERR__UNKNOWN_TOPIC :
    err_cause = "Unknown topic (" ;
    err_cause += topic;
    err_cause += ')';
    break;
  default:
    err_cause = "unknown";
    break;
  }

  throw FailedProduce(ERS_HERE, key, err_cause) ;

}
