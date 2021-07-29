// * This is part of the DUNE DAQ Application Framework, copyright 2020.
// * Licensing/copyright details are in the COPYING file that you should have received with this code.
#include "JsonInfluxConverter.hpp"

#include <ers/SampleIssues.hpp>
#include <ers/OutputStream.hpp>
#include <ers/StreamManager.hpp>
#include <regex.h>
#include <ers/ers.hpp>
#include <iostream>
#include <iomanip>
#include <string>
#include <cstdlib>
#include <cstdio>
#include <csignal>
#include <cstring>
#include <librdkafka/rdkafkacpp.h>
#include <sys/time.h>
#include <curl/curl.h>
#include <vector>
#include <cpr/cpr.h>
#include <nlohmann/json.hpp>
#include "boost/program_options.hpp"

namespace dunedaq { // namespace dunedaq

    ERS_DECLARE_ISSUE(kafkaopmon, CannotPostToDb,
        "Cannot post to Influx DB : " << error,
        ((std::string)error))

    ERS_DECLARE_ISSUE(kafkaopmon, CannotCreateConsumer,
        "Cannot create consumer : " << fatal,
        ((std::string)fatal))

    ERS_DECLARE_ISSUE(kafkaopmon, CannotConsumeMessage,
        "Cannot consume message : " << error,
        ((std::string)error))

    ERS_DECLARE_ISSUE(kafkaopmon, IncorrectParameters,
        "Incorrect parameters : " << fatal,
        ((std::string)fatal))
}

namespace bpo = boost::program_options;

static volatile sig_atomic_t run = 1;
static dunedaq::influxopmon::JsonConverter m_json_converter;
static std::vector<std::string> inserts_vectors;
static std::string m_query = "";


static int64_t now () {
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    return ((int64_t)tv.tv_sec * 1000) + (tv.tv_usec / 1000);
}

static std::vector<RdKafka::Message *>
consume_batch (RdKafka::KafkaConsumer *consumer, size_t batch_size, int batch_tmout) {

  std::vector<RdKafka::Message *> msgs;
  msgs.reserve(batch_size);

  int64_t end = now() + batch_tmout;
  int remaining_timeout = batch_tmout;

  while (msgs.size() < batch_size) {
    RdKafka::Message *msg = consumer->consume(remaining_timeout);

    switch (msg->err()) {
    case RdKafka::ERR__TIMED_OUT:
      delete msg;
      return msgs;

    case RdKafka::ERR_NO_ERROR:
      msgs.push_back(msg);
      break;

    default:
      ers::error(dunedaq::kafkaopmon::CannotConsumeMessage(ERS_HERE, "%% Unhandled consumer error: " + msg->errstr()));
      run = 0;
      delete msg;
      return msgs;
    }

    remaining_timeout = end - now();
    if (remaining_timeout < 0)
      break;
  }

  return msgs;
}



void execution_command(const std::string& adress, const std::string& cmd) {

  //std::cout << adress << std::endl;
  cpr::Response response = cpr::Post(cpr::Url{adress}, cpr::Body{cmd});
  //std::cout << cmd << std::endl;  
  if (response.status_code >= 400) {
      ers::error(dunedaq::kafkaopmon::CannotPostToDb(ERS_HERE, "Error [" + std::to_string(response.status_code) + "] making request"));
  } else if (response.status_code == 0) {
      ers::error(dunedaq::kafkaopmon::CannotPostToDb(ERS_HERE, "Query returned 0"));
  } 
}

void consumerLoop(RdKafka::KafkaConsumer *consumer, int batch_size, int batch_tmout, std::string adress)
{
  while (run) 
  {

    auto msgs = consume_batch(consumer, batch_size, batch_tmout);
    for (auto &msg : msgs) 
    {

      //execution_command(adress, message_text);
      std::string json_string((char *) msg->payload(), msg->len());

      m_json_converter.set_inserts_vector(json::parse(json_string));
      inserts_vectors = m_json_converter.get_inserts_vector();  
      
      for (const auto& insert : inserts_vectors ) {
          m_query = m_query + insert + "\n" ;
      }

      delete msg;
    }
    if(m_query!="")
    {
      execution_command(adress, m_query);
      m_query = "";
    }

  }
}

int main(int argc, char *argv[])
{
    std::string broker;
    std::string topic;
    std::string db_host;
    std::string db_port;
    std::string db_path;
    std::string db_dbname;
    std::string topic_str;
    std::vector<std::string> topics;    
    //Bulk consume parameters
    int batch_size = 100;
    int batch_tmout = 1000;
    //Kafka server settings
    std::string errstr;
    RdKafka::KafkaConsumer *consumer;
    //get parameters
    


    bpo::options_description desc{"example: -broker 188.185.122.48:9092 -topic kafkaopmon-reporting -dbhost 188.185.88.195 -dbport 80 -dbpath insert -dbname db1"};
    desc.add_options()
      ("help,h", "Help screen")
      ("broker,b", bpo::value<std::string>()->default_value("188.185.122.48:9092"), "Broker")
      ("topic,t", bpo::value<std::string>()->default_value("kafkaopmon-reporting"), "Topic")
      ("dbhost,ho", bpo::value<std::string>()->default_value("188.185.88.195"), "Database host")
      ("dbport,po", bpo::value<std::string>()->default_value("80"), "Database port")
      ("dbpath,pa", bpo::value<std::string>()->default_value("insert"), "Database path")
      ("dbname,n", bpo::value<std::string>()->default_value("db1"), "Database name");

    bpo::variables_map vm;
      
    try 
    {
      auto parsed = bpo::command_line_parser(argc, argv).options(desc).run();
      bpo::store(parsed, vm);
    }
    catch (bpo::error const& e) 
    {
      ers::error(dunedaq::kafkaopmon::IncorrectParameters(ERS_HERE, e.what()));
    }

    if (vm.count("help")) {
      std::cout << desc << std::endl;
      return 0;
    }
    
    broker = vm["broker"].as<std::string>();     
    topic = vm["topic"].as<std::string>();
    db_host = vm["dbhost"].as<std::string>();
    db_port = vm["dbport"].as<std::string>();
    db_path = vm["dbpath"].as<std::string>();
    db_dbname = vm["dbname"].as<std::string>();

    //Broker parameters

    try
    {     
      RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

      srand((unsigned) time(0));
      std::string group_id = "dunedqm-ErrorPlatform-group" + std::to_string(rand());

      conf->set("bootstrap.servers", broker, errstr);
      if(errstr != ""){
        ers::error(dunedaq::kafkaopmon::CannotCreateConsumer(ERS_HERE, errstr));
      }
      conf->set("client.id", "kafkaopmonproducer", errstr);
      if(errstr != ""){
        ers::error(dunedaq::kafkaopmon::CannotCreateConsumer(ERS_HERE, errstr));
      }
      conf->set("group.id", group_id, errstr);
      if(errstr != ""){
        ers::error(dunedaq::kafkaopmon::CannotCreateConsumer(ERS_HERE, errstr));
      }
      topics.push_back(topic);
      consumer = RdKafka::KafkaConsumer::create(conf, errstr);
      if(errstr != ""){
        ers::error(dunedaq::kafkaopmon::CannotCreateConsumer(ERS_HERE, errstr));
      }
      if (consumer != 0) consumer->subscribe(topics);
      else std::cout << errstr << std::endl;

      consumerLoop(consumer, batch_size, batch_tmout, db_host + ":" + db_port + "/" + db_path + "?db=" + db_dbname);
      // Close and destroy consumer 
      consumer->close();
      delete consumer;
    }
    catch( ers::IssueCatcherAlreadySet & ex )
    {
      ers::error( ex );
    }
  
    return 0;
}
