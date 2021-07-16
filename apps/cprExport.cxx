// * This is part of the DUNE DAQ Application Framework, copyright 2020.
// * Licensing/copyright details are in the COPYING file that you should have received with this code.

#include <ers/SampleIssues.hpp>
#include <ers/OutputStream.hpp>
#include <ers/StreamManager.hpp>

#include <ers/ers.hpp>
#include <iostream>
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

ERS_DECLARE_ISSUE(kafkaopmon, cannot_post_to_DB,
    "Cannot post to Influx DB " << error,
    ((std::string)error))

static volatile sig_atomic_t run = 1;

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
      ers::error(ers::InternalMessage(ERS_HERE, "%% Consumer error: " + msg->errstr()));
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
      ers::error(ers::InternalMessage(ERS_HERE, "Error [" + std::to_string(response.status_code) + "] making request"));
  } else if (response.status_code == 0) {
      ers::error(ers::InternalMessage(ERS_HERE, "Query returned 0"));
  } 
}

void consumerLoop(RdKafka::KafkaConsumer *consumer, int batch_size, int batch_tmout, std::string adress)
{
  while (run) 
  {

    auto msgs = consume_batch(consumer, batch_size, batch_tmout);
    for (auto &msg : msgs) 
    {
      
      char *message_text = (char *) msg->payload();
      //std::cout << message_text << std::endl;
      execution_command(adress, message_text);
      delete msg;
    }
  }
}

int main()
{
    //Broker parameters
    std::string broker = "188.185.122.48:9092";
    std::string topic = "opmonkafka-reporting";
    std::string db_host = "188.185.88.195";
    std::string db_path = "insert";
    std::string db_port = "80";
    std::string db_dbname = "db1";
    std::string topic_str;
    std::vector<std::string> topics;
    //Bulk consume parameters
    int batch_size = 100;
    int batch_tmout = 1000;
    //Kafka server settings
    std::string errstr;
    RdKafka::KafkaConsumer *consumer;
    try
    {     
      RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

      srand((unsigned) time(0));
      std::string groupId = "dunedqm-ErrorPlatform-group" + std::to_string(rand());

      conf->set("bootstrap.servers", broker, errstr);
      conf->set("client.id", "kafkaopmonprod", errstr);
      conf->set("group.id", groupId, errstr);
      
      topics.push_back(topic);
      consumer = RdKafka::KafkaConsumer::create(conf, errstr);
      if (consumer != 0)
      consumer->subscribe(topics);
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
