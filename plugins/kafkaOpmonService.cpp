// * This is part of the DUNE DAQ Application Framework, copyright 2020.
// * Licensing/copyright details are in the COPYING file that you should have received with this code.

#include "JsonInfluxConverter.hpp"

#include "opmonlib/OpmonService.hpp"
#include <librdkafka/rdkafkacpp.h>
#include <nlohmann/json.hpp>
#include <string>
#include <iostream>
#include <iomanip>
#include <fstream>
#include <vector>
#include <sstream>
#include <cstdio>
#include <memory>
#include <stdexcept>
#include <array>
#include <curl/curl.h>
#include <regex>

using json = nlohmann::json;

namespace dunedaq { // namespace dunedaq

    ERS_DECLARE_ISSUE(kafkaopmon, cannot_produce,
        "Cannot produce to kafka " << error,
        ((std::string)error))

    ERS_DECLARE_ISSUE(kafkaopmon, wrong_URI,
        "Incorrect URI" << uri,
        ((std::string)uri))
} // namespace dunedaq


namespace dunedaq::kafkaopmon { // namespace dunedaq

    class kafkaOpmonService : public dunedaq::opmonlib::OpmonService
    {
        public:


        explicit kafkaOpmonService(std::string uri) : dunedaq::opmonlib::OpmonService(uri) 
        {

            //Regex rescription:
            //"([a-zA-Z]+):\/\/([^:\/?#\s]+)+(?::(\d+))?(\/[^?#\s]+)?(?:\?(?:db=([^?#\s]+)))"
            //* 1st Capturing Group `([a-zA-Z])`: Matches protocol
            //* 2nd Capturing Group `([^:\/?#\s])+`: Matches hostname
            //* 3rd Capturing Group `(\d)`: Matches port
            //* 4th Capturing Group `([^\/?#]+)?`: Matches kafka topic
            
            std::regex uri_re(R"(([a-zA-Z]+):\/\/([^:\/?#\s]+):(\d+)\/([^\/?#]+))");

            std::smatch uri_match;
            if (!std::regex_match(uri, uri_match, uri_re)) 
            {
                ers::fatal(wrong_URI(ERS_HERE, " Invalid URI syntax: " + uri));
            }

            m_host = uri_match[2];
            m_port = uri_match[3];
            m_topic = uri_match[4];
            //Kafka server settings
            std::string brokers = m_host + ":" + m_port;
            std::string errstr;


            RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
            conf->set("bootstrap.servers", brokers, errstr);
            conf->set("client.id", std::getenv("DUNEDAQ_APPLICATION_NAME"), errstr);
            //Create producer instance
            m_producer = RdKafka::Producer::create(conf, errstr);
        }

        void publish(nlohmann::json j) override
        {
	        try
            {
                // serialize it to BSON
                m_producer->produce(m_topic, RdKafka::Topic::PARTITION_UA, RdKafka::Producer::RK_MSG_COPY, const_cast<char *>(j.dump().c_str()), j.dump().size(), nullptr, 0, 0, nullptr, nullptr);
                m_producer->purge(10 * 1000);
                if (m_producer->outq_len() > 0)
                {
                    std::string s;
                    s.push_back(m_producer->outq_len());
                    ers::error(cannot_produce(ERS_HERE, "Error [" + s + "] message(s) were not delivered"));
                }

            }
            catch(const std::exception& e)
            {
                std::string s = e.what();
                ers::error(cannot_produce(ERS_HERE, "Error [" + s + "] message(s) were not delivered"));
            }
        }

        protected:
            typedef OpmonService inherited;

        private:

        RdKafka::Producer *m_producer;

        std::string m_host;
        std::string m_port;
        std::string m_topic;
        
        std::vector<std::string> m_inserts;
        
        std::string m_query;
        const char* m_char_pointer;

    };

} // namespace dunedaq::kafkaopmon

extern "C" {
    std::shared_ptr<dunedaq::opmonlib::OpmonService> make(std::string service) { // namespace dunedaq::kafkaopmon
        return std::shared_ptr<dunedaq::opmonlib::OpmonService>(new dunedaq::kafkaopmon::kafkaOpmonService(service));
    }
}

