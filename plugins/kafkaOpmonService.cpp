/**
* @file kafkaOpmonService.cpp kafkaopmon class implementation
* 
* This is part of the DUNE DAQ software, copyright 2020.
* Licensing/copyright details are in the COPYING file that you should have
* received with this code.
*/


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
#include <regex>

using json = nlohmann::json;

namespace dunedaq { // namespace dunedaq

    ERS_DECLARE_ISSUE(kafkaopmon, CannotProduce,
        "Cannot produce to kafka " << error,
        ((std::string)error))

    ERS_DECLARE_ISSUE(kafkaopmon, WrongURI,
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
            
            std::regex uri_re(R"(([a-zA-Z]+):\/\/([^:\/?#\s]+):(\d+))");

            std::smatch uri_match;
            if (!std::regex_match(uri, uri_match, uri_re)) 
            {
                ers::fatal(WrongURI(ERS_HERE, " Invalid URI syntax: " + uri));
            }

            m_host = uri_match[2];
            m_port = uri_match[3];
            m_topic = "kafkaopmon-reporting";
            //Kafka server settings
            std::string brokers = m_host + ":" + m_port;
            std::string errstr;

            RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
            conf->set("bootstrap.servers", brokers, errstr);
            if(errstr != ""){
                CannotProduce(ERS_HERE, "Bootstrap server error : " + errstr);
            }
            if(const char* env_p = std::getenv("DUNEDAQ_APPLICATION_NAME")) 
                conf->set("client.id", env_p, errstr);
            else
                conf->set("client.id", "erskafkaOpmonproducerdefault", errstr);

            if(errstr != ""){
                CannotProduce(ERS_HERE, "Producer configuration error : " + errstr);
            }
            //Create producer instance
            m_producer = RdKafka::Producer::create(conf, errstr);

            if(errstr != ""){
                CannotProduce(ERS_HERE, "Producer creation error : " + errstr);
            }
        }

        void publish(nlohmann::json j) override
        {
	        try
            {
                // serialize it to BSON
                RdKafka::ErrorCode err = m_producer->produce(m_topic, RdKafka::Topic::PARTITION_UA, RdKafka::Producer::RK_MSG_COPY, const_cast<char *>(j.dump().c_str()), j.dump().size(), nullptr, 0, 0, nullptr, nullptr);
                if (err != RdKafka::ERR_NO_ERROR) { CannotProduce(ERS_HERE, "% Failed to produce " + RdKafka::err2str(err));}
            }
            catch(const std::exception& e)
            {
                std::string s = e.what();
                ers::error(CannotProduce(ERS_HERE, "Error [" + s + "] message(s) were not delivered"));
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

