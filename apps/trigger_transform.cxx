// * This is part of the DUNE DAQ Application Framework, copyright 2020.
// * Licensing/copyright details are in the COPYING file that you should have received with this code.
#include <iostream>
#include <string>
#include <highfive/H5File.hpp>
#include <highfive/H5Object.hpp>
#include <librdkafka/rdkafkacpp.h>
#include "dataformats/TriggerRecord.hpp"
#include "dataformats/wib/WIBFrame.hpp"
#include <ers/StreamFactory.hpp>

namespace dunedaq { // namespace dunedaq

    ERS_DECLARE_ISSUE(triggertransform, CannotPostToDb,
        "Cannot post to Influx DB : " << error,
        ((std::string)error))

    ERS_DECLARE_ISSUE(triggertransform, CannotCreateConsumer,
        "Cannot create consumer : " << fatal,
        ((std::string)fatal))

    ERS_DECLARE_ISSUE(triggertransform, CannotConsumeMessage,
        "Cannot consume message : " << error,
        ((std::string)error))

    ERS_DECLARE_ISSUE(triggertransform, IncorrectParameters,
        "Incorrect parameters : " << fatal,
        ((std::string)fatal))
        
    ERS_DECLARE_ISSUE(kafkaraw, CannotProduce,
        "Cannot produce to kafka " << error,
        ((std::string)error))

    ERS_DECLARE_ISSUE(kafkaraw, WrongURI,
        "Incorrect URI" << uri,
        ((std::string)uri))
} // namespace dunedaq


  RdKafka::Producer *m_producer;
  std::string m_host;
  std::string m_port;
  std::string m_topic;


  void readDataset(std::string path_dataset, void* buff) {

  std::string tr_header = "TriggerRecordHeader";
  if (path_dataset.find(tr_header) != std::string::npos) {
    std::cout << "--- TR header dataset" << path_dataset << std::endl;
    dunedaq::dataformats::TriggerRecordHeader trh(buff);
    std::cout << "Run number: " << trh.get_run_number()
              << " Trigger number: " << trh.get_trigger_number()
              << " Requested fragments: " <<trh.get_num_requested_components() << std::endl;
    std::cout << "============================================================" << std::endl;
  }
  else {
    std::cout << "+++ Fragment dataset" << path_dataset << std::endl;
    dunedaq::dataformats::Fragment frag(buff, dunedaq::dataformats::Fragment::BufferAdoptionMode::kReadOnlyMode);
    // Here I can now look into the raw data
    // As an example, we print a couple of attributes of the Fragment header and then dump the first WIB frame.
    if(frag.get_fragment_type() == dunedaq::dataformats::FragmentType::kTPCData) {
      std::cout << "Fragment with Run number: " << frag.get_run_number() 
                << " Trigger number: " << frag.get_trigger_number() 
                << " GeoID: " << frag.get_element_id() << std::endl;

      // Get pointer to the first WIB frame
      auto wfptr = reinterpret_cast<dunedaq::dataformats::WIBFrame*>(frag.get_data());       
      size_t raw_data_packets = (frag.get_size() - sizeof(dunedaq::dataformats::FragmentHeader)) / sizeof(dunedaq::dataformats::WIBFrame);

        //Message to be sent by kafka
        std::string message_to_kafka;
        std::cout << "Fragment contains " << raw_data_packets << " WIB frames" << std::endl;
        for (size_t i=0; i < raw_data_packets; ++i) {
            message_to_kafka = std::to_string(frag.get_run_number()) + std::to_string(frag.get_trigger_number()) + std::to_string(frag.get_element_id().element_id) + "\\n";
            auto wf1ptr = reinterpret_cast<dunedaq::dataformats::WIBFrame*>(frag.get_data()+i*sizeof(dunedaq::dataformats::WIBFrame));

            message_to_kafka += std::to_string(i) + "\\n";

            for(int j = 0; j< 256; j++)
            {
              message_to_kafka += wfptr->get_channel(j) + " ";
              //std::cout << "Channel " << std::to_string(j) << " : " << wfptr->get_channel(j) << std::endl;
            }
            message_to_kafka += "\\n";

            std::cout << message_to_kafka << std::endl;

            /*
            // print first WIB header
            if (i==0) {
                  std::cout << "First WIB header:"<< *(wfptr->get_wib_header());
                  //std::cout << "Printout sampled timestamps in WIB headers: " ;
                  //std::cout << "Channels :"<< *(wfptr) << std::endl;
                  for(int j = 0; j< 256; j++)
                  {
                    std::cout << "Channel " << std::to_string(j) << " : " << wfptr->get_channel(j) << std::endl;
                  }
            }*/
            // printout timestamp every now and then, only as example of accessing data...
            //if(i%1000 == 0) std::cout << "Timestamp " << i << ": " << wf1ptr->get_timestamp() << " ";
      }
      //kafka_exporter(message, "dunedqm-incommingchannel2");
      try
      {
          // serialize it to BSON
          RdKafka::ErrorCode err = m_producer->produce(m_topic, RdKafka::Topic::PARTITION_UA, RdKafka::Producer::RK_MSG_COPY, const_cast<char *>(message_to_kafka.c_str()), message_to_kafka.size(), nullptr, 0, 0, nullptr, nullptr);
          //if (err != RdKafka::ERR_NO_ERROR) { dunedaq::kafkaraw::CannotProduce(ERS_HERE, "% Failed to produce " + RdKafka::err2str(err));}
          if (err != RdKafka::ERR_NO_ERROR) { std::cout << "% Failed to produce " + RdKafka::err2str(err);}
          //else {std::cout << "Frame sent : " << message_to_kafka << std::endl;}
      }
      catch(const std::exception& e)
      {
          std::string s = e.what();
          //std::cout << s << std::endl;
          //ers::error(dunedaq::kafkaraw::CannotProduce(ERS_HERE, "Error [" + s + "] message(s) were not delivered"));
      } 
      std::cout << std::endl;


    }
    else {
      std::cout << "Skipping unknown fragment type" << std::endl;
    }

  }
}




// Recursive function to traverse the HDF5 file
void exploreSubGroup(HighFive::Group parent_group, std::string relative_path, std::vector<std::string>& path_list) {
  std::vector<std::string> childNames = parent_group.listObjectNames();
  for (auto& child_name : childNames) {
    std::string full_path = relative_path + "/" + child_name;
    HighFive::ObjectType child_type = parent_group.getObjectType(child_name);
    if (child_type == HighFive::ObjectType::Dataset) {
      //std::cout << "Dataset: " << child_name << std::endl;       
      path_list.push_back(full_path);
    } else if (child_type == HighFive::ObjectType::Group) {
      //std::cout << "Group: " << child_name << std::endl;

      HighFive::Group child_group = parent_group.getGroup(child_name);
      // start the recusion
      std::string new_path = relative_path + "/" + child_name;
      exploreSubGroup(child_group, new_path, path_list);
    }
  }
}



std::vector<std::string> traverseFile(HighFive::File input_file, int num_trs) {

  // Vector containing the path list to the HDF5 datasets
  std::vector<std::string> path_list; 

  std::string top_level_group_name = input_file.getPath();
  if (input_file.getObjectType(top_level_group_name) == HighFive::ObjectType::Group) {
    HighFive::Group parent_group = input_file.getGroup(top_level_group_name); 
    exploreSubGroup(parent_group, top_level_group_name, path_list);
  }
  // =====================================
  // THIS PART IS FOR TESTING ONLY
  // FIND A WAY TO USE THE HDF5DAtaStore 
  int i = 0;
  std::string prev_ds_name;
  for (auto& dataset_path : path_list) {
    if (dataset_path.find("Fragment") == std::string::npos && prev_ds_name.find("TriggerRecordHeader") != std::string::npos && i >= num_trs) {
      break;
    }
    if (dataset_path.find("TriggerRecordHeader") != std::string::npos) ++i;

    //readDataset(dataset_path);
    HighFive::Group parent_group = input_file.getGroup(top_level_group_name);
    HighFive::DataSet data_set = parent_group.getDataSet(dataset_path);
    HighFive::DataSpace thedataSpace = data_set.getSpace();
    size_t data_size = data_set.getStorageSize();
    char* membuffer = new char[data_size];
    data_set.read(membuffer);
    readDataset(dataset_path, membuffer);
    delete[] membuffer;

    prev_ds_name = dataset_path;
  }
  // =====================================
  
  return path_list;
}

int main(int argc, char** argv){
  int num_trs = 1000;

/*
  if(argc <2) {
    std::cerr << "Usage: data_file_browser <fully qualified file name> [number of events to read]" << std::endl;
    return -1;
  }

  if(argc == 3) {
    num_trs = std::stoi(argv[2]);
  }   
  // Open the existing hdf5 file
  HighFive::File file(argv[1], HighFive::File::ReadOnly);*/
  m_host = "188.185.122.48";
  m_port = "9092";
  m_topic = "dunedqm-incommingchannel2";
  //Kafka server settings
  std::string brokers = m_host + ":" + m_port;
  std::string errstr;

  RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
  conf->set("bootstrap.servers", brokers, errstr);
  if(errstr != ""){
      dunedaq::kafkaraw::CannotProduce(ERS_HERE, "Bootstrap server error : " + errstr);
  }
  if(const char* env_p = std::getenv("DUNEDAQ_APPLICATION_NAME")) 
      conf->set("client.id", env_p, errstr);
  else
      conf->set("client.id", "rawdataProducerdefault", errstr);

  if(errstr != ""){
      dunedaq::kafkaraw::CannotProduce(ERS_HERE, "Producer configuration error : " + errstr);
  }
  //Create producer instance
  m_producer = RdKafka::Producer::create(conf, errstr);

  if(errstr != ""){
      dunedaq::kafkaraw::CannotProduce(ERS_HERE, "Producer creation error : " + errstr);
  }


  HighFive::File file("/eos/home-y/yadonon/swtest_run000001_0000_yadonon_20210920T111253.hdf5", HighFive::File::ReadOnly);

  std::vector<std::string> data_path = traverseFile(file, num_trs);
  return 0;
}

