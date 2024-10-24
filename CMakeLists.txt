cmake_minimum_required(VERSION 3.12)
project(kafkaopmon VERSION 3.0.0)

find_package(daq-cmake REQUIRED)

daq_setup_environment()

find_package(opmonlib REQUIRED)
find_package(RdKafka REQUIRED)
find_package(ers REQUIRED)     
find_package(Boost COMPONENTS unit_test_framework program_options REQUIRED)



daq_add_library(*.cpp LINK_LIBRARIES ${Boost_LIBRARIES} opmonlib::opmonlib RdKafka::rdkafka RdKafka::rdkafka++)


##############################################################################
# Plugins

daq_add_plugin(streamOpMonFacility duneOpMonFacility LINK_LIBRARIES kafkaopmon)

##############################################################################
# No integration tests written

##############################################################################
# unit tests
daq_add_unit_test( OpMonPublisher_test       LINK_LIBRARIES kafkaopmon )
daq_add_unit_test( stream_OpMonFacility_test LINK_LIBRARIES kafkaopmon )

##############################################################################
# test application

daq_add_application( opmon_publisher_test opmon_publisher_test.cxx TEST LINK_LIBRARIES kafkaopmon )
daq_add_application( opmon_publish_to_kafka_test opmon_publish_to_kafka_test.cxx TEST LINK_LIBRARIES kafkaopmon )

daq_install()

