cmake_minimum_required(VERSION 3.12)
project(triggertransform VERSION 1.3.0)

find_package(daq-cmake REQUIRED)

daq_setup_environment()

find_package(opmonlib REQUIRED)
find_package(CPR REQUIRED)
find_package(CURL REQUIRED)
find_package(RdKafka REQUIRED)
find_package(ers REQUIRED)
find_package(dataformats REQUIRED)
find_package(HighFive REQUIRED)


##############################################################################
# Plugins

daq_add_application(trigger_transform trigger_transform.cxx LINK_LIBRARIES dataformats::dataformats HighFive RdKafka::rdkafka++ CURL::libcurl ers::ers ${CPR_LIBRARIES} )
target_include_directories(trigger_transform PUBLIC ${CPR_INCLUDE_DIR})


##############################################################################
# No integration tests written

##############################################################################
# No unit tests written

##############################################################################

daq_install()

