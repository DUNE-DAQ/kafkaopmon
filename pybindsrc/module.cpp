/**
 * @file module.cpp. This performs python binding for the kafkaopmon package.
 *
 * This is part of the DUNE DAQ Software Suite, copyright 2022.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include "pybind11_json.hpp"
#include <nlohmann/json.hpp>
#include "JsonFlattener.hpp"


namespace py = pybind11;

namespace dunedaq {
namespace kafkaopmon {
namespace python {
	

PYBIND11_MODULE(_daq_kafkaopmon_py, module) {

  py::class_<kafkaopmon::JsonFlattener>(module, "JsonFlattener")
    // constructor 
    .def(py::init<const nlohmann::json&>())
    // methods
    .def("get", &kafkaopmon::JsonFlattener::get);
}

} // namespace python
} // namespace kafkaopmon
} // namespace dunedaq
