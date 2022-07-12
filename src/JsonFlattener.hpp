/**
 * @file JsonFlattener.hpp unitlity class
 * 
 * This class takes the nested structure produced by opmon
 * and creates a number of simpler json with no chieldren blocks
 *
 * This is part of the DUNE DAQ Application Framework, copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#ifndef KAFKAOPMON_SRC_JSONFLATTENER_HPP_
#define KAFKAOPMON_SRC_JSONFLATTENER_HPP_

#include "logging/Logging.hpp"

#include <nlohmann/json.hpp>

#include <algorithm>
#include <iostream>
#include <iomanip>
#include <fstream>
#include <string>
#include <vector>
#include <sstream>
#include <array>

using json = nlohmann::json;

namespace dunedaq
{
  ERS_DECLARE_ISSUE(kafkaopmon, OpmonJSONValidationError,
		    "JSON input incorrect" << error, ((std::string)error))
  
  ERS_DECLARE_ISSUE(kafkaopmon, IncorrectJSON,
		    "JSON input incorrect" << Warning,
		    ((std::string)Warning))
  
  ERS_DECLARE_ISSUE(kafkaopmon, ErrorJSON,
		    "JSON input error" << Error,
		    ((std::string)Error))  
  
  namespace kafkaopmon
    {
        class JsonFlattener
        {

        public:

	  JsonFlattener() = delete;
	  explicit JsonFlattener(const nlohmann::json& j);
	  /**
	   * Convert a nlohmann::json wiht nested metrics into
	   * a vector of simple json that are similar to the logcal structure 
	   * accpeted by influx DB
	   */
	  const std::vector<nlohmann::json> & get() const { return m_components; }

	  inline static std::string m_source_id_tag = "source_id"; // NOLINT
	  static constexpr char m_separator = '.';
	  
	protected:
	  void parse_json(std::string path,
			  const nlohmann::json& j);

        private:
	  
	  std::vector<nlohmann::json> m_components;
	  nlohmann::json m_tags;
	  
        };
    } // namespace kafkaopmon
} // namespace dunedaq

#endif // KAFKAOPMON_SRC_JSONFLATTENER_HPP_
