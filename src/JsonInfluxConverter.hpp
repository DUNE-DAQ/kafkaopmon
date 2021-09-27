// * This is part of the DUNE DAQ Application Framework, copyright 2020.
// * Licensing/copyright details are in the COPYING file that you should have received with this code.

#ifndef KAFKAOPMON_SRC_JSONINFLUXCONVERTER_HPP_
#define KAFKAOPMON_SRC_JSONINFLUXCONVERTER_HPP_

#include "logging/Logging.hpp"

#include <nlohmann/json.hpp>

#include <algorithm>
#include <iostream>
#include <iomanip>
#include <fstream>
#include <string>
#include <vector>
#include <sstream>

using json = nlohmann::json;

namespace dunedaq
{
    ERS_DECLARE_ISSUE(influxopmon, IncorrectJSON,
        "JSON input incorrect" << Warning,
        ((std::string)Warning))

    ERS_DECLARE_ISSUE(influxopmon, ErrorJSON,
        "JSON input error" << Error,
        ((std::string)Error))

    namespace influxopmon
    {
        class JsonConverter
        {

        public:

	  JsonConverter() = default;

            /**
             * Convert a nlohmann::json object to an influxDB INSERT string.
             *
             * @param   Param 1 if true, the tags are not added to the querry.
             *          Param 2 is a vector of key-words delimiting tags
             *          Param 3 is the key word delimiting the timestamp
             *          Param 4 is a string formatted flatened json object
             *
             * @return Void, to get call get_inserts_vector
             */
            void set_inserts_vector(const json& json)
            { 
                try
                {
		  m_inserts_vector = json_to_influx(json);
                }
                catch (const std::runtime_error& re)
                {
                    // speciffic handling for runtime_error
                    ers::error(ErrorJSON(ERS_HERE, "Runtime error: " + std::string(re.what())));
                }
                catch (const std::exception& ex)
                {
                    // extending std::exception, except
                    ers::error(ErrorJSON(ERS_HERE, "Error occurred: " + std::string(ex.what())));
                }
                catch (...)  // NOLINT catchall isn't being used here to swallow problems without notification, so it's OK
                {
                    ers::error(ErrorJSON(ERS_HERE, "Unknown failure occurred. Possible memory corruption" ));
                }
            }
            /**
             * Get a converted vector, to set call set_inserts_vector.
             *
             * @return Vector of string formated influxDB INSERT querries.
             */
            std::vector<std::string> get_inserts_vector()
            {
                return m_inserts_vector;
            }
        
	  JsonConverter(JsonConverter const&) = delete;            
	  JsonConverter(JsonConverter&&) = default;                
	  JsonConverter& operator=(JsonConverter const&) = delete; 
	  JsonConverter& operator=(JsonConverter&&) = default;     

        private:

            std::vector<std::string> m_inserts_vector;

	  bool m_error_state = false;
            const std::string m_parent_tag = "__parent";
            const std::string m_time_tag = "__time"; 
            const std::string m_data_tag = "__data";
            const std::string m_children_tag = "__children";
            const std::string m_properties_tag = "__properties";
            const std::string m_tag_tag = "source_id=";
            const std::string separator = ".";
	        const std::vector<std::string> m_tags = { m_parent_tag, m_time_tag, m_data_tag, m_children_tag, m_properties_tag };

            int m_key_index = 0;
            std::string m_field_set = "";
            std::string m_measurement;
            std::string m_time_stamp;
            std::vector<std::string> m_tag_set;
            std::vector<std::string> m_querries;
            std::vector<std::string> m_hierarchy;

            std::string convert_time_to_NS(std::string time)
            {
                while (time.size() < 19)
                {
                    time = time + "0";
                }
                return time;
            }

            void check_keyword(const std::string& input_tag)
            {
	            if (std::find(m_tags.begin(), m_tags.end(), input_tag) ==  m_tags.end())
                {
                    ers::warning(IncorrectJSON(ERS_HERE, "Uncorrect tag " + input_tag + ", querry dumped, integrity might be compromised."));
                }
            }

            void RecursiveIterateData(std::vector<std::string> composite_key, const json& j)
            {
                
                //Breaks down structures in dataTag
                for (auto& item : j.items())
                {
                    if (item.value().type() == json::value_t::object)
                    {
                        composite_key.push_back(separator);
                        composite_key.push_back(item.key());
                        RecursiveIterateData(composite_key, item.value());
                    }
                    //if no structure
                    if (item.value().type() != json::value_t::object)
                    {
                        if (item.key() == "")
                        {
                            m_field_set = m_field_set + composite_key[0] + "=" + item.value().dump() + ",";
                        }
                        else
                        {
                            m_field_set = m_field_set + std::accumulate(composite_key.begin(), composite_key.end(), std::string("")) + separator + item.key() + "=" + item.value().dump() + ",";
                            //std::cout << std::accumulate(composite_key.begin(), composite_key.end(), std::string("")) + separator + item.key() + "=" + item.value().dump() << std::endl;
                        }
                    }
                    else
                    {
                        //Twice to also remove the separator
                        composite_key.pop_back();
                        composite_key.pop_back();
                    }
                }
            }
            
            std::string GetLastParent(std::vector<std::string> hierarchy,std::string tag_searched)
            {
                bool validTag = false;
                int n = 0;
                std::string concatenated_hierarchy;
                while (!validTag)
                {
                    for (std::string tag : m_tags)
                    {
                        if (hierarchy[hierarchy.size() - (n + 1)] == tag && tag == tag_searched)
                        {
                            for (int i = n-1; i >= 0; i--)
                            {
                                concatenated_hierarchy = concatenated_hierarchy + hierarchy[hierarchy.size() - (i +1)] + ".";
                            }

                            validTag = true;
                            return concatenated_hierarchy;
                        }
                    }
                    n++;
                }
                return "";
            }

            void build_string(const std::string& input)
            {
                auto last_in_hierarchy = m_hierarchy.back();

                if (last_in_hierarchy.substr(0, 2) == "__" && input.substr(0, 2) != "__")
                {
                    check_keyword(last_in_hierarchy);
                    if (last_in_hierarchy == m_children_tag)
                    {
                        m_tag_set.push_back(separator + input);
                    }
                    else if (last_in_hierarchy == m_parent_tag)
                    {
                        m_tag_set.push_back(input);
                    }     
                    else if (last_in_hierarchy == m_properties_tag)
                    {
                        m_measurement = input;
                    }
                }
            }

            void build_string(const std::string& key, json data)
            {
	            auto last_in_hierarchy = m_hierarchy.back();
                if (last_in_hierarchy == m_time_tag)
                {
                    m_time_stamp = convert_time_to_NS(data.dump());
                    std::string full_tag = "";
                    for (const std::string& tag : m_tag_set)
                    {
                        full_tag = full_tag + tag;
                    }
                    if (!m_error_state)
                    {
                        m_querries.push_back(m_measurement + "," + m_tag_tag + full_tag + " " + m_field_set.substr(0, m_field_set.size() - 1) + " " + m_time_stamp);
                    }
                    
                    m_field_set = "";
                    m_error_state = false;
                }
                else 
                {
                    std::string last_parent;

                    last_parent = GetLastParent(m_hierarchy, m_data_tag);
                    
                    //If no key remove the separator from parents //FIXME no key when parsed after strucutre...
                    if (last_parent.size() > 0 && key == "")
                    {
                        if (last_parent[last_parent.size() - 1] == separator[0])
                        {
                            last_parent = last_parent.substr(0, last_parent.size() - 1);
                        }
                    }

                    std::vector<std::string> v(1, last_parent + key);
                    RecursiveIterateData(v, data);
                    
                }
            }
            

            void recursive_iterate_items(const json& j)
            {
                for (auto& item : j.items())
                {
                    if (item.value().begin()->is_structured())
                    {
                        
                        build_string(item.key());
                        m_hierarchy.push_back(item.key());
                        recursive_iterate_items(item.value());
                        m_hierarchy.pop_back();
                        if ((m_hierarchy.back() == m_children_tag || m_hierarchy.back() == m_parent_tag) && !m_tag_set.empty())
                        {
			                m_tag_set.pop_back();
                        }
                    }
                    else
                    {
                        build_string(item.key());
                        m_hierarchy.push_back(item.key());
                        for (auto& last_item : item.value().items())
                        {
                            
                            if(last_item.value().type() == json::value_t::string)
                            {
                                if (last_item.value().dump()[0] != '"') { "\"" + last_item.value().dump(); }
                                if (last_item.value().dump()[last_item.value().dump().size() - 1] != '"') { last_item.value().dump() + "\""; }
                                build_string(last_item.key(), last_item.value());
                            }
                            else
                            {
                                build_string(last_item.key(), last_item.value());
                            }
                        }
                        m_hierarchy.pop_back();
                    }
                }
            }
        std::vector<std::string> json_to_influx(const json& json)
            {
	      m_hierarchy.clear();
                m_hierarchy.push_back("root");
                m_querries.clear();
                m_tag_set.clear();
                recursive_iterate_items(json);
                return m_querries;
            }
        };
    } // namespace influxopmon
} // namespace dunedaq

#endif // KAFKAOPMON_SRC_JSONINFLUXCONVERTER_HPP_
