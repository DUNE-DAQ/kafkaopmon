#!/usr/bin/env python3

import os
import socket
import inspect
from opmonlib.opmon_entry_pb2 import OpMonValue, OpMonId, OpMonEntry 
import google.protobuf.message as msg
from google.protobuf.descriptor import FieldDescriptor as fd
from google.protobuf.timestamp_pb2 import Timestamp
from datetime import datetime
from kafka import KafkaProducer
from typing import Optional
import logging

class OpMonPublisher:
    def __init__(
                    self, 
                    topic:str,
                    bootstrap:str = "monkafka.cern.ch:30092", # Removed for if we don't want to use OpMon (i.e. for ssh-standalone)
                    application_name:str = "python",
                    package_name:str = "unknown"
    ) -> None:
        ## Options from configurations
        self.application_name = application_name
        self.package_name = package_name
        self.bootstrap = bootstrap
        if not topic.startswith('monitoring.'):
            topic = 'monitoring.' + topic
        self.topic = topic

        ## runtime options
        self.log = logging.getLogger("OpMonPublisher")
        self.log.info("Starting Kafka producer")
        self.producer = KafkaProducer(
                                        bootstrap_servers=self.bootstrap,
                                        value_serializer=lambda v: v.SerializeToString(),
                                        key_serializer=lambda k: str(k).encode('utf-8')
        )
        self.log.info("Initialized Kafka producer")        

    def publish(
        self,
        session:str,
        application:str,
        message:msg,
        custom_origin:Optional[dict[str,str]] = {"": ""},
        substructure:Optional[str] = "",
    ):
        """Create an OpMonEntry and send it to Kafka."""
        data_dict = self.map_message(message)
        opmon_id = OpMonId(
            session = session,
            application = application,
            substructure = substructure
        )
        t = Timestamp()
        opmon_metric = OpMonEntry(
            time = t.GetCurrentTime(),
            origin = opmon_id,
            custom_origin = custom_origin,
            measurement = message.DESCRIPTOR.name,
            data = data_dict,
        )
        return self.producer.send(opmon_metric)

    def map_message(self, message:msg):
        message_dict = {}
        for name, descriptor in message.DESCRIPTOR.fields_by_name.items():
            message_dict[name] = self.map_entry(getattr(message, name), descriptor.cpp_type)
        return message_dict 

    def map_entry(self, value, field_type:int):
        formatted_OpMonValue = OpMonValue() 
        match field_type:
            case fd.CPPTYPE_INT32:
                formatted_OpMonValue.int4_value = value
            case fd.CPPTYPE_INT64:
                formatted_OpMonValue.int8_value = value
            case fd.CPPTYPE_UINT32:
                formatted_OpMonValue.uint4_value = value
            case fd.CPPTYPE_UINT64:
                formatted_OpMonValue.uint8_value = value
            case fd.CPPTYPE_DOUBLE:
                formatted_OpMonValue.double_value = value
            case fd.CPPTYPE_FLOAT:
                formatted_OpMonValue.float_value = value
            case fd.CPPTYPE_BOOL:
                formatted_OpMonValue.boolean_value = value
            case fd.CPPTYPE_STRING:
                formatted_OpMonValue.string_value = value
            case _:
                raise ValueError("Value is of a non-supported type.")
        return formatted_OpMonValue