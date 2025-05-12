#!/usr/bin/env python3

from opmonlib.opmon_entry_pb2 import OpMonValue, OpMonId, OpMonEntry
from google.protobuf.message import Message as msg
from google.protobuf.descriptor import FieldDescriptor as fd
from google.protobuf.timestamp_pb2 import Timestamp

import os
import logging
from pathlib import Path
import sys

from datetime import datetime
from kafka import KafkaProducer
from typing import Optional

class OpMonPublisher:
    def __init__(
                    self,
                    conf: dict[str: str],
                    uri: dict[str: str], 
                    default_topic:str,
                    type:str = "",
                    log_level:int = logging.DEBUG
    ) -> None:
        # Set up text logging
        self.log = logging.getLogger("OpMonPublisher")
        self.log.setLevel(log_level)

        if conf:
            self.conf = conf
        else:
            self.log.error("Missing OpMon configuration!")
            sys.exit(1)

        self.level = getattr(self.conf, "level", "")
        self.interval_s = getattr(self.conf, "interval_s", "")
        if not self.level or not self.interval_s:
            self.log.error("Invalid 'OpMonConf' format: Missing required fields.")
            sys.exit(1)

        if uri:
            self.uri = uri
        else:
            raise AttributeError("Missing OpMon URI!")

        self.path = getattr(self.uri, "path", "")
        self.type = getattr(self.uri, "type", "")
        if not self.path or not self.type:
            self.log.error("Invalid 'OpMonURI' format: Missing required fields.")
            sys.exit(1)

        self.bootstrap = None
        self.topic = None

        if "/" in self.path:
            self.bootstrap, self.topic = self.path.split("/", 1)

        if not self.topic:
            self.topic = "OpMon"
        self.topic = "monitoring." + self.topic

        ## Options from configurations
        if self.bootstrap == "":
            self.log.warning(f"There is no boostrap provided, not initializing publisher to topic {default_topic}")
            self.opmon_producer = None
            return None

        # Setup the opmon publisher
        if self.type == "stream":
            self.opmon_producer = KafkaProducer(
                bootstrap_servers = self.bootstrap,
                value_serializer = lambda v: v.SerializeToString(),
                key_serializer = lambda k: str(k).encode('utf-8')
            )
        elif self.type == "file":
            if not self.path:
                raise AttributeError("Invalid 'OpMonConf' - for file-based configurations a file must be provided!")
            if Path(self.path).suffix != ".log":
                raise AttributeError("Log file must end with '.log'.")
            file_handler = logging.FileHandler(self.path)
            file_handler.setLevel(log_level)
            self.opmon_producer = logging.getLogger("monitoring.%s", default_topic)
        else:
            self.opmon_producer = logging.getLogger("monitoring.%s", default_topic)
        return

    def validate_custom_origin(custom_origin:Optional[dict[str,str]] = {}):
        for key, value in custom_origin.items():
            if type(value) != "str":
                try:
                    custom_origin[key] = str(value)
                except:
                    raise TypeError(f"custom_origin[{key}] is not a string and cannot be converted to one.")
        return custom_origin

    def pack_to_OpMonEntry(
        self,
        session:str,
        application:str,
        message:msg,
        custom_origin:Optional[dict[str,str]],
        substructure:Optional[list[str]],
        t: Timestamp
    ) -> OpMonEntry:
        opmon_id = OpMonId(
            session = session,
            application = application,
            substructure = substructure
        )
        opmon_entry = OpMonEntry(
            time = t,
            origin = opmon_id,
            custom_origin = self.validate_custom_origin(custom_origin),
            measurement = message.DESCRIPTOR.full_name,
            data = self.map_message(message),
        )
        return opmon_entry

    def publish_to_stream(self, message:msg, metric: OpMonEntry) -> None:
        """Send an OpMonEntry to Kafka."""
        target_topic = self.extract_topic(message)
        target_key = self.extract_key(metric)

        self.opmon_producer.send(
            target_topic,
            value = metric,
            key = target_key
        )
        return

    def publish_to_file(self, metric: OpMonEntry) -> None:
        publisher = logging.getLogger("monitoring.%s.%s", session, application)
        return

    def publish(
        self,
        session:str,
        application:str,
        message:msg,
        custom_origin:Optional[dict[str,str]] = {},
        substructure:Optional[list[str]] = []
    ) -> None:

        # Pre-map checks
        if not self.opmon_producer:
            self.log.warning(f"An improperly initialized OpMonProducer with topic {self.default_topic} has been used, nothing will be published.")
            return
        if not isinstance(message, msg):
            raise ValueError("This is not an accepted publish value, it needs to be of type google.protobuf.message")

        metric = self.pack_to_OpMonEntry(
            self,
            session,
            application,
            message,
            custom_origin,
            substructure,
            Timestamp().GetCurrentTime()
        )

        if self.type == "stream":
            self.publish_to_stream(message, metric)
        else:
            self.publish_to_file(metric)
        return

    def extract_topic(self, message:msg) -> str:
        if not self.opmon_producer:
            self.log.warning(f"An improperly initialized OpMonProducer with topic {self.default_topic} has been used, nothign will be published.")
            return None
        return self.default_topic

    def extract_key(self, opmon_entry:OpMonEntry) -> str:
        if not self.opmon_producer:
            self.log.warning(f"An improperly initialized OpMonProducer with topic {self.default_topic} has been used, nothign will be published.")
            return None
        key = str(opmon_entry.origin.session)
        if (opmon_entry.origin.application != ""):
            key += "." + opmon_entry.origin.application
        for substructureID in opmon_entry.origin.substructure:
            key += "." + substructureID
        key += '/' + str(opmon_entry.measurement)
        return key

    def map_message(self, message:msg, top_block:str=""):
        message_dict = {}
        for name, descriptor in message.DESCRIPTOR.fields_by_name.items():
            if descriptor.label == fd.LABEL_REPEATED:
                continue # We don't want to keep repeated values as this doens't work for influxdb as there is no way to store repeated values
            elif descriptor.cpp_type == fd.CPPTYPE_MESSAGE:
                # Prepend the name of the nested message to the attribute name
                top_block += name + "."
                message_dict = message_dict | self.map_message(getattr(message, name), top_block)
            else:
                message_dict[top_block + name] = self.map_entry(getattr(message, name), descriptor.cpp_type)
        return message_dict

    def map_entry(self, value, field_type:int) -> OpMonValue:
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
            # Ignore unknown types.
        return formatted_OpMonValue