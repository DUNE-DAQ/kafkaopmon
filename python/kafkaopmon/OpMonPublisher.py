#!/usr/bin/env python3

import logging
import sys

from google.protobuf.message import Message as Msg
from google.protobuf.timestamp_pb2 import Timestamp
from kafka import KafkaProducer
from opmonlib.utils import parse_opmon_conf, pack_to_opmonentry


class OpMonPublisher:
    """Tool for publishing operational monitoring metrics to kafka."""

    def __init__(
        self, conf: dict[str:str], uri: dict[str:str], log_level: int = logging.INFO
    ) -> None:
        """Construct the object to publish OpMon metrics to kafka."""
        self.log = logging.getLogger("OpMonPublisher")
        self.log.setLevel(log_level)

        opmon_conf = parse_opmon_conf(self.log, conf, uri)
        self.type = opmon_conf["type"]
        if self.type != "stream":
            self.log.error("Type must be stream to publish to kafka.")
            sys.exit(1)
        self.bootstrap = opmon_conf["bootstrap"]
        self.level = opmon_conf["level"]
        self.interval_s = opmon_conf["interval_s"]
        self.topic = "monitoring." + opmon_conf["topic"]

        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap,
            value_serializer=lambda v: v.SerializeToString(),
            key_serializer=lambda k: str(k).encode("utf-8"),
        )
        return

    def extract_topic(self, message: Msg) -> str:
        """Extract the topic intended to publish to."""
        if not self.producer:
            self.log.warning(
                "Producer incorrectly formatted, nothing will be published."
            )
            return None
        return self.default_topic

    def publish(
        self,
        session: str,
        application: str,
        message: Msg,
        custom_origin: dict[str, str] | None = None,
        substructure: list[str] | None = None,
    ) -> None:
        """Publish the message to either a file or the terminal."""
        if not isinstance(message, Msg):
            self.log.error("Passed message needs to be of type google.protobuf.message")
            return

        metric = pack_to_opmonentry(
            self,
            session,
            application,
            message,
            custom_origin,
            substructure,
            Timestamp().GetCurrentTime(),
        )
        target_topic = self.extract_topic(message)
        target_key = self.extract_key(metric)

        self.producer.send(target_topic, value=metric, key=target_key)
        return
