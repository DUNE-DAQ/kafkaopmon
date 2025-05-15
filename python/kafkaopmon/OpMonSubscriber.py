#!/usr/bin/env python3

import getpass
import logging
import os
import re
import socket
import threading
from collections.abc import Callable

import google.protobuf.message as msg
import opmonlib.opmon_entry_pb2 as entry
from kafka import KafkaConsumer


class OpMonFunction:
    """Define callback function properties to store and validate function execution."""

    def __init__(
        self, function: Callable, opmon_id: re.Pattern, measurement: re.Pattern
    ) -> None:
        """Construct the OpMonFunction."""
        self.function = function
        self.opmon_id = opmon_id
        self.measurement = measurement
        return

    def match(self, key: str) -> bool:
        """Validate the key follows the standard structure."""
        opmon_id, measure = key.split("/", 1)
        if not self.opmon_id.match(opmon_id):
            return False
        if not self.measurement.match(measure):
            return False
        return True

    def execute(self, e: entry.OpMonEntry) -> None:
        """Execute the function."""
        self.function(e)
        return


class OpMonSubscriber:
    """Subscribe to a kafka topic to read OpMon metrics."""

    def __init__(
        self,
        bootstrap: str,
        group_id: str | None = None,
        timeout_ms: int = 500,
        topics: dict[str, str] | None = None,
    ) -> None:
        """Construct the OpMonSubscriber."""
        ## Options from configurations
        self.bootstrap = bootstrap
        self.group_id = group_id
        self.timeout = timeout_ms
        if len(topics) == 0:
            msg = "Topic list is empty"
            raise ValueError(msg)
        self.topics = topics
        ## runtime options
        self.running = False
        self.functions = {}
        self.thread = threading.Thread(target=self.message_loop)
        return

    def default_id(self) -> str:
        """Construct the default kafka consumer ID."""
        node = socket.gethostname()
        user = getpass.getuser()
        process = os.getpid()
        thread = threading.get_ident()
        return f"{node}-{user}-{process}-{thread}"

    def add_callback(
        self,
        name: str,
        function: Callable,
        opmon_id: str = ".*",
        measurement: str = ".*",
    ) -> bool:
        """Register a callback function to the OpMonSubscriber."""
        if name in self.functions:
            return False

        was_running = self.running
        if was_running:
            self.stop()

        f = OpMonFunction(
            function=function,
            opmon_id=re.compile(opmon_id),
            measurement=re.compile(measurement),
        )

        self.functions[name] = f

        if was_running:
            self.start()
        return True

    def clear_callbacks(self) -> None:
        """Remove all callback functions from the OpMonSubscriber."""
        if self.running:
            self.stop()
        self.functions.clear()
        return

    def remove_callback(self, name: str) -> bool:
        """Remove the named callback functions from the OpMonSubscriber."""
        if name not in self.functions.keys():
            return False

        was_running = self.running
        if was_running:
            self.stop()

        self.functions.pop(name)

        if was_running and len(self.functions) > 0:
            self.start()
        return True

    def start(self) -> None:
        """Start listening to the kafka topic."""
        logging.info("Starting run")
        self.running = True
        self.thread.start()
        return

    def stop(self) -> None:
        """Stop listening to the kafka topic."""
        self.running = False
        self.thread.join()
        return

    def message_loop(self) -> None:
        """Process entries read in with the KafkaConsumer."""
        if not self.group_id:
            group_id = self.default_id()
        else:
            group_id = self.group_id

        consumer = KafkaConsumer(
            bootstrap_servers=self.bootstrap,
            group_id=group_id,
            client_id=self.default_id(),
            consumer_timeout_ms=self.timeout,
        )

        topics = self.topics
        consumer.subscribe(["monitoring." + s for s in topics])

        keys_str = ", ".join(self.functions.keys())
        logging.info("ID: %s running with functions %s", group_id, keys_str)

        while self.running:
            try:
                message_it = iter(consumer)
                message = next(message_it)
                key = message.key.decode("ascii")
                ## The key from the message is binary
                ## In order to correctly match an ascii regex, we have to convert

                for function in self.functions.values():
                    if function.match(key):
                        e = entry.OpMonEntry()
                        e.ParseFromString(message.value)
                        function.execute(e)

            except msg.DecodeError:
                logging.exception("Could not parse message")
            except StopIteration:
                pass
            except Exception:
                logging.exception("Unhandled exception thrown")

        logging.info("Stop run")
