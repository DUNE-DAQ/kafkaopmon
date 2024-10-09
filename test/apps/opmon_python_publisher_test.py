#!/usr/bin/env python3

from kafkaopmon.OpMonPublisher import OpMonPublisher
from opmonlib.opmon_entry_pb2 import OpMonEntry 
import getpass, time
from druncschema.generic_pb2 import string_msg
import click
import logging


CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])

@click.command(context_settings=CONTEXT_SETTINGS)
@click.option('--kafka-address', type=click.STRING, default="monkafka.cern.ch", help="address of the kafka broker")
@click.option('--kafka-port', type=click.INT, default=30092, help='port of the kafka broker')
@click.option('--topic', type=click.STRING, default="test.kafkaopmon")
@click.option('-m/--message', type=click.STRING, default="kafkaopmon python binding test")
@click.option('-n/--number-messages', type=click.INT, default=10000)
def test(kafka_address:str, kafka_port:int, topic:str, message:str, number_messages:int):
    bootstrap = kafka_address + ":" + kafka_port
    pub = OpMonPublisher(
        default_topic = topic,
        bootstrap = bootstrap,
        application_name = "kafkaopmon_python_test",
        package_name = "kafkaopmon"
    )

    for i in range(number_messages):
        test_msg_str = message + str(i)
        test_msg = string_msg(value=test_msg_str)
        pub.publish(
            session = "test_session",
            application = "opmon_python_publisher_test",
            message = test_msg
        )

if __name__ == '__main__':
    test(show_default=True, standalone_mode=True)
