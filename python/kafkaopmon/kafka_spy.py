#
# @file kafka-spy.py connecting to kakfa to print the message content.
# This is part of the DUNE DAQ software, copyright 2020.
#  Licensing/copyright details are in the COPYING file that you should have
#  received with this code.
#

from kafka import KafkaConsumer
import json
import click

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option('--kafka-address', type=click.STRING, default="monkafka.cern.ch", help="address of the kafka broker")
@click.option('--kafka-port', type=click.INT, default=30092, help='port of the kafka broker')       
@click.option('--kafka-topics', multiple=True, default=['opmon'], help='topics of the kafka broker')
@click.option('--kafka-consumer-id', type=click.STRING, default='microservice', help='id of the kafka consumer, not really important')
@click.option('--kafka-consumer-group', type=click.STRING, default='kakfa-spy', help='group ID of the kafka consumer, very important to be unique or information will not be duplicated')
@click.option('--parse-as-json', type=click.BOOL, default=True, help='Tries to parse the messaage as a json')


def cli(kafka_address, kafka_port, kafka_topics, kafka_consumer_id, kafka_consumer_group, parse_as_json):

    bootstrap = f"{kafka_address}:{kafka_port}"
    print("From Kafka server:",bootstrap)
    
    consumer = KafkaConsumer(bootstrap_servers=bootstrap,
                             group_id=kafka_consumer_group, 
                             client_id=kafka_consumer_id)

    print("Consuming topics:", kafka_topics)
    consumer.subscribe(kafka_topics)

    if parse_as_json :
        print_json(consumer=consumer)
    else:
        print_string(consumer=consumer)
       


def print_string(consumer):
    for message in consumer:
        print("Key:", message.key)
        print("Timestamp:", message.timestamp)
        print("Message:", message.value)

def print_json(consumer):
    for message in consumer:
        js = json.loads(message.value)
        print("Key:", message.key)
        print("Timestamp:", message.timestamp)
        print("Json:", js)

            
if __name__ == '__main__':
    cli(show_default=True, standalone_mode=True)
