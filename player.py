#!/usr/bin/env python

import argparse
import cPickle as pickle
import logging
import sys

import eventlet
eventlet.monkey_patch()

from kombu import BrokerConnection
from kombu.pools import producers


LOG = logging.getLogger(__name__)


parser = argparse.ArgumentParser(
    description='play back recorded notification events',
    )
parser.add_argument('input_file',
                    help='the data file to read',
                    )
parser.add_argument('--amqp',
                    default='amqp://guest:secrete@localhost//',
                    help='location of AMQP service',
                    )
args = parser.parse_args()

console = logging.StreamHandler(sys.stderr)
console.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(message)s')
console.setFormatter(formatter)
root_logger = logging.getLogger('')
root_logger.addHandler(console)
root_logger.setLevel(logging.DEBUG)

with open(args.input_file, 'rb') as input:

    with BrokerConnection(args.amqp) as connection:
        while True:
            try:
                event = pickle.load(input)
            except EOFError:
                break
            else:
                print event.get('event_type')
                with producers[connection].acquire(block=True) as producer:
                    producer.publish(event, routing_key='notifications.info')
