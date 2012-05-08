#!/usr/bin/env python

import cPickle as pickle
import logging

import notificationclient


LOG = logging.getLogger(__name__)


import argparse
import sys

from kombu import BrokerConnection

parser = argparse.ArgumentParser(
    description='record notification events for playback',
    )
parser.add_argument('output_file',
                    help='the data file to create',
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

with open(args.output_file, 'wb') as output:

    def process_event(body, message):
        LOG.debug(body.get('event_type', 'unknown event'))
        pickle.dump(body, output)
        message.ack()

    with BrokerConnection(args.amqp) as conn:
        handler = notificationclient.NotificationClient(conn, process_event)
        try:
            handler.run()
        except KeyboardInterrupt:
            pass
        print 'Exiting'
