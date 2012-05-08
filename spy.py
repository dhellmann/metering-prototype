#!/usr/bin/env python

import datetime
import logging
import pprint

import notificationclient


LOG = logging.getLogger(__name__)

TIMESTAMP_FMT = '%Y-%m-%d %H:%M:%S'


def get_shared_info(body):
    if 'audit_period_ending' in body['payload']:
        timestamp = body['payload']['audit_period_ending']
        audit_period_beginning = body['payload']['audit_period_beginning']
        start = datetime.datetime.strptime(
            audit_period_beginning.partition('.')[0],
            TIMESTAMP_FMT,
            )
        audit_period_ending = body['payload']['audit_period_ending']
        end = datetime.datetime.strptime(
            audit_period_ending.partition('.')[0],
            TIMESTAMP_FMT,
            )
        # FIXME(dhellmann): This is a datetime.timedelta now but
        # should be a simple number based on the frequency of the
        # counter.
        duration = (end - start)
    else:
        # FIXME(dhellmann): Can probably do better than this for termating an instance
        timestamp = body['timestamp']
        duration = 0

    return {'user_id': body['payload']['user_id'],
            'project_id': body['payload']['tenant_id'],
            'resource_id': body['payload']['instance_id'],
            'counter_datetime': timestamp,
            'counter_duration': duration,
            }


def c1(body):
    c = {'source': '?',
         'counter_type': 'instance',
         'counter_volume': body['payload']['instance_type_id'],
         'payload': body['payload']['display_name'],
         }
    c.update(get_shared_info(body))
    return c


def c2(body):
    c = {'source': '?',
         'counter_type': 'cpu',
         'counter_volume': 0,
         }
    c.update(get_shared_info(body))
    return c


def c3(body):
    c = {'source': '?',
         'counter_type': 'ram',
         'counter_volume': body['payload']['memory_mb'],
         }
    c.update(get_shared_info(body))
    return c


def c4(body):
    c = {'source': '?',
         'counter_type': 'disk',
         'counter_volume': body['payload']['disk_gb'],
         }
    c.update(get_shared_info(body))
    return c


def c5(body):
    c = {'source': '?',
         'counter_type': 'io',
         'counter_volume': 0,
         }
    c.update(get_shared_info(body))
    return c


def process_event(body, message):
    print
    pprint.pprint(body)

    event_type = body.get('event_type')
    if event_type in set(['compute.instance.create.end',
                          'compute.instance.delete.end',
                          'compute.instance.exists',
                          ]):
        counters = [c1(body),
                    c2(body),
                    c3(body),
                    c4(body),
                    c5(body),
                    ]

    else:
        LOG.debug('ignoring message: %s', event_type)
        counters = []

    if counters:
        pprint.pprint(counters)
        print

    # elif not event_type.startswith('compute.instance'):
    #     LOG.info('Event: %s (%s)', event_type, body['timestamp'])
    # else:
    #     payload = body['payload']
    #     interesting = dict(
    #         event_type=body['event_type'],
    #         timestamp=body['timestamp'],
    #         display_name=payload['display_name'],
    #         user_id=payload['user_id'],
    #         address=payload.get('address'),
    #         tenant_id=payload.get('tenant_id'),
    #         instance_id=payload.get('instance_id'),
    #         )
    #     LOG.debug('interesting = %s', interesting)
    message.ack()


if __name__ == '__main__':
    import argparse
    import sys

    from kombu import BrokerConnection

    parser = argparse.ArgumentParser(
        description='watch notifications go by in the queue',
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

    with BrokerConnection(args.amqp) as conn:
        handler = notificationclient.NotificationClient(conn, process_event)
        try:
            handler.run()
        except KeyboardInterrupt:
            pass
        print 'Exiting'
