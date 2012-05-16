from pprint import pprint

import eventlet
eventlet.monkey_patch()

from kombu import Exchange, Queue
from kombu.mixins import ConsumerMixin


class MessageHandler(ConsumerMixin):

    queue = Queue(name='notifications.info',
                  exchange=Exchange(name='nova',
                                    type='topic',
                                    durable=False,
                                    auto_delete=False,
                                    ),
                  routing_key='notifications.info',
                  durable=False,
                  auto_delete=False,
                  )

    def __init__(self, connection):
        self.connection = connection

    def on_consume_ready(self, *args, **kwds):
        print 'Ready to receive', args, kwds

    def on_consume_end(self, *args, **kwds):
        print 'Done', args, kwds

    def on_iteration(self, *args, **kwds):
        pass

    def get_consumers(self, Consumer, channel):
        return [Consumer(queues=[self.queue],
                         callbacks=[self.process_event],
                         )
                ]

    def process_event(self, body, message):
        res = eventlet.spawn_n(self._process_event, body, message)
        #print 'spawned', res

    def _process_event(self, body, message):
        print 'Raw body:', body
        # message is a kombu.transport.pyamqplib.Message
        #print 'Message :', message
        event_type = body.get('event_type')
        if event_type is None:
            print 'unrecognized message'
        elif not event_type.startswith('compute.instance'):
            print event_type, body['timestamp']
        else:
            payload = body['payload']
            interesting = dict(
                event_type=body['event_type'],
                timestamp=body['timestamp'],
                display_name=payload['display_name'],
                user_id=payload['user_id'],
                address=payload.get('address'),
                tenant_id=payload.get('tenant_id'),
                instance_id=payload.get('instance_id'),
                )
            pprint(interesting)
        print
        message.ack()


if __name__ == '__main__':
    from kombu import BrokerConnection
    from kombu.utils.debug import setup_logging
    setup_logging(loglevel='INFO')

    # from eventlet import backdoor

    with BrokerConnection('amqp://guest:secrete@localhost//') as conn:
        handler = MessageHandler(conn)
        # eventlet.spawn(backdoor.backdoor_server,
        #                eventlet.listen(('localhost', 3000)),
        #                locals())
        try:
            handler.run()
        except KeyboardInterrupt:
            pass
        print 'Exiting'
