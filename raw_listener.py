# Doesn't see exists events. Not waiting long enough, or listening in
# the wrong way?

from pprint import pprint

from kombu import BrokerConnection, Exchange, Queue
from kombu.mixins import ConsumerMixin

nova_exchange = Exchange(name='nova',
                         type='topic',
                         durable=False,
                         auto_delete=False,
                         )
instance_exists = Queue(name='notifications.info',
                        exchange=nova_exchange,
                        routing_key='notifications.info',
                        durable=False,
                        auto_delete=False,
                        )


class MessageHandler(ConsumerMixin):

    def __init__(self, connection):
        self.connection = connection

    def get_consumers(self, Consumer, channel):
        return [Consumer(queues=[instance_exists],
                         callbacks=[self.process_event],
                         )
                ]

    def process_event(self, body, message):
        #print body.keys()
        event_type = body['event_type']
        if not event_type.startswith('compute.instance'):
            print event_type, body['timestamp']
        else:
            payload=body['payload']
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

    with BrokerConnection('amqp://guest:secrete@localhost//') as conn:
        try:
            MessageHandler(conn).run()
        except KeyboardInterrupt:
            print 'Exiting'
        # instance_exists(conn.channel()).declare()

        # with conn.Consumer(instance_exists, callbacks=[show_message]) as consumer:
        #     while True:
        #         print 'looping...'
        #         conn.drain_events()
