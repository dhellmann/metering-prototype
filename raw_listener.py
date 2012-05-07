# Doesn't see exists events. Not waiting long enough, or listening in
# the wrong way?

from pprint import pprint

from kombu import BrokerConnection, Exchange, Queue


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


def show_message(body, message):
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


with BrokerConnection('amqp://guest:secrete@localhost//') as conn:
    instance_exists(conn.channel()).declare()

    with conn.Consumer(instance_exists, callbacks=[show_message]) as consumer:
        while True:
            print 'looping...'
            conn.drain_events()
