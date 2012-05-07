# Doesn't see exists events. Not waiting long enough, or listening in
# the wrong way?

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
    print body
    message.ack()


with BrokerConnection('amqp://guest:secrete@localhost//') as conn:
    instance_exists(conn.channel()).declare()

    with conn.Consumer(instance_exists, callbacks=[show_message]) as consumer:
        while True:
            print 'looping...'
            conn.drain_events()
