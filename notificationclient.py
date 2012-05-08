import logging

import eventlet
eventlet.monkey_patch()

from kombu import Exchange, Queue
from kombu.mixins import ConsumerMixin


LOG = logging.getLogger(__name__)


class NotificationClient(ConsumerMixin):

    def __init__(self, name, connection, callback):
        self.connection = connection
        self.callback = callback
        # FIXME(dhellmann): Only works with Nova right now
        LOG.debug('creating queue %s', name)
        self.queue = Queue(name=name,
                           exchange=Exchange(name='nova',
                                             type='topic',
                                             durable=False,
                                             auto_delete=False,
                                             ),
                           routing_key='notifications.info',
                           durable=False,
                           auto_delete=True,
                           exclusive=True,
                           )

    def get_consumers(self, Consumer, channel):
        return [Consumer(queues=[self.queue],
                         callbacks=[self.process_event],
                         )
                ]

    def process_event(self, body, message):
        eventlet.spawn_n(self.callback, body, message)

    def on_consume_ready(self, *args, **kwds):
        LOG.debug('ready to receive notifications')

    def on_consume_end(self, *args, **kwds):
        LOG.debug('shutting down')

    def on_iteration(self, *args, **kwds):
        pass
