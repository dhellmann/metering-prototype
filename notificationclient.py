import abc
import logging

import eventlet
eventlet.monkey_patch()

from kombu import Exchange, Queue
from kombu.mixins import ConsumerMixin


LOG = logging.getLogger(__name__)


class NotificationClient(ConsumerMixin):
    __metaclass__ = abc.ABCMeta

    # FIXME(dhellmann): Only works with Nova right now
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

    def get_consumers(self, Consumer, channel):
        return [Consumer(queues=[self.queue],
                         callbacks=[self.process_event],
                         )
                ]

    def process_event(self, body, message):
        eventlet.spawn_n(self._process_event, body, message)

    def on_consume_ready(self, *args, **kwds):
        LOG.debug('ready to receive notifications')

    def on_consume_end(self, *args, **kwds):
        LOG.debug('shutting down')

    def on_iteration(self, *args, **kwds):
        pass

    @abc.abstractmethod
    def _process_event(self, body, message):
        pass
