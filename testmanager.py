from nova import log as logging

from nova import flags
from nova import manager
from nova.rpc import impl_kombu

import notificationclient

FLAGS = flags.FLAGS
LOG = logging.getLogger('nova.metering.testmanager')


# Taken from ceilometer bootstrap code
class Connection(impl_kombu.Connection):
    """A Kombu connection that does not use the AMQP Proxy class when
    creating a consumer, so we can decode the message ourself."""

    def create_consumer(self, topic, proxy, fanout=False):
        """Create a consumer without using ProxyCallback."""
        if fanout:
            self.declare_fanout_consumer(topic, proxy)
        else:
            self.declare_topic_consumer(topic, proxy)


class MeteringManager(manager.Manager):
    def init_host(self):
        LOG.debug('init_host')
        self.notifier_conn = Connection(FLAGS)
        self.notifier_conn.declare_topic_consumer(
            topic='notifications.info',
            callback=self._on_notification,
            )
        self.notifier_conn.consume_in_thread()

    def _on_notification(self, body):
        event_type = body.get('event_type')
        LOG.info('NOTIFICATION: %s', event_type)

    @manager.periodic_task
    def _first_task(self, context):
        LOG.debug('_first_task')

    @manager.periodic_task
    def _second_task(self, context):
        LOG.debug('_second_task')
