import logging

from nova import flags
from nova import manager

FLAGS = flags.FLAGS
LOG = logging.getLogger(__name__)


class MeteringManager(manager.Manager):
    def init_host(self):
        LOG.debug('init_host')

    @manager.periodic_task
    def _first_task(self, context):
        LOG.debug('_first_task')

    @manager.periodic_task
    def _second_task(self, context):
        LOG.debug('_second_task')
