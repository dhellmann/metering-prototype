import logging

import notificationclient


LOG = logging.getLogger(__name__)


class ComputeEventToMeter(notificationclient.NotificationClient):

    def _process_event(self, body, message):
        LOG.debug('Raw body: %s', body)
        # message is a kombu.transport.pyamqplib.Message
        #print 'Message :', message
        event_type = body.get('event_type')
        if event_type is None:
            LOG.debug('unrecognized message')
        elif not event_type.startswith('compute.instance'):
            LOG.info('Event: %s (%s)', event_type, body['timestamp'])
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
            LOG.debug('interesting = %s', interesting)
        message.ack()


if __name__ == '__main__':
    import sys

    from kombu import BrokerConnection
    #from kombu.utils.debug import setup_logging
    #setup_logging(loglevel='DEBUG')

    console = logging.StreamHandler(sys.stderr)
    console.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(message)s')
    console.setFormatter(formatter)
    root_logger = logging.getLogger('')
    root_logger.addHandler(console)
    root_logger.setLevel(logging.DEBUG)

    # from eventlet import backdoor

    with BrokerConnection('amqp://guest:secrete@localhost//') as conn:
        handler = ComputeEventToMeter(conn)
        # eventlet.spawn(backdoor.backdoor_server,
        #                eventlet.listen(('localhost', 3000)),
        #                locals())
        try:
            handler.run()
        except KeyboardInterrupt:
            pass
        print 'Exiting'
