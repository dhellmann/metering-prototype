
import eventlet
eventlet.monkey_patch()

from kombu.pools import producers
from kombu import Exchange, Queue

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

if __name__ == "__main__":
    from kombu import BrokerConnection

    connection = BrokerConnection('amqp://guest:secrete@localhost//')

    payload = {u'_context_roles': [u'admin'],
               u'_context_request_id': u'req-659a8eb2-4372-4c01-9028-ad6e40b0ed22',
               u'_context_quota_class': None,
               u'event_type': u'compute.instance.exists',
               u'timestamp': u'2012-05-08 16:03:44.122481',
               u'message_id': u'4b884c03-756d-4c06-8b42-80b6def9d302',
               u'_context_auth_token': None,
               u'_context_is_admin': True,
               u'_context_project_id': None,
               u'_context_timestamp': u'2012-05-08T16:03:43.760204',
               u'_context_read_deleted': u'no',
               u'_context_user_id': None,
               u'_context_remote_address': None,
               u'publisher_id': u'compute.vagrant-precise',
               u'payload': {u'state_description': u'',
                            u'display_name': u'testme',
                            u'memory_mb': 512,
                            u'disk_gb': 0,
                            u'audit_period_beginning': u'2012-05-08 15:00:00',
                            u'tenant_id': u'7c150a59fe714e6f9263774af9688f0e',
                            u'created_at': u'2012-05-07 22:16:18',
                            u'instance_type_id': 2,
                            u'bandwidth': {},
                            u'instance_id': u'3a513875-95c9-4012-a3e7-f90c678854e5',
                            u'instance_type': u'm1.tiny',
                            u'state': u'active',
                            u'audit_period_ending': u'2012-05-08 16:00:00',
                            u'user_id': u'1e3ce043029547f1a61c1996d1a531a2',
                            u'deleted_at': u'',
                            u'launched_at': u'2012-05-07 23:01:27',
                            u'image_ref_url': u'http://10.0.2.15:9292/images/9438520c-cb77-4d72-b01d-d6ec766c6e66'},
               u'priority': u'INFO',
               }

    with producers[connection].acquire(block=True) as producer:
        for i in xrange(1000):
            producer.publish(payload,
                             routing_key='notifications.info',
                             )
        # producer.publish({'stop_now': True},
        #                  routing_key='notifications.info',
        #                  )
