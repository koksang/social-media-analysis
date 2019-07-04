import time
import random
import json
from kafka import KafkaProducer
from kafka.errors import KafkaError

# broker config from docker
ADDRESS = 'localhost:9092'
TRENDS = 'twitter-trends'
USERS = 'twitter-users'
# value serializer for sending data to broker
json_serializer = lambda data: json.dumps(data).encode('utf-8')
# producer
producer = KafkaProducer(
                bootstrap_servers=ADDRESS,
                value_serializer=json_serializer,
                retries=2)

def publish_trends_message(json_data, client=producer, serializer=json_serializer, topic=TRENDS):
    def on_send_success(record_metadata):
        print('TOPIC: %s -- PARTITION: %s -- OFFSET: %s' % (record_metadata.topic, record_metadata.partition, record_metadata.offset))
    def on_send_error(excp):
        log.error('Error ', exc_info=excp)
    for row in json_data:
        client.send(topic, {'topic': row['topic'],'url': row['url']}).add_callback(on_send_success).add_errback(on_send_error)
    # flush all async messages
    producer.flush()

def publish_users_message(json_data, client=producer, serializer=json_serializer, topic=USERS):
    def on_send_success(record_metadata):
        print('TOPIC: %s -- PARTITION: %s -- OFFSET: %s' % (record_metadata.topic, record_metadata.partition, record_metadata.offset))
    def on_send_error(excp):
        log.error('Error ', exc_info=excp)
    for row in json_data:
        client.send(topic, {'topic': row['topic'],'user': row['user']}).add_callback(on_send_success).add_errback(on_send_error)
    # flush all async messages
    producer.flush()