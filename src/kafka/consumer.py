import json
from kafka import KafkaConsumer, TopicPartition

# broker config from docker
ADDRESS = 'localhost:9092'
TRENDS = 'twitter-trends'
USERS = 'twitter-users'

# initialize consumer to given topic and broker
consumer_trends = KafkaConsumer(
                TRENDS,
                bootstrap_servers=ADDRESS,
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                consumer_timeout_ms=1000
            )
consumer_users = KafkaConsumer(
                USERS,
                bootstrap_servers=ADDRESS,
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                consumer_timeout_ms=1000
            )

# loop and print messages
def get_trends_message(client=consumer_trends):
    messages = [json.loads(msg.value.decode('utf-8')) for msg in client]
    return messages

def get_users_message(client=consumer_users):
    messages = [json.loads(msg.value.decode('utf-8')) for msg in client]
    return messages