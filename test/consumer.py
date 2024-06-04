from confluent_kafka import Consumer
import os

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'ksb-demo-group',
    'auto.offset.reset': 'earliest',
    'plugin.library.paths': os.getcwd()+'/audit_consumer_interceptor',  # Ensure correct path
    'audit.bootstrap.servers': os.environ.get('AUDIT_BOOTSTRAP_SERVER'),
    'audit.sasl.mechanism': 'PLAIN',
    'audit.security.protocol': 'SASL_SSL',
    'audit.sasl.username': os.environ.get('AUDIT_SASL_USERNAME'),
    'audit.sasl.password': os.environ.get('AUDIT_SASL_PASSWORD'),
    'audit.topic': 'audit_topic',
}

consumer = Consumer(conf)
consumer.subscribe(['test_topic'])

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue
        print(f'Received message: {msg.value().decode("utf-8")}')
finally:
    consumer.close()