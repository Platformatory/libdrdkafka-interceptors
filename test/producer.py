from confluent_kafka import Producer
import random
import os

conf = {
    'bootstrap.servers': 'localhost:9092',
    'plugin.library.paths': os.getcwd()+'/audit_producer_interceptor.so',  # Ensure correct path
    'client.id': 'ksb-demo-producer',
    'audit.bootstrap.servers': os.environ.get('AUDIT_BOOTSTRAP_SERVER'),
    'audit.sasl.mechanism': 'PLAIN',
    'audit.security.protocol': 'SASL_SSL',
    'audit.sasl.username': os.environ.get('AUDIT_SASL_USERNAME'),
    'audit.sasl.password': os.environ.get('AUDIT_SASL_PASSWORD'),
    'audit.topic': 'audit_topic',
}

def main():

    producer = Producer(conf)

    def delivery_report(err, msg):
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    # Produce a message
    producer.produce('test_topic', "Hello, KSB! Here's a random number - "+str(random.randint(1, 2100)), callback=delivery_report)
    producer.produce('test_topic', "Hello, KSB! Here's another random number - "+str(random.randint(1, 2100)), callback=delivery_report)
    producer.flush()

if __name__ == "__main__":
    main()