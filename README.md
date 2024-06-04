# Librdkafka Interceptors

Source for Kafka Summit Bangalore 2024 talk on [librdkafka masterclass](https://www.confluent.io/events/kafka-summit-bangalore-2024/interceptor-masterclass-for-librdkafka-clients/).

![audit-use-case](assets/images/LibrdKafka%20Interceptors%20Example.png)

## Setup

### Kafka cluster

To run the example, you will need a kafka cluster running. The most simple way to get a Kafka cluster is using [docker](https://kafka.apache.org/quickstart) or [Confluent Cloud](https://confluent.cloud/)

With docker - 

```bash
docker run -d -p 9092:9092 apache/kafka:3.7.0
```

### Build interceptors

#### Producer interceptor

```bash
gcc -g -o audit_producer_interceptor.so -shared -fPIC audit_producer_interceptor.c -lrdkafka -lcrypto -ljansson -luuid
```

#### Consumer interceptor

```bash
gcc -g -o audit_consumer_interceptor.so -shared -fPIC audit_consumer_interceptor.c -lrdkafka -ljansson
```

### Test interceptors

The test directory contains python scripts for producer and consumer with the interceptors configured. The code uses Confluent Cloud as the audit producer's cluster. This can be changed based on the kafka cluster used.

#### Create virtual environment

```bash
python -m venv test/.venv

source test/.venv/bin/activate

pip install -r test/requirements.txt

cp test/.env.example test/.env

# Update the bootstrap server, username and password in test/.env

set -a; source test/.env; set +a
```

#### Run producer

Create the producer and audit topics in the clusters

```bash
python test/producer.py
```

#### Run consumer

```bash
python test/consumer.py
```

#### Test topic data

```bash
kafka-console-consumer --bootstrap-server localhost:9092 --from-beginning --property print.headers=true --topic test_topic
```

#### Test audit data

```bash
echo -e "security.protocol=SASL_SSL\nsasl.mechanism=PLAIN\nsasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$AUDIT_SASL_USERNAME\" password=\"$AUDIT_SASL_PASSWORD\";"

kafka-console-consumer --bootstrap-server $AUDIT_BOOTSTRAP_SERVER --consumer.config client.properties --from-beginning --property print.key=true --topic audit_topic
```