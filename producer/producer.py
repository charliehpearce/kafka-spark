import json
import random
import datetime
from kafka import KafkaProducer

SYMBOLS = ["APPL", "TSLA", "BTC", "GM", "SONY"]


def generate_payload():
    payload = {
        'ts': str(datetime.datetime.now()),
        'symbol': SYMBOLS[random.randint(0, len(SYMBOLS)-1)],
        'price': round(random.normalvariate(200, 20), 2)
    }
    return payload


def main():
    # https://kafka-python.readthedocs.io/en/master/usage.html
    topic = 'prices'
    producer = KafkaProducer(
        bootstrap_servers='kafka:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    for _ in range(5000):
        payload = generate_payload()
        future = producer.send(topic, payload)
        result = future.get(timeout=60)
        print(result)


if __name__ == "__main__":
    main()
