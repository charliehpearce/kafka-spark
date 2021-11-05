from kafka import KafkaConsumer
import pandas as pd
import json
from pyspark.sql import SparkSession


def main():
    # Define kafka consumer
    consumer = KafkaConsumer(
        'prices',
        bootstrap_servers='kafka:9092',
        group_id='group1',
        auto_offset_reset='earliest',
        consumer_timeout_ms=10000,  # If no messages recived after 10s break the iterator
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    # Open spark session
    spark = SparkSession.builder.master('local').getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    batch = []
    for msg in consumer:
        batch.append(msg.value)
        print(msg.value)

    # Create Spark df
    data = (tuple(i.values()) for i in batch)
    cols = list(batch[0].keys())
    df = spark.createDataFrame(data=data, schema=cols)
    df.show()

    # Summary stats
    df_summary = df.describe(['price'])
    df_summary.show()

    # Summary table into dictionary
    stats = dict(
        map(lambda row: (row.summary, float(row.price)), df_summary.collect()))

    # Filter by 3*price std and show dataframe
    upper_bound = stats['mean'] + 3*stats['stddev']
    lower_bound = stats['mean'] - 3*stats['stddev']
    df.filter((df.price > upper_bound) | (df.price < lower_bound)).show()


if __name__ == "__main__":
    main()
