from kafka import KafkaConsumer
import pandas as pd
import json
from pyspark.sql import SparkSession


def main():
    # Define consumer obj
    consumer = KafkaConsumer(
        'prices',
        bootstrap_servers='kafka:9092',
        group_id='group1',
        auto_offset_reset='earliest',
        consumer_timeout_ms=10000,  # If no messages recived after 10s break the iterator
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    batch = []
    for msg in consumer:
        batch.append(msg.value)
        print(msg.value)

    # Open spark session
    spark = SparkSession.builder.master('local').getOrCreate()

    data = [tuple(i.values()) for i in batch]
    cols = list(batch[0].keys())

    df = spark.createDataFrame(data=data, schema=cols)

    df.show()

    # Calcualte stats
    print(f'There are {len(batch)} objects in the topic "prices"')
    df = pd.DataFrame(batch)
    price_col = df['price']
    max_value = price_col.max()
    min_value = price_col.min()
    mean_value = price_col.mean()
    std_value = price_col.std()

    print(
        f'Max: {max_value}, min: {min_value}, mean: {mean_value}, std: {std_value}')

    # Is outlier if the price is 3 std away from the mean
    upper_bound = mean_value + 3*std_value
    lower_bound = mean_value - 3*std_value

    # Filter dataframe
    outliers = df[(df['price'] > upper_bound) |
                  (df['price'] < lower_bound)]

    print(outliers.to_string(index=False))


if __name__ == "__main__":
    main()
