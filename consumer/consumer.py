from pyspark.sql import SparkSession
from pyspark.sql.functions import decode, from_json, col, explode
from pyspark.sql.types import StructType, StructField, FloatType, TimestampType, StringType


def main():
    # Open spark session
    spark = SparkSession.builder.master('local').getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    """
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

    # Create Spark DF
    data = (tuple(i.values()) for i in batch)
    cols = list(batch[0].keys())
    df = spark.createDataFrame(data=data, schema=cols)
    df.show()
    """

    schema = StructType([
        StructField('ts', StringType(), True),
        StructField('symbol', StringType(), True),
        StructField('price', FloatType(), True)
    ])

    input_stream = spark\
        .read\
        .format('kafka')\
        .option('kafka.bootstrap.servers', 'kafka:9092')\
        .option('kafka.group.id', 'group1')\
        .option('subscribe', 'prices')\
        .option('startingOffsets', 'earliest')\
        .option('endingOffsets', 'latest')\
        .load()\
        .select(from_json(col("value").cast("string"), schema).alias("parsed_value"))

    df = input_stream.select("parsed_value.*")

    df.printSchema()

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
