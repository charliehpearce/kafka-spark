from pyspark.sql import SparkSession
from pyspark.sql.functions import decode, from_json, col, explode
from pyspark.sql.types import StructType, StructField, FloatType, TimestampType, StringType


def main():
    # Open spark session
    spark = SparkSession.builder.master('local').getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

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
    stats = dict([(row.summary, float(row.price))
                 for row in df_summary.collect()])

    # Filter by 3*price std and show dataframe
    upper_bound = stats['mean'] + 3*stats['stddev']
    lower_bound = stats['mean'] - 3*stats['stddev']
    df.filter((df.price > upper_bound) | (
        df.price < lower_bound)).show(truncate=False)


if __name__ == "__main__":
    main()
