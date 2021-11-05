# Consumer 

The consumer will read all data from the Kafka topic and write it directly to Spark. 

The schema needs to be defined that is equivalent to the structure of the incoming JSON, see an example below. It's worth noting that all JSON needs to be utf-8 encoded.

```python
schema = StructType([
  StructField('ts', StringType(), True),
  StructField('symbol', StringType(), True),
  StructField('price', FloatType(), True)
])
```

The Kafka topic will be read from the earliest offset to the latest and parsed into a Spark dataframe. Summary stats are then generated for the `price` column, which are parsed into a Python dictionary for ease of use.

The dataframe price column is then filtered by $ \mu ± 3\sigma $. The resulting dataframe is then printed to the console.

### Dependencies

Apart from the Python dependecies in `requirements.txt`, the Spark package `spark-sql-kafka-0-10_2.12:3.2.0` is required. Previous versions of spark-sql-kafka proved to be problematic. 

