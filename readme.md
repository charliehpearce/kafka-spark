# Data Engeneering

The exercise is to develop an Extract-Transform-Load (ETL) pipeline. We have provided a template that will serve as the scaffolding and make it easy to develop and test your development.

The pipeline has two parts:
1. A producer: generates mock data and sends it to a Kafka topic called `prices`
1. A consumer: reads the data from the `prices` topic and calculates some statistics. 

Your consumer application should read data from a [Kafka](https://kafka.apache.org/) topic and compute some statistics and print them to standard output for visual inspection.

Write your application in Python, Java or Scala. We must be able to run the project from the command line, without the need of an IDE or extra dependencies than the ones listed in the requirements section.

We have provided a [docker-compose](https://docs.docker.com/compose/) file to provide all components of the project; you can use other technologies if you prefer.

The exercise should not take more than 3 hours. However, you are free to take as much time as you'd like to work on it.

# Requirements
- docker-compose version 1.25.4+
- docker 18.09.6+

# Producer
This application writes data to a `prices` Kafka topic.

A template for the producer is in the [producer](./producer/producer.py). Data, in json format, should be written to the `prices` topic.

Each message will include three pieces of data:
1. ts: the event timestamp (yyyy-mm-dd hh:mm:ss.fff)
2. symbol: a stock symbol, randomly selected from a pre-defined list of 5 symbols.
3. price: a float data type with two-digit precision.

Sample message
```
{
    "ts": "2021-03-10 16:21:32.000", 
    "symbol": "APPL", 
    "price": 238.32
}
```

The producer should send 5000 messages to the topic and exit. To ensure that we have some results in step 3. You can use `random.normalvariate` to generate the prices with a normal distribution.

The borker url is `kafka:9092` and the topic name is `prices`. The topic is pre-created for you. 

This application can be written using standard Python, or other laguage if you prefer.

# Consumer
The consumer is an application that reads all data available in the `prices` topic and peforms the following computations: 

1. Count and print the number of messages in the topic.
   
2. Calculate the min, max, mean and standard deviation for the prices per symbol and print to screen.  
 
3. Find any outliers using the computed statistics from step 2, print only the timestamp, symbol and anomalous price.  

The consumer is a batch process. You do not need to implement the computations in a streaming manner.

You are encouraged to use Spark or other data-processing frameworks but this is not essential. 

# Useful docker-compose commands

### Run all services
Use this command to run all services. The producer application should execute and send messges to the `prices` topic. The consumer application should read the data and print the output to screen.

```
docker-compose up --remove-orphans --force-recreate --build
```
--- 

During the development process, it might be useful to run the services independently and on-demand to quickly assess your changes.  

You can do this by running either the consumer or producer independently. 


### Producer
```
docker-compose up --remove-orphans --force-recreate --build producer
```
---

### Consumer
```
docker-compose up --remove-orphans --force-recreate --build consumer
```
---

### Stop all services
```
docker-compose down
```
--- 


## Other considerations
Include whatever documentation you believe is necessary for us to understand and maintain the application.  

Include tests that verify the behavior of your application.  

## Bonus Points
Implement your consumer application using a distributed processing framework such as [Apache Spark](https://spark.apache.org/).
