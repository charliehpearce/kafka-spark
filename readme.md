# Kafka Spark 

A data engineering project combining randomly generated data with a Spark processing. See `consumer/` and `producer/` for information on the consumer and producer respectively.

# Requirements
- docker-compose version 1.25.4+
- docker 18.09.6+

# Useful docker-compose commands

Use this command to run all services. The producer application should execute and send messges to the `prices` topic. The consumer application should read the data and print the output to screen.

```
docker-compose up --remove-orphans --force-recreate --build
```

### Producer
```
docker-compose up --remove-orphans --force-recreate --build producer
```
### Consumer
```
docker-compose up --remove-orphans --force-recreate --build consumer
```
### Stop all services
```
docker-compose down
```
