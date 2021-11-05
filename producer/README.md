# Producer 

The producer will generate a payload of randomly generated data in the form:

```python
payload = {
  'ts': str(datetime.datetime.now()),
  'symbol': SYMBOLS[random.randint(0, len(SYMBOLS)-1)],
  'price': round(random.normalvariate(200, 20), 2)
}
```

Changing the parameters in random.normalvariate($\mu$,  $\sigma$) will adjust the mean and standard deivation of the generated prices.

The payload is then parsed to a JSON string and encoded with utf-8. To change the Kafka bootstrap server ip address and port(s) update the `bootstrap_servers` section of `KafkaConsumer`.

