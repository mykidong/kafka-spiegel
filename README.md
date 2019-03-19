# kafka-spiegel
This Kafka Spiegel moves data from a kafka cluster to another kafka cluster.
It is similar to [Kafka MirrorMaker](http://kafka.apache.org/documentation.html#basic_ops_mirror_maker).

This consists of kafka consumer, disruptor queue, and kafka producer.
Kafka consumer consumes data from source kafka cluster and publish it to disruptor queue. 
In disruptor handler, the incoming events are collected which will be flushed to destination kafka cluster at every timer interval(for instance, every 1000 ms).

Kafka Consumer Properties to consume from source kafka cluster should look like this:
```
# source consumer props.
bootstrap.servers=localhost:9092
group.id=kafka-spiegel-test-group
enable.auto.commit=false
session.timeout.ms=30000
key.deserializer=org.apache.kafka.common.serialization.ByteArrayDeserializer
value.deserializer=org.apache.kafka.common.serialization.ByteArrayDeserializer
```

, and Kafka Producer Properties to produce to destination kafka cluster like this:
```
# destination kafka producer props.
bootstrap.servers=localhost:9093
acks=all
max.in.flight.requests.per.connection=1
key.serializer=org.apache.kafka.common.serialization.ByteArraySerializer
value.serializer=org.apache.kafka.common.serialization.ByteArraySerializer
```

, and additional spiegel properties for KafkaSpiegel like this:
```
# spiegel props.
consumer.poll.timeout=1000
disruptor.buffer.size=1024
producer.flush.interval.in.mills=1000
producer.flush.max.event.size=1000
```

Kafka Spiegel can be run like this:
```
java kafka.spiegel.KafkaSpiegelMain --consumer.props props/sourceConsumer.properties \
                                    --producer.props props/destProducer.properties \
                                    --topics item-view-event \
                                    --spiegel.props props/spiegel.properties
```


## Run Demo
The following shows data movement from source kafka cluster to destination kafka cluster.

## Run source kafka broker.
As source kafka cluster, run source kafka broker:
```
mvn -e -Dtest=RunLocalKafka -DbrokerPath=kafkaPropLocal.properties -DzkPath=zkPropLocal.properties test;
```

## Run destination kafka broker.
As destination kafka cluster, run destination kafka broker:
```
mvn -e -Dtest=RunLocalKafka -DbrokerPath=kafkaPropLocal2.properties -DzkPath=zkPropLocal2.properties test;
```

## Run kafka spiegel.
Run kafka spiegel which runs kafka consumer connected to source kafka cluster and kafka producer connected to destination kafka cluster:
```
mvn -e -Dtest=KafkaSpiegelTestSkip \
        -DconsumerProp=props/sourceConsumer.properties \                          
        -DproducerProp=props/destProducer.properties \       
        -DspiegelProp=props/spiegel.properties \
        -Dtopics=events test;
```

## Run test producer to send message to source kafka broker.
This is test data producer to send messages to source kafka broker.
```
mvn -e -Dtest=GenericRecordKafkaProducer test;
```


## Run test consumer to print messages consumed from destination kafka broker.
To print the messages from the destination kafka broker, run the following test case:
```
mvn -e -Dtest=KafkaSpiegelConsumer test;
```
