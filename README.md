# kafka-spiegel

TODO: description to be added.


## run source kafka broker.
mvn -e -Dtest=RunLocalKafka -DbrokerPath=kafkaPropLocal.properties -DzkPath=zkPropLocal.properties test;

## run destination kafka broker.
mvn -e -Dtest=RunLocalKafka -DbrokerPath=kafkaPropLocal2.properties -DzkPath=zkPropLocal2.properties test;

## run kafka spiegel.
mvn -e -Dtest=KafkaSpiegelTestSkip test;

## run test producer to send message to source kafka broker.
mvn -e -Dtest=GenericRecordKafkaProducer test;

## run test consumer to print messages consumed from destination kafka broker.
TODO:...