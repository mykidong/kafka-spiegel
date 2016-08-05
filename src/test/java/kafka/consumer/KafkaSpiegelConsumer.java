package kafka.consumer;

import kafka.consumer.deserialize.AvroDeserializeService;
import kafka.consumer.deserialize.ClasspathAvroDeserializeService;
import kafka.spiegel.KafkaSpiegelTestSkip;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.xml.DOMConfigurator;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by mykidong on 2016-08-05.
 */
public class KafkaSpiegelConsumer {
    private static Logger log;

    @Before
    public void init() throws Exception {
        java.net.URL url = new KafkaSpiegelTestSkip().getClass().getResource("/log4j-test.xml");
        System.out.println("log4j url: " + url.toString());

        DOMConfigurator.configure(url);

        log = LoggerFactory.getLogger(KafkaSpiegelConsumer.class);
    }


    @Test
    public void run() throws Exception
    {
        // destination kafka broker.
        String brokers = System.getProperty("brokers", "localhost:9093");

        // kafka consumer properties.
        Properties kafkaConsumerProps = new Properties();
        kafkaConsumerProps.put("bootstrap.servers", brokers);
        kafkaConsumerProps.put("group.id", "kafka-spiegel-destination-group");
        kafkaConsumerProps.put("enable.auto.commit", "true");
        kafkaConsumerProps.put("session.timeout.ms", "30000");
        kafkaConsumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        kafkaConsumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        List<String> topics = new ArrayList<>();
        topics.add("item-view-event");

        Properties topicAndPathProps = new Properties();
        topicAndPathProps.put("item-view-event", "/META-INF/avro/item-view-event.avsc");

        AvroDeserializeService avroDeserializeService = new ClasspathAvroDeserializeService(topicAndPathProps);

        Thread t = new Thread(new ConsumerTask(kafkaConsumerProps, avroDeserializeService, topics));
        t.start();

        Thread.sleep(Long.MAX_VALUE);
    }


    private static class ConsumerTask implements Runnable
    {
        private Properties kafkaConsumerProps;
        private AvroDeserializeService avroDeserializeService;

        private KafkaConsumer<Integer, byte[]> consumer;

        private List<String> topics;

        public ConsumerTask(Properties kafkaConsumerProps, AvroDeserializeService avroDeserializeService, List<String> topics)
        {
            this.kafkaConsumerProps = kafkaConsumerProps;
            this.avroDeserializeService = avroDeserializeService;
            this.topics = topics;

            this.consumer = new KafkaConsumer<Integer, byte[]>(this.kafkaConsumerProps);
        }

        @Override
        public void run() {

            this.consumer.subscribe(topics);

            while (true)
            {
                ConsumerRecords<Integer, byte[]> records = consumer.poll(1000);


                for (ConsumerRecord<Integer, byte[]> record : records) {
                    String topic = record.topic();
                    int partition = record.partition();
                    byte[] value = record.value();
                    long offset = record.offset();

                    TopicPartition tp = new TopicPartition(topic, partition);

                    GenericRecord genericRecord = avroDeserializeService.deserializeAvro("item-view-event", value);

                    try {
                        log.info("value in json: [{}]", genericRecord.toString());

                    }catch (Exception e)
                    {                        log.error("error: " + e.getMessage());
                        throw new RuntimeException(e);
                    }
                }
            }

        }
    }
}
