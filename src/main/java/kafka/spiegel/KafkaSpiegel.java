package kafka.spiegel;

import com.lmax.disruptor.dsl.Disruptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by mykidong on 2016-08-05.
 */
public class KafkaSpiegel {

    private static Logger log = LoggerFactory.getLogger(KafkaSpiegel.class);

    public static final String CONF_POLL_TIMEOUT = "consumer.poll.timeout";
    public static final String CONF_BUF_SIZE = "disruptor.buffer.size";


    private KafkaConsumer<byte[], byte[]> consumer;
    private Producer<byte[], byte[]> producer;

    private Properties consumerProps;
    private Properties producerProps;
    private List<String> topics;
    private Map<String,String> spiegelProps;
    private long timeout;
    private Map<TopicPartition, OffsetAndMetadata> latestTpMap;

    private int bufferSize;
    private ProduceHandler produceHandler;
    private Disruptor<SpiegelEvent> spiegelEventDisruptor;

    private SpiegelEventTranslator spiegelEventTranslator;


    public KafkaSpiegel(Properties consumerProps, Properties producerProps, List<String> topics, Map<String,String> spiegelProps)
    {
        this.consumerProps = consumerProps;
        this.producerProps = producerProps;
        this.topics = topics;
        this.spiegelProps = spiegelProps;
        this.timeout = Long.parseLong(this.spiegelProps.get(CONF_POLL_TIMEOUT));
        this.bufferSize = Integer.parseInt(this.spiegelProps.get(CONF_BUF_SIZE));


        this.consumer = new KafkaConsumer<byte[], byte[]>(this.consumerProps);
        this.producer = new KafkaProducer<byte[], byte[]>(this.producerProps);


        // init. produce handler.
        this.produceHandler = new ProduceHandler();
        this.produceHandler.setConsumer(this.consumer);
        this.produceHandler.setProducer(this.producer);


        // disruptor instance for spiegel event.
        spiegelEventDisruptor = DisruptorSingleton.getInstance(this.bufferSize, this.produceHandler);

        // init. disruptor translator.
        this.spiegelEventTranslator = new SpiegelEventTranslator();
    }

    public void run()
    {
        try
        {
            this.consumer.subscribe(this.topics, new PartitionBalancer(this.produceHandler));

            while(true)
            {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(this.timeout);

                for (ConsumerRecord<byte[], byte[]> record : records) {
                    String topic = record.topic();
                    int partition = record.partition();
                    byte[] value = record.value();
                    long offset = record.offset();

                    TopicPartition tp = new TopicPartition(topic, partition);

                    latestTpMap.put(tp, new OffsetAndMetadata(offset));

                    // set props to translator.
                    this.spiegelEventTranslator.setTopic(topic);
                    this.spiegelEventTranslator.setPartition(partition);
                    this.spiegelEventTranslator.setValue(value);
                    this.spiegelEventTranslator.setOffset(offset);

                    // publish it to disruptor queue.
                    this.spiegelEventDisruptor.publishEvent(this.spiegelEventTranslator);
                }
            }
        } catch (WakeupException e) {

        }finally {
            // do something before closing consumer.
            this.consumer.close();
        }
    }
}
