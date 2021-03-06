package kafka.spiegel;

import com.lmax.disruptor.dsl.Disruptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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

    /**
     * consumer poll timeout in millis.
     */
    public static final String CONF_POLL_TIMEOUT = "consumer.poll.timeout";


    /**
     * disruptor ringbuffer size.
     */
    public static final String CONF_BUF_SIZE = "disruptor.buffer.size";

    /**
     * producer messages flush interval in millis.
     */
    public static final String CONF_FLUSH_INTERVAL = "producer.flush.interval.in.mills";

    /**
     * max events size to flush messages by producer.
     */
    public static final String CONF_MAX_EVENT_SIZE = "producer.flush.max.event.size";

    /**
     * default max. event size to flush by producer.
     */
    public final long DEFAULT_MAX_EVENT_SIZE = 100000;


    private KafkaConsumer<byte[], byte[]> consumer;
    private Producer<byte[], byte[]> producer;

    private Properties consumerProps;
    private Properties producerProps;
    private List<String> topics;
    private Map<String, String> spiegelProps;
    private long timeout;
    private long interval;
    private long maxEventSize;

    private int bufferSize;
    private ProduceHandler produceHandler;
    private Disruptor<SpiegelEvent> spiegelEventDisruptor;

    private SpiegelEventTranslator spiegelEventTranslator;

    private boolean wakeupCalled = false;

    private final Object lock = new Object();

    public KafkaSpiegel(Properties consumerProps, Properties producerProps, List<String> topics, Map<String, String> spiegelProps) {
        this.consumerProps = consumerProps;

        // set auto commit to false.
        this.consumerProps.put("enable.auto.commit", "false");

        this.producerProps = producerProps;
        this.producerProps.put("retries", Integer.MAX_VALUE);
        this.producerProps.put("max.block.ms", Long.MAX_VALUE);

        this.topics = topics;
        this.spiegelProps = spiegelProps;
        this.timeout = Long.parseLong(this.spiegelProps.get(CONF_POLL_TIMEOUT));
        this.bufferSize = Integer.parseInt(this.spiegelProps.get(CONF_BUF_SIZE));
        this.interval = Long.parseLong(this.spiegelProps.get(CONF_FLUSH_INTERVAL));
        this.maxEventSize = (this.spiegelProps.containsKey(CONF_MAX_EVENT_SIZE)) ? Long.parseLong(this.spiegelProps.get(CONF_MAX_EVENT_SIZE)) : DEFAULT_MAX_EVENT_SIZE;


        this.consumer = new KafkaConsumer<byte[], byte[]>(this.consumerProps);
        this.producer = new KafkaProducer<byte[], byte[]>(this.producerProps);


        // init. produce handler.
        this.produceHandler = new ProduceHandler(this.consumer, this.producer, this.interval, this.maxEventSize);


        // disruptor instance for spiegel event.
        spiegelEventDisruptor = DisruptorSingleton.getInstance(this.bufferSize, this.produceHandler);

        // init. disruptor translator.
        this.spiegelEventTranslator = new SpiegelEventTranslator();
    }

    public KafkaConsumer<byte[], byte[]> getConsumer() {
        return this.consumer;
    }

    public void setWakeupCalled(boolean wakeupCalled) {
        this.wakeupCalled = wakeupCalled;
    }


    public void run() {
        try {
            synchronized (this.consumer) {
                this.consumer.subscribe(this.topics, new PartitionBalancer(this.produceHandler));
            }

            while (true) {
                // if wakeupCalled flag set to true, throw WakeupException to exit, before that flushing message by producer
                // and offsets committed by consumer will occur.
                if (this.wakeupCalled) {
                    throw new WakeupException();
                }

                ConsumerRecords<byte[], byte[]> records = null;

                synchronized (this.consumer) {
                    records = consumer.poll(this.timeout);
                }

                for (ConsumerRecord<byte[], byte[]> record : records) {
                    String topic = record.topic();
                    int partition = record.partition();
                    byte[] value = record.value();
                    long offset = record.offset();

                    TopicPartition tp = new TopicPartition(topic, partition);

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

        } finally {
            this.produceHandler.flushAndCommit();

            this.consumer.close();
        }
    }
}
