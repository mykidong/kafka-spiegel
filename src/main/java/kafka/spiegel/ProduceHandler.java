package kafka.spiegel;

import com.lmax.disruptor.EventHandler;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;

/**
 * Created by mykidong on 2016-08-05.
 */
public class ProduceHandler implements EventHandler<SpiegelEvent>, ProduceController {

    private KafkaConsumer<byte[], byte[]> consumer;
    private Producer<byte[], byte[]> producer;

    public void setConsumer(KafkaConsumer<byte[], byte[]> consumer) {
        this.consumer = consumer;
    }

    public void setProducer(Producer<byte[], byte[]> producer) {
        this.producer = producer;
    }

    @Override
    public void onEvent(SpiegelEvent spiegelEvent, long l, boolean b) throws Exception {

    }

    @Override
    public void flushAndCommit() {

    }
}
