package kafka.spiegel;

import com.lmax.disruptor.EventHandler;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by mykidong on 2016-08-05.
 */
public class ProduceHandler implements EventHandler<SpiegelEvent>, ProduceController {

    private static Logger log = LoggerFactory.getLogger(ProduceHandler.class);

    private ConcurrentMap<TopicPartition, OffsetAndMetadata> partitionOffsetMap = new ConcurrentHashMap<>();
    private List<SpiegelEvent> events = new ArrayList<>();

    private long interval;
    private long maxEventSize;

    private final Lock lock = new ReentrantLock();

    private KafkaConsumer<byte[], byte[]> consumer;
    private Producer<byte[], byte[]> producer;

    private AtomicLong count = new AtomicLong(0);

    private CustomTimer customTimer;

    public ProduceHandler(KafkaConsumer<byte[], byte[]> consumer, Producer<byte[], byte[]> producer, long interval, long maxEventSize)
    {
        this.consumer = consumer;
        this.producer = producer;
        this.interval = interval;
        this.maxEventSize = maxEventSize;

        // run timer.
        this.customTimer = new CustomTimer(this, this.interval);
        this.customTimer.runTimer();
    }

    @Override
    public void onEvent(SpiegelEvent spiegelEvent, long l, boolean b) throws Exception {
        try
        {
            lock.lock();

            String topic = spiegelEvent.getTopic();
            int partition = spiegelEvent.getPartition();
            long offset = spiegelEvent.getOffset();

            TopicPartition tp = new TopicPartition(topic, partition);

            if(this.partitionOffsetMap.containsKey(tp))
            {
                OffsetAndMetadata lastOffset = this.partitionOffsetMap.get(tp);
                if(lastOffset.offset() < offset)
                {
                    this.partitionOffsetMap.put(tp, new OffsetAndMetadata(offset, ""));
                }
            }
            else
            {
                this.partitionOffsetMap.put(tp, new OffsetAndMetadata(offset, ""));
            }

            this.events.add(spiegelEvent);

            long currentTotalCount = this.count.incrementAndGet();

            if (currentTotalCount >= this.maxEventSize) {
                this.flushAndCommit();
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void flushAndCommit() {
        try {
            lock.lock();

            try {
                if(this.count.get() > 0) {
                    // send messages.
                    for (SpiegelEvent event : events) {
                        this.producer.send(new ProducerRecord<byte[], byte[]>(event.getTopic(), event.getValue())).get();
                    }

                    // producer flush.
                    this.producer.flush();

                    // commit offsets.
                    this.consumer.commitAsync(this.partitionOffsetMap, new OffsetCommitCallback() {
                        @Override
                        public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
                            if(e != null)
                            {
                                log.error(e.getMessage());
                            }
                        }
                    });

                    // reset events list.
                    this.events = new ArrayList<>();

                    // reset partition offset map.
                    this.partitionOffsetMap = new ConcurrentHashMap<>();

                    // reset message count.
                    this.count.set(0);

                    //log.info("messages flushed to destination kafka and offset commited to source kafka...");
                }

            }catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        } finally {
            lock.unlock();
        }
    }

    private static class CustomTimer {
        private CustomTimerTask timerTask;

        private int delay = 1000;

        private long interval;

        private Timer timer;

        private ProduceHandler produceHandler;

        public CustomTimer(ProduceHandler produceHandler,
                           long interval) {
            this.interval = interval;

            this.produceHandler = produceHandler;

            this.timerTask = new CustomTimerTask(this.produceHandler);
            this.timer = new Timer();
        }

        public void runTimer() {
            this.timer.scheduleAtFixedRate(this.timerTask, delay, this.interval);
        }

        public void reset() {
            this.timerTask.cancel();
            this.timer.purge();
            this.timerTask = new CustomTimerTask(this.produceHandler);
        }
    }

    private static class CustomTimerTask extends TimerTask {
        private ProduceHandler produceHandler;

        public CustomTimerTask(ProduceHandler produceHandler) {
            this.produceHandler = produceHandler;
        }

        @Override
        public void run() {
            this.produceHandler.flushAndCommit();
            //log.info("flushEvents called from Timer...");
        }
    }
}
