package kafka.spiegel;

import com.lmax.disruptor.EventFactory;

/**
 * Created by mykidong on 2016-08-05.
 */
public class SpiegelEvent {

    private String topic;
    private int partition;
    private byte[] value;
    private long offset;


    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public static final EventFactory<SpiegelEvent> FACTORY = new EventFactory<SpiegelEvent>() {
        @Override
        public SpiegelEvent newInstance() {
            return new SpiegelEvent();
        }
    };
}
