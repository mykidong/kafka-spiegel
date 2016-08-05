package kafka.spiegel;

import com.lmax.disruptor.EventTranslator;

/**
 * Created by mykidong on 2016-08-05.
 */
public class SpiegelEventTranslator implements EventTranslator<SpiegelEvent> {

    private String topic;
    private int partition;
    private byte[] value;
    private long offset;

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    @Override
    public void translateTo(SpiegelEvent spiegelEvent, long l) {
        spiegelEvent.setTopic(this.topic);
        spiegelEvent.setPartition(this.partition);
        spiegelEvent.setValue(this.value);
        spiegelEvent.setOffset(this.offset);
    }
}
