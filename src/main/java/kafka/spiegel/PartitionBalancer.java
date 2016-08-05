package kafka.spiegel;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * Created by mykidong on 2016-08-05.
 */
public class PartitionBalancer implements ConsumerRebalanceListener{
    private static Logger log = LoggerFactory.getLogger(PartitionBalancer.class);

    private ProduceController produceController;

    public PartitionBalancer(ProduceController produceController)
    {
        this.produceController = produceController;
    }


    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {
        this.produceController.flushAndCommit();
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {
        for(TopicPartition tp : collection)
        {
            String topic = tp.topic();
            int partition = tp.partition();

            log.info("new partition assigned: topic [{}], parition [{}]", topic, partition);
        }
    }
}
