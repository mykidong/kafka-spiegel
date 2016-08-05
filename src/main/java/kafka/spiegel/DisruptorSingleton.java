package kafka.spiegel;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

/**
 * Created by mykidong on 2016-08-05.
 */
public class DisruptorSingleton {

    private static Disruptor disruptor = null;

    private static final Object lock = new Object();

    public static <T> Disruptor getInstance(int bufferSize, EventHandler<T>... handlers)
    {
        if(disruptor == null) {
            synchronized(lock) {
                if(disruptor == null) {
                    disruptor = new Disruptor(SpiegelEvent.FACTORY,
                            bufferSize,
                            Executors.newCachedThreadPool(),
                            ProducerType.SINGLE, // Single producer
                            new BlockingWaitStrategy());

                    disruptor.handleEventsWith(handlers);

                    disruptor.start();
                }
            }
        }

        return disruptor;
    }

}
