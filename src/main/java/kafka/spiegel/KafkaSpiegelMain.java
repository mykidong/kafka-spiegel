package kafka.spiegel;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.*;

/**
 * Created by mykidong on 2016-08-05.
 */
public class KafkaSpiegelMain {

    private static Logger log = LoggerFactory.getLogger(KafkaSpiegelMain.class);

    public static void main(String[] args) throws Exception{

        OptionParser parser = new OptionParser();
        parser.accepts("consumer.props").withRequiredArg().ofType(String.class);
        parser.accepts("producer.props").withRequiredArg().ofType(String.class);
        parser.accepts("topics").withRequiredArg().ofType(String.class);
        parser.accepts("spiegel.props").withRequiredArg().ofType(String.class);

        OptionSet options = parser.parse(args);

        // consumer properties path in classpath.
        String consumerPropPath = (String) options.valueOf("consumer.props");
        Properties consumerProps = loadProperties(consumerPropPath);

        // producer properites path in classpath.
        String producerPropPath = (String) options.valueOf("producer.props");
        Properties producerProps = loadProperties(producerPropPath);

        // topics which are comma seperated.
        String topicLine = (String) options.valueOf("topics");
        List<String> topics = Arrays.asList(topicLine.split(","));

        // kafka spiegel properties path in classpath.
        String spiegelPropPath = (String) options.valueOf("spiegel.props");
        Properties spiegelPropsTemp = loadProperties(spiegelPropPath);

        Map<String, String> spiegelProps = new HashMap<>();
        for(Object key : spiegelPropsTemp.keySet())
        {
            spiegelProps.put((String) key, (String)spiegelPropsTemp.get(key));
        }

        Thread mainThread = Thread.currentThread();

        KafkaSpiegel kafkaSpiegel = new KafkaSpiegel(consumerProps, producerProps, topics, spiegelProps);
        kafkaSpiegel.run();

        // register Message as shutdown hook
        Runtime.getRuntime().addShutdownHook(new ShutdownHookThread(kafkaSpiegel, mainThread));
    }


    private static Properties loadProperties(String propPath)
    {
        if(!propPath.startsWith("/"))
        {
            propPath = "/" + propPath;
        }

        Properties prop = new Properties();
        InputStream is = null;
        try {
            // load properties file from classpath.
            is = new KafkaSpiegelMain().getClass().getResourceAsStream(propPath);
            prop.load(is);
        } catch (Exception e) {
            // if properties file in classpath does not exist, then load it from file path.
            try {
                is = new FileInputStream(propPath);
                prop.load(is);
            }catch (Exception ex)
            {
                throw new RuntimeException(ex);
            }
        }

        return prop;
    }


    private static class ShutdownHookThread extends Thread {
        private KafkaSpiegel kafkaSpiegel;

        private Thread mainThread;

        public ShutdownHookThread(KafkaSpiegel kafkaSpiegel, Thread mainThread) {
            this.kafkaSpiegel = kafkaSpiegel;
            this.mainThread = mainThread;
        }

        public void run() {
            this.kafkaSpiegel.getConsumer().wakeup();

            // to make sure that WakeupException should be thrown before exit.
            this.kafkaSpiegel.setWakeupCalled(true);
            try {
                mainThread.join();
            } catch (InterruptedException ie) {
                ie.printStackTrace();
            }
        }
    }

}
