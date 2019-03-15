package kafka.spiegel;

import org.apache.log4j.xml.DOMConfigurator;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by mykidong on 2016-08-05.
 */
public class KafkaSpiegelTestSkip {

    private static Logger log;

    @Before
    public void init()
    {
        java.net.URL url = new KafkaSpiegelTestSkip().getClass().getResource("/log4j-test.xml");
        System.out.println("log4j url: " + url.toString());

        DOMConfigurator.configure(url);

        log = LoggerFactory.getLogger(KafkaSpiegelTestSkip.class);
    }


    @Test
    public void run() throws Exception
    {
        String consumerProp = System.getProperty("consumerProp", "props/sourceConsumer.properties");
        String producerProp = System.getProperty("producerProp", "props/destProducer.properties");
        String spiegelProp = System.getProperty("spiegelProp", "props/spiegel.properties");
        String topics = System.getProperty("topics", "item-view-event");

        List<String> argsList = new ArrayList<>();
        argsList.add("--consumer.props");
        argsList.add(consumerProp);

        argsList.add("--producer.props");
        argsList.add(producerProp);

        argsList.add("--topics");
        argsList.add(topics);

        argsList.add("--spiegel.props");
        argsList.add(spiegelProp);

        String[] args = argsList.toArray(new String[0]);

        // java kafka.spiegel.KafkaSpiegelMain --consumer.props props/sourceConsumer.properties \
        //                                     --producer.props props/destProducer.properties \
        //                                     --topics item-view-event \
        //                                     --spiegel.props props/spiegel.properties

        KafkaSpiegelMain.main(args);
    }
}
