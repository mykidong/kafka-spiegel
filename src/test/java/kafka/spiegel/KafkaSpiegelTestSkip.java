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
    public void run()
    {
        List<String> argsList = new ArrayList<>();
        argsList.add("--consumer.props");
        argsList.add("props/sourceConsumer.properties");

        argsList.add("--producer.props");
        argsList.add("props/destProducer.properties");

        argsList.add("--topics");
        argsList.add("item-view-event");

        argsList.add("--spiegel.props");
        argsList.add("props/spiegel.properties");

        String[] args = argsList.toArray(new String[0]);

        KafkaSpiegel.main(args);
    }
}
