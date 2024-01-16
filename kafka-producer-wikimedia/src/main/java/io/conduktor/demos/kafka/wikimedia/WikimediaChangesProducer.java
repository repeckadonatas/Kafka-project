package io.conduktor.demos.kafka.wikimedia;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {
    public static void main(String[] args) throws InterruptedException {

        //String bootstrapServers = "localhost:9092";

        // Create Producer Properties

        Properties properties = new Properties();

        // Connect to bootstrap server (Conduktor in this case)
        properties.setProperty("bootstrap.servers", "suited-raptor-8496-eu2-kafka.upstash.io:9092");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule " +
                                                    "required " +
                                                    "username='c3VpdGVkLXJhcHRvci04NDk2JHvIFDYf-8ofZH0GOgHPakr3DfUuvvxXsOrVq44' " +
                                                    "password='NjNjNmU4NTgtNzczYi00OTMzLWIwYTQtNDRiOGRiYzQxZDQ0';");
        properties.setProperty("sasl.mechanism", "SCRAM-SHA-256");

        //properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        // Set safe producer configs (Kafka <= 2.8)
        //properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        //properties.setProperty(ProducerConfig.ACKS_CONFIG, "all"); // same as setting -1
        //properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));


        // Set high throughput Producer configs (making a Producer much more efficient)
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024)); //32 bit batch size
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");


        // Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        String topic = "wikimedia.recentchange"; //Name of the stream

        // Event handler allows to handle the events coming from the stream and send them to the Producer
        BackgroundEventHandler eventHandler = new WikimediaChangeHandler(producer,topic);

        String url = "https://stream.wikimedia.org/v2/stream/recentchange";

        // Building an Event Source to take stream data from url
        EventSource.Builder builder = new EventSource.Builder(URI.create(url));
        BackgroundEventSource eventSource = new BackgroundEventSource.Builder(eventHandler, builder).build();

        // Start the producer in another thread
        eventSource.start();    // Allows to start the EventHandler

        // producer for 10 minutes and block the program until then
        TimeUnit.MINUTES.sleep(10);

    }
}
