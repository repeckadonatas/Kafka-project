package io.conduktor.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoWithShutdown {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a Kafka Consumer!");

        String groupID = "my-java-application";

        // A topic to consume from
        String topic = "demo_java";

        // Create Producer Properties
        Properties properties = new Properties();

        // connect to localhost
//        properties.setProperty("bootstrap.servers", "localhost:9092");

        // connect to Conduktor Playground (setup Conduktor first!!!)
        properties.setProperty("bootstrap.servers", "suited-raptor-8496-eu2-kafka.upstash.io:9092");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule " +
                                                    "required " +
                                                    "username='c3VpdGVkLXJhcHRvci04NDk2JHvIFDYf-8ofZH0GOgHPakr3DfUuvvxXsOrVq44' " +
                                                    "password='NjNjNmU4NTgtNzczYi00OTMzLWIwYTQtNDRiOGRiYzQxZDQ0';");
        properties.setProperty("sasl.mechanism", "SCRAM-SHA-256");

        // Create Consumer config
        // Deserializer depends on the typo of data that is being sent from the Producer (String, Avro, JSON, etc.)
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        // Setting a GroupID for our Consumer group
        // group.id property allows consumers in a group to resume at the right offset
        properties.setProperty("group.id", groupID);

        // Setting a parameter to choose from which Consumer group to read the messages (earliest/latest)
        // To read the entire history of the topic, choose "earliest"
        properties.setProperty("auto.offset.reset", "earliest");

        // Create a Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);


        // To gracefully shutdown Kafka Consumer using Shutdown hook
        // Shutdown hook
        // Get a reference to the main Thread
        final Thread mainThread = Thread.currentThread();

        // Adding a Shutdown Hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected a shutdown, let's begin by calling consumer.wakeup()...");
                consumer.wakeup();

                // Join the main thread to allow of execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });


        // Subscribe to a topic
        // A Consumer can read from multiple topics ("topic1", "topic2", etc.)
        // Placing subscription in a try/catch for error and exception handling
        try {
            consumer.subscribe(Arrays.asList(topic));

            // Poll for data
            while (true) {

                log.info("Polling");

                // Reading messages of the topic
                // Setting up a wait time in between the messages in case the messages stop coming as to not overload kafka
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                // Iterating over a collection of records that was polled
                for (ConsumerRecord<String, String> record: records) {
                    log.info("Key: " + record.key() + ", Value: " + record.value());
                    log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                }
            }
        } catch (WakeupException e) {
            log.info("Consumer is starting to shut down");
        } catch (Exception e) {
            log.error("Unexpected exception in the consumer", e);
        } finally {
            consumer.close(); // Close the consumer. This will also commit offsets
            log.info("The Consumer is now gracefully shut down");
        }
    }
}
