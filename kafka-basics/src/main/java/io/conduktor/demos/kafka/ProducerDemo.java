package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a Kafka Producer!");

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

         // Set Producer Properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // Create a Producer Record
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>("demo_java", "hello world");


        // Send Data -- asynchronous operation
        producer.send(producerRecord);

        // Flush and Close the Producer
        // Flush
        // tell the producer to send all data and block until done --synchronous operation
        producer.flush();

        // Close
        // calling .close() will also call .flush() first, but it is possible to call .flush() before if needed
        producer.close();
    }
}
