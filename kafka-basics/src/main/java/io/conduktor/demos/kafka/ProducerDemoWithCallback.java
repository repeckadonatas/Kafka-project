package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

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

        // Setting a smaller batch size before sending multiple batches of messages
        properties.setProperty("batch.size", "400");

        // Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // Sending multiple batches of messages to better demonstrate the behaviour of StickyPartitioner
        for (int j = 0; j < 10; j++){
            // Sending multiple messages very quickly, the Producer uses StickyPartitioner to improve the performance (a batch of messages is sent to the same partition in a topic).
            // Otherwise, the Producer will use Round Robin
            for (int i = 0; i < 30; i++){
                // Create a Producer Record
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>("demo_java", "hello world" + i);


                // Send Data -- asynchronous operation
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        //executes every time a record is successfully sent or an exception is thrown
                        if (e == null) {
                            // the record was successfully sent
                            log.info("Received new metadata \n" +
                                    "Topic: " + metadata.topic() + "\n" +
                                    "Partition: " + metadata.partition() + "\n" +
                                    "Offset: " + metadata.offset() + "\n" +
                                    "Timestamp: " + metadata.timestamp());
                        } else {
                            log.error("Error while producing", e);
                        }
                    }
                });
            }

            // After sending a batch of messages, the Producer will wait 500 ms before sending a new batch of messages
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }


        // Flush and Close the Producer
        // Flush
        // tell the producer to send all data and block until done --synchronous operation
        producer.flush();

        // Close
        // calling .close() will also call .flush() first, but it is possible to call .flush() before if needed
        producer.close();
    }
}
