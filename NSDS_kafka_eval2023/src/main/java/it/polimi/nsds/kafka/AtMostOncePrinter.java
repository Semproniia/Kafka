package it.polimi.nsds.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.metrics.internals.IntGaugeSuite;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Properties;

public class AtMostOncePrinter {
    private static final String topic = "inputTopic";
    private static final String serverAddr = "localhost:9092";
    private static final String outputTopic = "outputTopic";
    private static final int threshold = 500;

    public static void main(String[] args) {
        //String groupId = args[0];
        String groupId = "atMostOncePrinterGroup";

        //consumer
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(false));


        KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));


        while (true) {
            final ConsumerRecords<String, Integer> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES));
            for (final ConsumerRecord<String, Integer> record : records) {
                consumer.commitSync();
                if(record.value() > threshold){
                    System.out.println("OUTPUT1: " +
                    "\tKey: " + record.key() +
                    "\tValue: " + record.value()
                );
                }   
            }
    // TODO: complete the implementation by adding code here

        }
    }
}
