package it.polimi.nsds.kafka.eval;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.*;

// Group number:
// Group members:

// Is it possible to have more than one partition for topics "sensors1" and "sensors2"?

// Is there any relation between the number of partitions in "sensors1" and "sensors2"?

// Is it possible to have more than one instance of Merger?
// If so, what is the relation between their group id?

// Is it possible to have more than one partition for topic "merged"?

// Is it possible to have more than one instance of Validator?
// If so, what is the relation between their group id?

public class Consumers51 {
    public static void main(String[] args) {
        String serverAddr = "localhost:9092";
        int stage = Integer.parseInt(args[0]);
        String groupId = args[1];
        // TODO: add arguments if necessary
        switch (stage) {
            case 0:
                new Merger(serverAddr, groupId).execute();
                break;
            case 1:
                new Validator(serverAddr, groupId).execute();
                break;
            case 2:
                System.err.println("Wrong stage");
        }
    }

    private static class Merger {
        private final String serverAddr;
        private final String consumerGroupId;

        // TODO: add attributes if necessary

        // TODO: add arguments if necessary
        public Merger(String serverAddr, String consumerGroupId) {
            this.serverAddr = serverAddr;
            this.consumerGroupId = consumerGroupId;
        }

        public void execute() {
            // Consumer
            final Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);

            // TODO: add properties if needed

            KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(consumerProps);

            // Producer
            final Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);

            // TODO: add properties if needed

            final KafkaProducer<String, Integer> producer = new KafkaProducer<>(producerProps);

            // TODO: implement the processing logic
            while (true) {
                // TODO: implement the processing logic
            }
        }
    }

    private static class Validator {
        private final String serverAddr;
        private final String consumerGroupId;

        // TODO: add attributes if needed

        public Validator(String serverAddr, String consumerGroupId) {
            this.serverAddr = serverAddr;
            this.consumerGroupId = consumerGroupId;
        }

        public void execute() {
            // Consumer
            final Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);

            // TODO: add properties if needed

            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);

            // Producer
            final Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);

            // TODO: add properties if needed

            final KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

            // TODO: implement the processing logic
            while (true) {
                // TODO: implement the processing logic
            }
        }
    }
}