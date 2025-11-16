package it.polimi.nsds.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class PopularTopicConsumer {
    private static final String topic1 = "inputTopic1";
    private static final String topic2 = "inputTopic2";
    private static final String topic3 = "inputTopic3";

    private static final String serverAddr = "localhost:9092";

    public static void main(String[] args) {
        //String groupId = args[0];
        String groupId = "popularTopicConsumerGroup";

        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(List.of(topic1, topic2, topic3));

        final Map<String, Integer> topicCountMap = new HashMap<>();

        try {
            while (true) {
                final ConsumerRecords<String, Integer> records = consumer.poll(Duration.ofMillis(100));
                    for (final ConsumerRecord<String, Integer> record : records) {

                        // Stampa il messaggio ricevuto
                        System.out.println("Received: <" + record.key() + ", " + record.value() + ", " + record.topic() + ">");

                        // Aggiorniamo il conteggio dei messaggi per il topic del record ricevuto
                        topicCountMap.put(record.topic(), topicCountMap.getOrDefault(record.topic(), 0) + 1);

                        // Troviamo i topic più popolari, ossia quelli con il conteggio massimo
                        int maxCount = Collections.max(topicCountMap.values());
                            List<String> popularTopics = new ArrayList<>();
                            for (Map.Entry<String, Integer> entry : topicCountMap.entrySet()) {
                                if (entry.getValue() == maxCount) {
                                    popularTopics.add(entry.getKey());
                                }
                            }

                            System.out.println("Popular topic(s): " + popularTopics + " with count: " + maxCount);
                        }
                    }
                } finally {
                    consumer.close();
                }
            }


    }

