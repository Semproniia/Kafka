package it.polimi.nsds.kafka.eval;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.security.ProtectionDomain;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;

import javax.xml.validation.Validator;

// Group number: 51
// Group members: Sara Imparato, Simone Cosenza, Matteo Gugliotta

// Is it possible to have more than one partition for topics "sensors1" and "sensors2"?
    /*
    * Yes, sensor1 and sensor2 can have more than one partition.
    * (in the hypotesis that we have only one instance of Merger):
    * inside Merger, we use single consumer that subscribes to both topics...
    * => so it can receive data from any number of partitions...
    * => simply, merger will read all the partition, because he is the only consumer!
    * (see next answer for the case of multiple instances of Merger)
    */

// Is there any relation between the number of partitions in "sensors1" and "sensors2"?
    /*
    * IF we have only one instance of merger, the answer is:
    * - no, there is no relation between the number of partitions in sensors1 and sensors2.
    * Merger will read all the partitions of both topics, independently from their number. 
    * IF we have more instances of merger, the answer is:
    * - yes, there must be the same number of partitions in sensors1 and sensors2.
    * This ensures that messages with the same key from both topics
    * will be sent to the same instance of Merger,
    * allowing correct merging of values for each key.
    */

// Is it possible to have more than one instance of Merger?
    /*
     * Yes, it is possible to have more than one instance of Merger.
     */

// If so, what is the relation between their group id?
    /*
     * If we have more than one instance of Merger, they must share the same group id.
     * This is because they must cooperate to read all the partitions of sensors1 and sensors2.
     * If, in contrast, they had different group ids,
     * each instance would read all the partitions of both topics, 
     * and it would lead to duplicated processing of the same messages.
     */

// Is it possible to have more than one partition for topic "merged"?
    /*
     * Yes, it is possible to have more than one partition for topic "merged".
     * Because the Merger can be implemented with multiple instances,
     * so each instance can write to a different partition of topic "merged".
     * Consequently, the Validator(s) can read from multiple partitions of topic "merged".
     */

// Is it possible to have more than one instance of Validator?
    /*
     * Technically yes, it is possible to have more than one instance of Validator,
     * but Validators should have different transactionalID.
     */

// If so, what is the relation between their group id?
    /*
     * If we have more than one instance of Validator, they must share the same group id.
     * This is because they must cooperate to read all the partitions of topic "merged".
     * If, in contrast, they had different group ids,
     * each instance would read all the partitions of topic "merged", 
     * and it would lead to duplicated processing of the same messages.
     */

public class Consumers51 {
    public static void main(String[] args) {
        String serverAddr = "localhost:9092";
        String groupId = "groupId";
        //int stage = Integer.parseInt(args[0]);
        //String groupId = args[1];
        // TODO: add arguments if necessary
        /*switch (stage) {
            case 0:
                new Merger(serverAddr, groupId).execute();
                break;
            case 1:
                new Validator(serverAddr, groupId).execute();
                break;
            case 2:
                System.err.println("Wrong stage");
        }*/
        Merger merger = new Merger(serverAddr, groupId);
        Validator validator = new Validator(serverAddr, groupId);
        new Thread(merger::execute).start();
        new Thread(validator::execute).start();
    }

    private static class Merger {
        private final String serverAddr;
        private final String consumerGroupId;
        //defining 2 topics to match
        private static final String topic1 = "sensors1";
        private static final String topic2 = "sensors2";

        // TODO: add attributes if necessary
        //we use 2 hash map for each sensor to keep track of their last value read for a given key
        private HashMap<String, Integer> sensor1Data;
        private HashMap<String, Integer> sensor2Data;
        private String outputTopic = "merged";

        // TODO: add arguments if necessary
        public Merger(String serverAddr, String consumerGroupId) {
            this.serverAddr = serverAddr;
            this.consumerGroupId = consumerGroupId;
            sensor1Data = new HashMap<>();
            sensor2Data = new HashMap<>();
        }

        public void execute() {
            // Consumer
            final Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
            //added properties for consumner to deserialize
            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
            //to achieve at least once, we have to desable auto commit, and do it manually
            consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(false));
            // TODO: add properties if needed

            KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(consumerProps);
            //merger will subscribe to both topic1 and topic2
            consumer.subscribe(List.of(topic1, topic2));
            // Producer
            final Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
            //added serializer properties to the producer part
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());

            // TODO: add properties if needed

            final KafkaProducer<String, Integer> producer = new KafkaProducer<>(producerProps);

            // TODO: implement the processing logic
            while (true) {
                //to reduce duplicate we batch an arbitray number of records being pulled, in our case we chose 10
                List<ConsumerRecord<String, Integer>> batch = new ArrayList<>();
                int batchSize = 10;
                while(batch.size() < batchSize){
                    final ConsumerRecords<String, Integer> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES));
                    for(ConsumerRecord<String, Integer> r : records){
                        batch.add(r);
                    }

                }
                //we batched 10 messages, now we can process them and commit after
                for (ConsumerRecord<String, Integer> r : batch) {
                    //based on the match of the topic
                    if(topic1.equals(r.topic())){
                        //for topic 1, we update the value of that key with the new value just read
                        sensor1Data.put(r.key(),r.value());
                        //beacuse we need to compute the sum, if the other map does not have a value for that key, we add that with an initial 0 value
                        if(!sensor2Data.containsKey(r.key())){
                            sensor2Data.put(r.key(), 0);
                        }
                        //same for topic 2
                    }else if(topic2.equals(r.topic())){
                        sensor2Data.put(r.key(),r.value());
                        if(!sensor1Data.containsKey(r.key())){
                            sensor1Data.put(r.key(), 0);
                        }
                    }
                    //just to make the code more clear, we compute the currentSum apart
                    int currentSum = sensor1Data.get(r.key()) + sensor2Data.get(r.key());
                    //we produce the new record on the output topic, for that key, with the sum computed
                    producer.send(new ProducerRecord<>(outputTopic, r.key(), currentSum));
                    /*System.out.println(
                        "MERGED: " +
                        "\tTopic: " + outputTopic +
                        "\tKey: " + r.key() +
                        "\tValue: " + currentSum
                );*/
                //after processing the message, we commit, this will ensure the at least once semantic
                consumer.commitSync();  
                }
                // TODO: implement the processing logic
            }
        }
    }

    private static class Validator {
        private final String serverAddr;
        private final String consumerGroupId;

        private static final String defaultInputTopic = "merged";
        private static final String defaultOutputTopic1 = "outputTopic1";
        private static final String defaultOutputTopic2 = "outputTopic2";


        private static final String producerTransactionalId = "validatorProducerId";

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
            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
            
            // The consumer does not commit automatically, but within the producer transaction
            consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(false));
            consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
            //fine delle nuove proprietà

            KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(consumerProps);

            //consumer will subscribe only to "merged" topic
            String inputTopic = defaultInputTopic;
            consumer.subscribe(Collections.singletonList(inputTopic));


            // Producer
            final Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);

            // TODO: add properties if needed
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());

            //per l'atomicità aggiungiamo le proprietà:
            producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, producerTransactionalId);
            producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, String.valueOf(true));


            //creazione del producer(prof)
            final KafkaProducer<String, Integer> producer = new KafkaProducer<>(producerProps);

            producer.initTransactions();

            // TODO: implement the processing logic
            while (true) {
                // TODO: implement the processing logic
                final ConsumerRecords<String, Integer> records = consumer.poll(Duration.ofMillis(100));

                producer.beginTransaction();

                for (final ConsumerRecord<String, Integer> record : records) {
                    
                    //Stampiamo ciò che riceviamo
                    final String key = record.key();
                    final Integer value = record.value();
                    System.out.println(
                            "....SONO IL VALIDATORE E HO RICEVUTO.... - " +
                            "Key: " + key +
                                    "\tValue: " + value
                    );

                    //invio, atomicamente, i messaggi ai due topic outputTopic1 e outputTopic2
                    producer.send(new ProducerRecord<>(defaultOutputTopic1, key, value));
                    producer.send(new ProducerRecord<>(defaultOutputTopic2, key, value));

                } //graffa fine for

                // The producer manually commits the offsets for the consumer within the transaction
                final Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
                for (final TopicPartition partition : records.partitions()) {
                    final List<ConsumerRecord<String, Integer>> partitionRecords = records.records(partition);
                    final long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    map.put(partition, new OffsetAndMetadata(lastOffset + 1));
                }


                producer.sendOffsetsToTransaction(map, consumer.groupMetadata());
                producer.commitTransaction();
            } //graffa fine while

        } //graffa fine execute
    } //graffa fine classe Validator
}