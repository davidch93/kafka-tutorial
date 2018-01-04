package com.dch.tutorial.kafka;

import com.dch.tutorial.kafka.deserializer.UserDeserializer;
import com.dch.tutorial.kafka.hbase.HBaseClientOperations;
import com.dch.tutorial.kafka.model.User;
import org.apache.hadoop.hbase.TableName;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Consumer that consume user data and store into HBase.
 *
 * @author David.Christianto
 */
public class UserConsumer {

    private final static String TOPIC = "my-failsafe-topic";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";

    public static void main(String... args) throws Exception {
        new UserConsumer().runConsumer();
    }

    /**
     * Method used to create Kafka Consumer.
     *
     * @return {@link KafkaConsumer}
     */
    private Consumer<Long, User> createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "UserConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, UserDeserializer.class.getName());

        // Create the consumer using props.
        final Consumer<Long, User> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC));
        return consumer;
    }

    /**
     * Method used to run Kafka Consumer.
     *
     * @throws IOException If error occurred when Kafka failed to consume data.
     */
    public void runConsumer() throws IOException {
        final HBaseClientOperations clientOperations = new HBaseClientOperations();
        clientOperations.initHBase(clientOperations);

        final Consumer<Long, User> consumer = createConsumer();
        final int giveUp = 100;
        int noRecordsCount = 0;
        boolean stopped = false;

        while (!stopped) {
            final ConsumerRecords<Long, User> consumerRecords = consumer.poll(1000);

            if (consumerRecords.isEmpty()) {
                noRecordsCount++;
                if (noRecordsCount > giveUp)
                    stopped = true;
            } else {
                List<User> users = new ArrayList<>();
                consumerRecords.forEach(record -> {
                    System.out.printf("Consumer Record:(%d, %s, %d, %d)\n", record.key(), record.value().toString(),
                            record.partition(), record.offset());
                    users.add(record.value());
                });
                clientOperations.put(TableName.valueOf(HBaseClientOperations.TABLE_NAME), users);
                clientOperations.scan(TableName.valueOf(HBaseClientOperations.TABLE_NAME))
                        .forEach(user -> System.out.println("-----" + user.toString()));
            }
            consumer.commitAsync();
        }
        clientOperations.deleteTable(TableName.valueOf(HBaseClientOperations.TABLE_NAME));
        consumer.close();
        System.out.println("DONE");
    }
}
