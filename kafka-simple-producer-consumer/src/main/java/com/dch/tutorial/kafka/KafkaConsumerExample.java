package com.dch.tutorial.kafka;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Simple Kafka Consumer client example.
 * 
 * @author David.Christianto
 */
public class KafkaConsumerExample {

	private final static String TOPIC = "my-failsafe-topic";
	private final static String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";

	/**
	 * Method used to create Kafka Consumer.
	 * 
	 * @return {@link KafkaConsumer}
	 */
	private static Consumer<Long, String> createConsumer() {
		final Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaConsumerExample");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

		// Create the consumer using props.
		final Consumer<Long, String> consumer = new KafkaConsumer<>(props);

		// Subscribe to the topic.
		consumer.subscribe(Collections.singletonList(TOPIC));
		return consumer;
	}

	public static void runConsumer() throws InterruptedException {
		final Consumer<Long, String> consumer = createConsumer();

		final int giveUp = 100;
		int noRecordsCount = 0;
		boolean stopped = false;

		while (!stopped) {
			final ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);

			if (consumerRecords.isEmpty()) {
				noRecordsCount++;
				if (noRecordsCount > giveUp)
					stopped = true;
			} else {
				consumerRecords.forEach(record -> {
					System.out.printf("Consumer Record:(%d, %s, %d, %d)\n", record.key(), record.value(),
							record.partition(), record.offset());
				});
			}
			consumer.commitAsync();
		}
		consumer.close();
		System.out.println("DONE");
	}

	public static void main(String... args) throws Exception {
		runConsumer();
	}
}
