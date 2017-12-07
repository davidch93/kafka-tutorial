package com.dch.tutorial.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;

import com.dch.tutorial.kafka.model.User;
import com.dch.tutorial.kafka.serializer.UserSerializer;

public class KafkaProducerWithObjectExample {

	private final static String TOPIC = "my-failsafe-topic";
	private final static String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";

	/**
	 * Method used to create Kafka Producer.
	 * 
	 * @return {@link KafkaProducer}
	 */
	private static Producer<Long, User> createProducer() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaProducerExample");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, UserSerializer.class.getName());
		return new KafkaProducer<>(props);
	}

	/**
	 * Method used to execute Kafka Producer, send synchronous message to
	 * destination topic.
	 * 
	 * @param sendMessageCount
	 *            Number of message to send.
	 * @throws Exception
	 *             If error occurred when send message.
	 */
	public static void runProducerSync(final int sendMessageCount) throws Exception {
		final Producer<Long, User> producer = createProducer();
		long time = System.currentTimeMillis();

		try {
			for (long index = time; index < time + sendMessageCount; index++) {
				User user = new User();
				user.setId(index);
				user.setName("[Sync] Hello " + index);

				final ProducerRecord<Long, User> record = new ProducerRecord<>(TOPIC, index, user);

				RecordMetadata metadata = producer.send(record).get();

				long elapsedTime = System.currentTimeMillis() - time;
				System.out.printf("sent record(key=%s value=%s) " + "meta(partition=%d, offset=%d) time=%d\n",
						record.key(), record.value(), metadata.partition(), metadata.offset(), elapsedTime);
			}
		} finally {
			producer.flush();
			producer.close();
		}
	}

	public static void main(String... args) throws Exception {
		runProducerSync(5);
	}
}
