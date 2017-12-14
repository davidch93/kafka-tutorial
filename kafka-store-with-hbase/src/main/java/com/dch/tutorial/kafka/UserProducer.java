package com.dch.tutorial.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.LongStream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;

import com.dch.tutorial.kafka.model.User;
import com.dch.tutorial.kafka.serializer.UserSerializer;

public class UserProducer {

	private final static String TOPIC = "my-failsafe-topic";
	private final static String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";

	/**
	 * Method used to create Kafka Producer.
	 * 
	 * @return {@link UserProducer}
	 */
	private Producer<Long, User> createProducer() {
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
	 * @param users
	 *            List of user to send.
	 * @throws Exception
	 *             If error occurred when send message.
	 */
	public void runProducerSync(List<User> users) throws Exception {
		final Producer<Long, User> producer = createProducer();
		long time = System.currentTimeMillis();

		users.forEach(user -> {
			try {
				ProducerRecord<Long, User> record = new ProducerRecord<>(TOPIC, user.getId(), user);
				RecordMetadata metadata = producer.send(record).get();

				long elapsedTime = System.currentTimeMillis() - time;
				System.out.printf("sent record(key=%s value=%s) " + "meta(partition=%d, offset=%d) time=%d\n",
						record.key(), record.value(), metadata.partition(), metadata.offset(), elapsedTime);
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
		});

		producer.flush();
		producer.close();
	}

	public static void main(String... args) throws Exception {
		List<User> users = new ArrayList<>();
		long time = System.currentTimeMillis();
		LongStream.range(time, time + 5).forEach(index -> {
			User user = new User();
			user.setId(index);
			user.setFirstName("First " + index);
			user.setLastName("Last " + index);
			user.setEmail("Email " + index);
			users.add(user);
		});

		new UserProducer().runProducerSync(users);
	}
}
