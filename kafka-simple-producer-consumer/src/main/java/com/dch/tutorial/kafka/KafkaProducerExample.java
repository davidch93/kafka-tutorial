package com.dch.tutorial.kafka;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Simple Kafka Producer client example.
 * 
 * @author David.Christianto
 */
public class KafkaProducerExample {

	private final static String TOPIC = "my-failsafe-topic";
	private final static String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";

	/**
	 * Method used to create Kafka Producer.
	 * 
	 * @return {@link KafkaProducer}
	 */
	private Producer<Long, String> createProducer() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaProducerExample");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
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
	public void runProducerSync(final int sendMessageCount) throws Exception {
		final Producer<Long, String> producer = createProducer();
		long time = System.currentTimeMillis();

		try {
			for (long index = time; index < time + sendMessageCount; index++) {
				final ProducerRecord<Long, String> record = new ProducerRecord<>(TOPIC, index, "[Sync] Hello " + index);

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

	/**
	 * Method used to execute Kafka Producer, send asynchronous message to
	 * destination topic.
	 * <p>
	 * Kafka defines a Callback interface that you use for asynchronous operations.
	 * The callback interface allows code to execute when the request is complete.
	 * The async send method is used to send a record to a topic, and the provided
	 * callback gets called when the send is acknowledged. The send method is
	 * asynchronous, and when called returns immediately once the record gets stored
	 * in the buffer of records waiting to post to the Kafka broker. The send method
	 * allows sending many records in parallel without blocking to wait for the
	 * response after each one.
	 * </p>
	 * 
	 * @param sendMessageCount
	 *            Number of message to send.
	 * @throws InterruptedException
	 *             If error occurred when send message.
	 */
	public void runProducerAsync(final int sendMessageCount) throws InterruptedException {
		final Producer<Long, String> producer = createProducer();
		long time = System.currentTimeMillis();
		final CountDownLatch countDownLatch = new CountDownLatch(sendMessageCount);

		try {
			for (long index = time; index < time + sendMessageCount; index++) {
				final ProducerRecord<Long, String> record = new ProducerRecord<>(TOPIC, index,
						"[Async] Hello " + index);

				producer.send(record, (metadata, exception) -> {
					long elapsedTime = System.currentTimeMillis() - time;
					if (metadata != null)
						System.out.printf("sent record(key=%s value=%s) " + "meta(partition=%d, offset=%d) time=%d\n",
								record.key(), record.value(), metadata.partition(), metadata.offset(), elapsedTime);
					else
						exception.printStackTrace();

					countDownLatch.countDown();
				});
			}
			countDownLatch.await(25, TimeUnit.SECONDS);
		} finally {
			producer.flush();
			producer.close();
		}
	}

	public static void main(String... args) throws Exception {
		new KafkaProducerExample().runProducerSync(5);
		new KafkaProducerExample().runProducerAsync(5);
	}
}
