package com.tuto.springkafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class SpringKafkaApplication {

	public static void main(String[] args) throws Exception {
		ConfigurableApplicationContext context = SpringApplication.run(SpringKafkaApplication.class, args);

		MessageProducer producer = context.getBean(MessageProducer.class);
		MessageListener listener = context.getBean(MessageListener.class);

		for (int i = 0; i < 10; i++) {
			producer.sendDefaultMessage("This is the sashimi number " + (i + 1));
		}

		listener.latch.await(10, TimeUnit.SECONDS);

		producer.sendEvolvedMessage(new CustomMessage("refNum1", "STARTED"));
		producer.sendEvolvedMessage(new CustomMessage("refNum2", "STARTED"));
		producer.sendEvolvedMessage(new CustomMessage("refNum1", "COMPLETED"));
		producer.sendEvolvedMessage(new CustomMessage("refNum3", "STARTED"));
		producer.sendEvolvedMessage(new CustomMessage("refNum2", "CANCELLED"));
		producer.sendEvolvedMessage(new CustomMessage("refNum3", "COMPLETED"));

		listener.latchAh.await(10, TimeUnit.SECONDS);

		context.close();
	}

	@Bean
	public MessageProducer messageProducer() {
		return new MessageProducer();
	}

	@Bean
	public MessageListener messageListener() {
		return new MessageListener();
	}

	public static class MessageProducer {

		@Autowired
		private KafkaTemplate<String, String> defaultKafkaTemplate;

		@Autowired
		private KafkaTemplate<String, CustomMessage> evolvedKafkaTemplate;

		@Value(value = "${message.topic.name}")
		private String defaultTopic;

		@Value(value = "${message.topic.evolve}")
		private String evolvedTopic;

		public void sendDefaultMessage(String message) {
			ListenableFuture<SendResult<String, String>> future = defaultKafkaTemplate.send(defaultTopic, message);

			future.addCallback(new ListenableFutureCallback<>() {

				@Override
				public void onSuccess(SendResult<String, String> result) {
					System.out.println("Sent message=[" + message + "] with offset=[" + result.getRecordMetadata()
							.offset() + "]");
				}

				@Override
				public void onFailure(Throwable ex) {
					System.out.println("Unable to send message=[" + message + "] due to : " + ex.getMessage());
				}
			});
		}

		public void sendEvolvedMessage(CustomMessage customMessage) {
			ListenableFuture<SendResult<String, CustomMessage>> future = evolvedKafkaTemplate.send(evolvedTopic, customMessage);
			future.addCallback(new ListenableFutureCallback<>() {
					@Override
					public void onSuccess(SendResult<String, CustomMessage> result) {
//						System.out.println(result.toString());
						System.out.println("Sent message=[" + customMessage + "] with offset=[" + result.getRecordMetadata()
								.offset() + "]");
					}

					@Override
					public void onFailure(Throwable ex) {
						System.out.println("Unable to send message=[" + customMessage + "] due to : " + ex.getMessage());
					}
				}
			);
		}

	}

	public static class MessageListener {

		private CountDownLatch latch = new CountDownLatch(3);

		private CountDownLatch latchAh = new CountDownLatch(5);

		// TOPIC tuto

		@KafkaListener(topics = "tuto", groupId = "listenDefault", containerFactory = "defaultKafkaListenerContainerFactory")
		public void listenDefault(@Payload String message, @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
			System.out.println("Received default message: " + message + " from partition: " + partition);
		}

		@KafkaListener(topics = "tuto", groupId = "listenFiltered", containerFactory = "filteredKafkaListenerContainerFactory")
		public void listenFiltered(@Payload String message) {
			System.out.println("Received ONLY 10 message: " + message);
		}

		@KafkaListener(topicPartitions = @TopicPartition(topic = "tuto", partitions = { "0" }),
				topics = "tuto", groupId = "listenPartition0", containerFactory = "defaultKafkaListenerContainerFactory")
		public void listenPartition0(@Payload String message) {
			System.out.println("Received Partition-0 message: " + message);
		}

		@KafkaListener(topicPartitions = @TopicPartition(topic = "tuto", partitions = { "1" }),
				topics = "tuto", groupId = "listenPartition1", containerFactory = "defaultKafkaListenerContainerFactory")
		public void listenPartition1(@Payload String message) {
			System.out.println("Received Partition-1 message: " + message);
		}

		@KafkaListener(topicPartitions = @TopicPartition(topic = "tuto", partitions = { "2" }),
				topics = "tuto", groupId = "listenPartition2", containerFactory = "defaultKafkaListenerContainerFactory")
		public void listenPartition2(@Payload String message) {
			System.out.println("Received Partition-2 message: " + message);
		}

		// TOPIC mymessage

		@KafkaListener(topics = "mymessage", groupId = "listenMessageObject", containerFactory = "evolvedKafkaListenerContainerFactory")
		public void listenMessageObject(CustomMessage customMessage) {
			System.out.println("Received class message: " + customMessage.getItemRef() + " is at status " + customMessage.getMessageType());
			latchAh.countDown();
		}
	}
}
