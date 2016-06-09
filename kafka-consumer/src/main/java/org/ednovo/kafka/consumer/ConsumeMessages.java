package org.ednovo.kafka.consumer;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

public final class ConsumeMessages implements Runnable {

	private final Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap;

	private final String consumerTopic;

	private final DataProcessor rowDataProcessor;

	private static final Logger LOG = LoggerFactory.getLogger(MessageConsumer.class);

	public ConsumeMessages(String consumerTopic, Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap,
			DataProcessor rowDataProcessor) {
		this.consumerTopic = consumerTopic;
		this.consumerMap = consumerMap;
		this.rowDataProcessor = rowDataProcessor;
	}

	public void run() {
		try {
			LOG.info("consumer process started for the topic : {}", consumerTopic);
			KafkaStream<byte[], byte[]> stream = consumerMap.get(consumerTopic).get(0);
			/**
			 * process consumed data
			 */
			for (MessageAndMetadata<byte[], byte[]> aStream : stream) {
				String message = null;
				message = new String(aStream.message());
				LOG.info("consume receiving event {}", message);
				this.rowDataProcessor.processRow(message);
			}

		} catch (Exception e) {
			LOG.error("Error while consume messages", e);
		}
	}
}
