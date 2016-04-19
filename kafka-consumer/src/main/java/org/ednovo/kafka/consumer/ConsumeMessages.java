package org.ednovo.kafka.consumer;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

public final class ConsumeMessages implements Runnable {

	private Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap;

	private String consumerTopic;

	private DataProcessor rowDataProcessor;

	private static Logger LOG = LoggerFactory.getLogger(MessageConsumer.class);

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
			ConsumerIterator<byte[], byte[]> it = stream.iterator();
			/**
			 * process consumed data
			 */
			while (it.hasNext()) {
				String message = null;
				message = new String(it.next().message());
				LOG.info("consume receiving event {}", message);
				this.rowDataProcessor.processRow(message);
			}

		} catch (Exception e) {
			LOG.error("Error while consume messages", e);
		}
	}
}
