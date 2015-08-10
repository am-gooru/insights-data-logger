package org.ednovo.kafka.consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

public final class ConsumeMessages implements Runnable {

	private Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap;

	private String consumerTopic;

	private DataProcessor rowDataProcessor;

	private static Logger logger = LoggerFactory.getLogger(MessageConsumer.class);

	public ConsumeMessages(String consumerTopic, Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap) {
		this.consumerTopic = consumerTopic;
		this.consumerMap = consumerMap;
	}

	public void run() {
		try {
			logger.info("consumer process started for the topic : {}", consumerTopic);
			KafkaStream<byte[], byte[]> stream = consumerMap.get(consumerTopic).get(0);
			ConsumerIterator<byte[], byte[]> it = stream.iterator();
			/**
			 * process consumed data
			 */
			while (it.hasNext()) {
				String message = null;
				message = new String(it.next().message());
				Gson gson = new Gson();
				Map<String, String> messageMap = new HashMap<String, String>();
				try {
					messageMap = gson.fromJson(message, messageMap.getClass());
				} catch (Exception e) {
					ConsumerLogFactory.errorActivity.error(message);
					continue;
				}

				/**
				 * TODO We're only getting raw data now. We'll have to use the server IP as well for extra information.
				 **/
				if (messageMap != null && !messageMap.isEmpty()) {
					ConsumerLogFactory.activity.info(message);
					this.rowDataProcessor.processRow(messageMap.get("raw"));
				} else {
					ConsumerLogFactory.errorActivity.error(message);
				}
			}

		} catch (Exception e) {
			logger.error("Error while consume messages", e);
		}
	}
}
