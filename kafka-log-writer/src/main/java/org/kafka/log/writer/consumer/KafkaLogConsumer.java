/*******************************************************************************
 * KafkaLogConsumer.java
 * insights-event-logger
 * Created by Gooru on 2014
 * Copyright (c) 2014 Gooru. All rights reserved.
 * http://www.goorulearning.org/
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 ******************************************************************************/
package org.kafka.log.writer.consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import org.kafka.event.microaggregator.core.CassandraConnectionProvider;
import org.kafka.event.microaggregator.core.Constants;
import org.kafka.event.microaggregator.core.MicroAggregationLoader;
import org.kafka.event.microaggregator.dao.AggregationDAOImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.netflix.astyanax.model.ColumnList;


public class KafkaLogConsumer extends Thread implements Runnable,Constants{

	private MicroAggregationLoader microAggregationLoader;
	private final ConsumerConnector consumer;
	private AggregationDAOImpl aggregationDAOImpl;
	private static String topic;

	private static String ZOOKEEPER_IP;
	private static String ZOOKEEPER_PORT;
	private static String KAFKA_GROUPID;
	private static String KAFKA_FILE_TOPIC;
	private static String KAFKA_FILE_ERROR_TOPIC;
	
	private static Logger logger = LoggerFactory.getLogger(KafkaLogConsumer.class);

	public KafkaLogConsumer() {
		microAggregationLoader = new MicroAggregationLoader();
		Map<String, String> kafkaProperty = new HashMap<String, String>();
		kafkaProperty = microAggregationLoader.getKafkaProperty("v2~kafka~logwritter~consumer");
		ZOOKEEPER_IP = kafkaProperty.get("zookeeper_ip");
		ZOOKEEPER_PORT = kafkaProperty.get("zookeeper_portno");
		KAFKA_FILE_TOPIC = kafkaProperty.get("kafka_topic");
		KAFKA_GROUPID = kafkaProperty.get("kafka_groupid");
		KAFKA_FILE_ERROR_TOPIC = "error-" + KAFKA_FILE_TOPIC;
		this.topic = KAFKA_FILE_TOPIC;

		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig());
	}

	 public static String buildEndPoint(String ip, String portNo){
			
			StringBuffer stringBuffer  = new StringBuffer();
			String[] ips = ip.split(",");
			String[] ports = portNo.split(",");
			for( int count = 0; count<ips.length; count++){
				
				if(stringBuffer.length() > 0){
					stringBuffer.append(",");
				}
				
				if(count < ports.length){
					stringBuffer.append(ips[count]+":"+ports[count]);
				}else{
					stringBuffer.append(ips[count]+":"+ports[0]);
				}
			}
			return stringBuffer.toString();
		}
	 
	private static ConsumerConfig createConsumerConfig() {
		Properties props = new Properties();

		props.put("zookeeper.connect", KafkaLogConsumer.buildEndPoint(ZOOKEEPER_IP, ZOOKEEPER_PORT));
		props.put("group.id", KAFKA_GROUPID);
		props.put("zookeeper.session.timeout.ms", "10000");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");
		logger.info("Kafka File writer consumer config: " + ZOOKEEPER_IP + ":" + ZOOKEEPER_PORT + "::" + topic + "::" + KAFKA_GROUPID);

		return new ConsumerConfig(props);

	}

	public void run() {

		consumedData();
	}

	void consumedData() {
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		Integer noOfThread = 1;
		int loopCount = 0, status = 1, mailLoopCount;
		int targetCount = 10;
		long sleepTime = 0;
		aggregationDAOImpl = new AggregationDAOImpl(new CassandraConnectionProvider());
		aggregationDAOImpl.putValueByType(columnFamily.JOB_TRACKER.columnFamily(), Constants.MONITOR_KAFKA_LOG_CONSUMER, Constants.STATUS, status);
		topicCountMap.put(topic, noOfThread);
		/**
		 * Iterate the loop for few times till the kafka get reconnected
		 */
		while (loopCount < targetCount) {
			try {
				/**
				 * Get config settings for kafka consumer
				 */
				ColumnList<String> columnList = aggregationDAOImpl.readRow(columnFamily.JOB_TRACKER.columnFamily(), Constants.MONITOR_KAFKA_LOG_CONSUMER, null).getResult();
				status = columnList.getIntegerValue(Constants.STATUS, 1);
				mailLoopCount = columnList.getIntegerValue(Constants.MAIL_LOOP_COUNT, 10);
				targetCount = columnList.getIntegerValue(Constants.THREAD_LOOP_COUNT, 10);
				sleepTime = columnList.getIntegerValue(Constants.THREAD_SLEEP_TIME, 10000);

				/**
				 * will kill the thread,if the status is 0
				 */
				if (status == 0) {
					return;
				}

				/**
				 * will send the mail to developer for kafka failure
				 * notification
				 */
				if (mailLoopCount != 0 && (loopCount % mailLoopCount) == 0) {
					logger.info("mail sending logic");
				}

				Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
				KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
				ConsumerIterator<byte[], byte[]> it = stream.iterator();
				while (it.hasNext()) {
					String message = new String(it.next().message());
					Gson gson = new Gson();
					Map<String, String> messageMap = new HashMap<String, String>();
					try {
						messageMap = gson.fromJson(message, messageMap.getClass());

					} catch (Exception e) {
						LogWritterFactory.errorActivity.error(message);
						continue;
					}

					// TODO We're only getting raw data now. We'll have to use
					// the
					// server IP as well for extra information.
					if (messageMap != null && !messageMap.isEmpty()) {
						// Write the consumed JSON to Log file.
						LogWritterFactory.activity.info(message);
						String eventJson = (String) messageMap.get("raw");
					} else {
						LogWritterFactory.errorActivity.error(message);
						continue;
					}
				}
			} catch (Exception e) {
				logger.error("Message Log Consumer:" + e);
			} finally {
				try {
					Thread.sleep(sleepTime);
				} catch (InterruptedException e) {
					logger.error("Message Log Consumer Interrupted:" + e);
				}
			}
			loopCount++;
		}
	}

	void consumedError() {
		Map<String, Integer> topicErrorCountMap = new HashMap<String, Integer>();
		Integer noOfThread = 1;
		topicErrorCountMap.put(KAFKA_FILE_ERROR_TOPIC, new Integer(noOfThread));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerErrorMap = consumer.createMessageStreams(topicErrorCountMap);
		KafkaStream<byte[], byte[]> errStream = consumerErrorMap.get(KAFKA_FILE_ERROR_TOPIC).get(0);
		ConsumerIterator<byte[], byte[]> itErr = errStream.iterator();
		while (itErr.hasNext()) {
			String message = new String(itErr.next().message());
			Gson gson = new Gson();
			Map<String, String> messageMap = new HashMap<String, String>();
			try {
				messageMap = gson.fromJson(message, messageMap.getClass());
			} catch (Exception e) {
				LogWritterFactory.errorActivity.error(message);
				continue;
			}

			// TODO We're only getting raw data now. We'll have to use the
			// server IP as well for extra information.
			if (messageMap != null) {
				// Write the consumed JSON to Log file.
				LogWritterFactory.errorActivity.info(message);
				String eventJson = (String) messageMap.get("raw");
			} else {
				LogWritterFactory.errorActivity.error(message);
				continue;
			}
		}

	}

	public static void main(String args[]) {
		KafkaLogConsumer kafka = new KafkaLogConsumer();
		kafka.run();
	}
}
