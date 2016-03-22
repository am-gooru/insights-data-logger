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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

public class KafkaLogConsumer extends Thread implements Runnable {

	private static ConsumerConnector consumer;
	private static String topic;

	private static String ZOOKEEPER_IP;
	private static String ZOOKEEPER_PORT;
	private static String KAFKA_GROUPID;
	private static String KAFKA_FILE_TOPIC;
	
	private static Logger LOG = LoggerFactory.getLogger(KafkaLogConsumer.class);

	public KafkaLogConsumer() {
		getKafkaConsumer();
	}

	private void getKafkaConsumer() {
		ZOOKEEPER_IP = System.getenv("LOG_WRITER_ZOOKEEPER_IP");
		ZOOKEEPER_PORT = "2181";
		KAFKA_FILE_TOPIC = System.getenv("LOG_WRITER_TOPIC");
		KAFKA_GROUPID = System.getenv("LOG_WRITER_GROUPID");
		KafkaLogConsumer.topic = KAFKA_FILE_TOPIC;
		LOG.info("ZOOKEEPER_IP : " + ZOOKEEPER_IP + " - KAFKA_FILE_TOPIC: " + KAFKA_FILE_TOPIC + " -KAFKA_GROUPID : "+ KAFKA_GROUPID);
		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig());
	}

	public static String buildEndPoint(String ip, String portNo) {

		StringBuffer stringBuffer = new StringBuffer();
		String[] ips = ip.split(",");
		String[] ports = portNo.split(",");
		for (int count = 0; count < ips.length; count++) {

			if (stringBuffer.length() > 0) {
				stringBuffer.append(",");
			}

			if (count < ports.length) {
				stringBuffer.append(ips[count] + ":" + ports[count]);
			} else {
				stringBuffer.append(ips[count] + ":" + ports[0]);
			}
		}
		return stringBuffer.toString();
	}

	private static ConsumerConfig createConsumerConfig() {

		Properties props = new Properties();
		props.put("zookeeper.connect", KafkaLogConsumer.buildEndPoint(ZOOKEEPER_IP, ZOOKEEPER_PORT));
		props.put("group.id", KAFKA_GROUPID);
		props.put("zookeeper.session.timeout.ms", "20000");
		props.put("zookeeper.sync.time.ms", "2000");
		props.put("auto.commit.interval.ms", "1000");
		LOG.info("Kafka File writer consumer config: " + ZOOKEEPER_IP + ":" + ZOOKEEPER_PORT + "::" + topic + "::" + KAFKA_GROUPID);

		return new ConsumerConfig(props);

	}

	public void run() {

		initConsumer();
	}
	
	/**
	 * Clean Shutdown
	 */
	public static void shutdownLogConsumer(){
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			LOG.debug("Kafka Log Consumer unable to wait for 1000ms before it's shutdown");
		}
		consumer.shutdown();
	}

	public void initConsumer() {

		Integer noOfThread = 1;
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, noOfThread);
		try {

			/**
			 * get list of kafka stream from specific topic
			 */
			topicCountMap.put(topic, new Integer(1));
			LOG.info("Logwriter topic : "+topic);
			LOG.info("LogWriter topicCountMap : "+topicCountMap);
			Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
			KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
			ConsumerIterator<byte[], byte[]> it = stream.iterator();
			/**
			 * process consumed data
			 */
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

				/**
				 * TODO We're only getting raw data now. We'll have to use the server IP as well for extra information.
				 */
				if (messageMap != null && !messageMap.isEmpty()) {
					/**
					 * Write the consumed JSON to Log file.
					 */
					LogWritterFactory.activity.info(message);
				} else {
					LogWritterFactory.errorActivity.error(message);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error("Log writter Message  Consumer:" + e);
		}
	}
}
