/*******************************************************************************
 * MicroAggregatorConsumer.java
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
package org.kafka.event.microaggregator.consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.Message;

import org.json.JSONException;
import org.kafka.event.microaggregator.core.MicroAggregationLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;

import com.google.gson.Gson;

public class MicroAggregatorConsumer extends Thread {
	
private final ConsumerConnector consumer;
private static String topic;
private static String ZK_IP;
private static String ZK_PORT;
private static String KAFKA_TOPIC;
private static String KAFKA_GROUPID;

private MicroAggregationLoader microAggregationLoader;
static final Logger LOG = LoggerFactory.getLogger(MicroAggregatorConsumer.class);
private static final Logger activityLogger = LoggerFactory.getLogger("activityLog");

public MicroAggregatorConsumer() {

	Map<String,String> kafkaProperty = new HashMap<String, String>();
	microAggregationLoader = new MicroAggregationLoader(null);
	kafkaProperty =  microAggregationLoader.getKafkaProperty("v2~kafka~microaggregator~consumer");
	ZK_IP = kafkaProperty.get("zookeeper_ip");
	ZK_PORT = kafkaProperty.get("zookeeper_portno");
	KAFKA_TOPIC = kafkaProperty.get("kafka_topic");
	KAFKA_GROUPID = kafkaProperty.get("kafka_groupid");
	 
	this.topic = KAFKA_TOPIC;
	consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig());
}

private static ConsumerConfig createConsumerConfig() {
	Properties props = new Properties();

	props.put("zookeeper.connect",MicroAggregatorConsumer.buildEndPoint(ZK_IP, ZK_PORT));
    props.put("group.id", KAFKA_GROUPID);
    props.put("zookeeper.session.timeout.ms", "10000");
    props.put("zookeeper.sync.time.ms", "200");
    props.put("auto.commit.interval.ms", "1000");
	LOG.info("Kafka micro aggregator consumer config: " + ZK_IP + ":" + ZK_PORT + "::" + topic + "::" + KAFKA_GROUPID);

	return new ConsumerConfig(props);

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

public void run() {
	Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
	Integer noOfThread =1;
	
	topicCountMap.put(topic, new Integer(1));
	Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
	KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
	ConsumerIterator<byte[], byte[]> it = stream.iterator();
	while (it.hasNext()) {
		// String message = ExampleUtils.getMessage(it.next());
		String message = new String(it.next().message());
		Gson gson = new Gson();
		Map<String, String> messageMap = new HashMap<String, String>();
		try {
			messageMap = gson.fromJson(message, messageMap.getClass());
		} catch (Exception e) {
			LOG.error("Message Consumer Error: " + e.getMessage());
			continue;
		}

		// TODO We're only getting raw data now. We'll have to use the
		// server IP as well for extra information.
		if (messageMap != null) {
			updateActivityStream(messageMap);
			staticAggregation(messageMap);
		} else {
			LOG.error("Message Consumer Error messageMap : No data found");
			continue;
		}
	}
}

@Async
public void updateActivityStream(Map<String, String> messageMap) {
	String eventJson = (String) messageMap.get("raw");
	if (eventJson != null) {
		try {
			LOG.info("EventJson:{}", eventJson);
			microAggregationLoader.updateActivityStream(eventJson);
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}
}

@Async
public void staticAggregation(Map<String, String> messageMap) {
	LOG.info("static Aggregator Consumed");
	System.out.println("static Aggregator Consumed");
	String eventJson = (String) messageMap.get("aggregationDetail");
	if (eventJson != null && !eventJson.isEmpty()) {
		microAggregationLoader.staticAggregation(eventJson);
	}
}
}
