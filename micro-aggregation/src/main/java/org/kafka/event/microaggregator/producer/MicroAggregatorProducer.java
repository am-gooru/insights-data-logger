/*******************************************************************************
 * MicroAggregatorProducer.java
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
package org.kafka.event.microaggregator.producer;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.producer.ProducerConfig;

import org.json.JSONObject;
import org.kafka.event.microaggregator.consumer.MicroAggregatorConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class MicroAggregatorProducer 
{
	private static final long serialVersionUID = 8483062836459978581L;
	private static final Logger LOG = LoggerFactory.getLogger(MicroAggregatorConsumer.class);
	private SimpleDateFormat dateFormatter = new SimpleDateFormat("dd/MM/yyy HH:mm:ss:S");
	private Producer<String, String> producer;
	private String topic = "event-log-writer-dev";
	protected Properties props = new Properties();
	
	public MicroAggregatorProducer(){
	}
	
	public MicroAggregatorProducer(String kafkaIp, String port, String topic, String producerType) {
		init(kafkaIp, port, topic, producerType);
	}
	
	public void init(String kafkaIp, String port, String topic, String producerType) {
		this.topic = System.getenv("INSIGHTS_KAFKA_AGGREGATOR_TOPIC");;
		LOG.info("Kafka File writer producer config: "+ kafkaIp+":"+port+"::"+topic+"::"+producerType);
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("zk.connect", kafkaIp + ":" + port);		
		props.put("producer.type", producerType);
		props.put("compression.codec", "1");
		
		
		try{
		producer = new Producer<String, String>(
				new ProducerConfig(props));
		}
		catch (Exception e) {
		}
	}
	
	public void sendEventForAggregation(String eventLog) {
		Map<String, String> message = new HashMap<String, String>();
		message.put("timestamp", dateFormatter.format(System.currentTimeMillis()));
		message.put("raw", new String(eventLog));
		
		String messageAsJson = new JSONObject(message).toString();
		send(messageAsJson);
	}
		
	private void send(String message) {
		LOG.info("message: {}",message);
		LOG.info("topic: {}",topic);
		ProducerData<String, String> data = new ProducerData<String, String>(topic, message);
		producer.send(data);
	}

	public static void main(String args[])
	{
		String samplejson = "{\"userIp\":\"38.64.65.241\",\"organizationUid\":\"4261739e-ccae-11e1-adfb-5404a609bd14\",\"resourceInstanceId\":\"2b60dff5-fe36-4e2a-aadc-c07189421461\",\"userAgent\":\"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.31 (KHTML, like Gecko) Chrome/26.0.1410.64 Safari/537.31\",\"sessionActivityItemId\":\"null\",\"sessionActivityId\":null,\"endTime\":1367366400798,\"type\":\"start\",\"startTime\":1367366400779,\"sessionToken\":\"156e533d-fffa-4581-abe2-4f932e7919ee\",\"contentGooruId\":\"38ceafa3-de9f-4158-98ec-19b8724fd188\",\"eventId\":\"7147dc48-c799-7712-feff-a4049689e2c8\",\"parentGooruId\":\"03bfd74b-3171-48b1-8ff4-3a4b870a4424\",\"parentEventId\":\"e5c8c0db-b566-d588-3d07-53dcc0c66848\",\"userId\":11878,\"context\":\"/activity/log/7147dc48-c799-7712-feff-a4049689e2c8/start\",\"eventName\":\"collection-play\",\"imeiCode\":\"null\",\"apiKey\":\"5673eaa7-15e3-4d6b-b3ef-5f7729c82de3\"}";
		MicroAggregatorProducer producer = new MicroAggregatorProducer("192.241.237.160", "2181", "event-log-writer-daniel", "async");
		producer.sendEventForAggregation(samplejson);
	}
}
