/*******************************************************************************
 * KafkaEventHandler.java
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
package org.ednovo.kafka.producer;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.json.JSONObject;
import org.springframework.stereotype.Component;

@Component
public class KafkaEventHandler {

	private static final long serialVersionUID = 8483062836459978581L;
	

	private SimpleDateFormat dateFormatter = new SimpleDateFormat("dd/MM/yyy HH:mm:ss:S");
	private Producer<String, String> producer;
	private String topic = "activity-log";
	protected Properties props = new Properties();
	private ExecutorService executor;
	private static final int NTHREDS = 7;

	public KafkaEventHandler() {
		
	}

	public KafkaEventHandler(String ip, String port, String topic) {
		init(ip, port, topic);
	}
	
	public KafkaEventHandler(String ip, String port, String topic,Integer poolSize) {
		init(ip, port, topic, poolSize);
	}

	public void init(String ip, String portNo, String topic) {
	
		this.topic = topic;
		props.put("metadata.broker.list",KafkaEventHandler.buildEndPoint(ip, portNo));
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");
		props.put("retry.backoff.ms", "1000");
		props.put("producer.type", "sync");
		executor = Executors.newFixedThreadPool(NTHREDS);
		try {
			producer = new Producer<String, String>(new ProducerConfig(props));
		} catch (Exception e) {
			e.printStackTrace();
		}
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

	public void init(String ip, String portNo, String topic,int poolSize) {
		this.topic = topic;
		props.put("metadata.broker.list",KafkaEventHandler.buildEndPoint(ip, portNo));
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");
		props.put("retry.backoff.ms", "1000");
		props.put("producer.type", "sync");
		executor = Executors.newFixedThreadPool(poolSize);

		try {
			producer = new Producer<String, String>(new ProducerConfig(props));
		} catch (Exception e) {
		}
	}
	
	public void sendEventLog(String eventLog) {
		
		Map<String, String> message = new HashMap<String, String>();
		message.put("timestamp", dateFormatter.format(System.currentTimeMillis()));
		message.put("raw", new String(eventLog));
		String messageAsJson = new JSONObject(message).toString();
		send(messageAsJson);
	}

	public void sendData(String eventLog) {
		KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, eventLog);
		producer.send(data);
	}

	public Producer<String, String> getProducer(){
	return producer;	
	}
	
	public String getTopic(){
	return topic;	
	}
	
	public void send(final String message) {

		executor.submit(new Runnable() {
			
			@Override
			public void run() {
				KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, message);
				producer.send(data);
			}
		});
	}

}
