/*******************************************************************************
 * KafkaEventHandler.java
 * kafka-producer
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
import java.util.Map;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.producer.ProducerConfig;

import org.json.JSONObject;
import org.springframework.stereotype.Component;

@Component
public class KafkaEventHandler {

	private static final long serialVersionUID = 8483062836459978581L;

	private SimpleDateFormat dateFormatter = new SimpleDateFormat("dd/MM/yyy HH:mm:ss:S");
	private Producer<String, String> producer;
	private String topic = "topic name";
	protected Properties props = new Properties();

	public KafkaEventHandler() {
	}

	public KafkaEventHandler(String ip, String port, String topic) {
		init(ip, port, topic);
	}

	public void init(String ip, String port, String topic) {
		this.topic = topic;
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("zk.connect", ip + ":" + port);

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

	private void send(String message) {
		ProducerData<String, String> data = new ProducerData<String, String>(topic, message);
		producer.send(data);
	}

}
