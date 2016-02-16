/*******************************************************************************
 * CassandraProcessor.java
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
package org.ednovo.kafka.consumer;

import java.util.Properties;

import org.ednovo.data.model.Event;
import org.json.JSONObject;
import org.logger.event.cassandra.loader.CassandraDataLoader;
import org.logger.event.cassandra.loader.DataLoggerCaches;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraProcessor extends BaseDataProcessor implements
		DataProcessor {
	protected Properties properties;
	protected CassandraDataLoader dataLoader;
	private DataLoggerCaches loggerCache;
	static final Logger logger = LoggerFactory.getLogger(CassandraProcessor.class);

	public CassandraProcessor() {
		init();
	}

	protected void init() {
		setLoggerCache(new DataLoggerCaches());
		dataLoader = new CassandraDataLoader();
	}

	@Override
	public void handleRow(Object row) throws Exception {

		/**
		 * This changes to be revertable
		 */
		JSONObject eventJson = (JSONObject) row;
		eventJson.put("context", new JSONObject(eventJson.getString("context")));
		eventJson.put("user", new JSONObject(eventJson.getString("user")));
		JSONObject payLoadObject = new JSONObject(eventJson.getString("payLoadObject"));
		JSONObject answerObject = new JSONObject(payLoadObject.getString("answerObject"));
		payLoadObject.put("answerObject", answerObject);
		eventJson.put("payLoadObject", payLoadObject);
		eventJson.put("metrics", new JSONObject(eventJson.getString("metrics")));
		eventJson.put("session", new JSONObject(eventJson.getString("session")));
		eventJson.put("version", new JSONObject(eventJson.getString("version")));

		LOG.info("eventJSON : " +eventJson);
        if (row != null && (row instanceof Event)) {
       	
       	Event event = new Event(eventJson.toString());
       	event.setFields(eventJson.toString());
       	 if(event.getVersion() == null){
            	return;
            }
       	 
       	if (event.getEventName() == null || event.getEventName().isEmpty() || event.getContext() == null) {
       		logger.warn("EventName or Context is empty. This is an error in EventObject");
       		return;
        	}

//       	eventObjectValidator.validateEventObject(eventObject);
        	dataLoader.processMessage(event);
        }
	}
         
        public DataLoggerCaches getLoggerCache() {
		return loggerCache;
	}

	public void setLoggerCache(DataLoggerCaches loggerCache) {
		this.loggerCache = loggerCache;
	}

}
