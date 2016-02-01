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

import java.text.ParseException;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.ednovo.data.model.EventData;
import org.ednovo.data.model.Event;
import org.ednovo.data.model.EventValidator;
import org.json.JSONObject;
import org.logger.event.cassandra.loader.CassandraDataLoader;
import org.logger.event.cassandra.loader.DataLoggerCaches;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

public class CassandraProcessor extends BaseDataProcessor implements
		DataProcessor {
	protected Properties properties;
	protected CassandraDataLoader dataLoader;
	private Gson gson;
	private final String GOORU_EVENT_LOGGER_API_KEY = "5673eaa7-15e3-4d6b-b3ef-5f7729c82de3";
	private final String EVENT_SOURCE = "kafka-logged";
	private EventValidator eventValidator;
	private DataLoggerCaches loggerCache;
	static final Logger logger = LoggerFactory.getLogger(CassandraProcessor.class);

	public CassandraProcessor() {
		init();
	}

	protected void init() {
		gson = new Gson();
		setLoggerCache(new DataLoggerCaches());
		dataLoader = new CassandraDataLoader();
		eventValidator = new EventValidator(null);
	}

	@Override
	public void handleRow(Object row) throws Exception {

		/**
		 * This changes to be revertable
		 */
		JSONObject eventJson = (JSONObject) row;
		eventJson.put("context", new JSONObject(eventJson.getString("context")));
		eventJson.put("user", new JSONObject(eventJson.getString("user")));
		eventJson.put("payLoadObject", new JSONObject(eventJson.getString("payLoadObject")));
		eventJson.put("metrics", new JSONObject(eventJson.getString("metrics")));
		eventJson.put("session", new JSONObject(eventJson.getString("session")));
		eventJson.put("version", new JSONObject(eventJson.getString("version")));
			
        if (row != null && (row instanceof Event)) {
       	
       	 Event event = (Event) row;
        	
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
        public void updateToStaging(String statTime,String endTime,String eventName,String apiKey){
        	/*try {
				dataLoader.updateStaging(statTime, endTime,eventName,apiKey);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}*/
        }
        
	private void cleanseData(EventData eventData) {
		updateEventNameForConsistency(eventData);
		updateEventNameIfEmpty(eventData);
		cleansEventValue(eventData);
		if(eventData.getStartTime() == null && eventData.getExpireTime() != null) {
			// SessionExpired event did NOT have start / end time. Use expiry time.
			eventData.setStartTime(eventData.getExpireTime());
			eventData.setEndTime(eventData.getExpireTime());
		}
	}


	private void cleansEventValue(EventData eventData){
	
			String eventValue = eventData.getQuery();
			if(eventValue != null) {
			 if (eventValue.startsWith("\'") || eventValue.startsWith("\"")) {
				eventValue = eventValue.substring(1, eventValue.length());
		      }
		      if (eventValue.endsWith("\'") || eventValue.endsWith("\"")) {
		    	  eventValue = eventValue.substring(0, eventValue.length() - 1);
		      }
		      eventValue = eventValue.trim();
		      if(eventValue.length() > 100){
		    	  eventValue = eventValue.substring(0, 100);
		      }
		      eventData.setQuery(eventValue);
			}
	}
	
	
	private void updateEventNameForConsistency(EventData eventData) {
		//Handle event naming convention inconsistency - ensure event map is not empty
		String eventName = DataUtils.makeEventNameConsistent(eventData.getEventName());
		
		
		if(eventName != null) {
			if(eventName.equalsIgnoreCase("collection-play") && !StringUtils.isEmpty(eventData.getParentGooruId())) {
				// This is actually collection-resource-play event, coming up disguised as collection-play. Change.
				eventName = "collection-resource-play";
			}
            if(eventName.equalsIgnoreCase("collection-resource-play-dots") && eventData.getAttemptStatus() != null) {                               
            	// This is used to remove collection resource play from server side logs
                eventName = "collection-question-resource-play-dots";
             }

 		}
		eventData.setEventName(eventName);
	}

	private void updateEventNameIfEmpty(EventData eventData) {
		 if (StringUtils.isEmpty(eventData.getEventName()) && eventData.getContext() != null){
	     		Map<String, String> guessedAttributes = DataUtils.guessEventName(eventData.getContext());
	     		if(guessedAttributes != null) {
	     			if(guessedAttributes.get("event_name") != null ) {
	     				eventData.setEventName(guessedAttributes.get("event_name"));
	     			}
	     			if(guessedAttributes.get("parent_gooru_id") != null  && eventData.getParentGooruId() == null) {
	     				eventData.setParentGooruId(guessedAttributes.get("parent_gooru_id"));
	     			}
	     			if(guessedAttributes.get("content_gooru_id") != null  && eventData.getContentGooruId() == null) {
	     				eventData.setContentGooruId(guessedAttributes.get("content_gooru_id"));
	     			}
	     		}
			 }
	}

	public DataLoggerCaches getLoggerCache() {
		return loggerCache;
	}

	public void setLoggerCache(DataLoggerCaches loggerCache) {
		this.loggerCache = loggerCache;
	}

}
