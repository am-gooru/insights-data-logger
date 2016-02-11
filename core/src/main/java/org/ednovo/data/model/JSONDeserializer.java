/*******************************************************************************
 * JSONDeserializer.java
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
package org.ednovo.data.model;

import java.util.HashMap;
import java.util.Map;

import org.logger.event.cassandra.loader.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JSONDeserializer {

	private static final Logger LOG = LoggerFactory.getLogger(JSONDeserializer.class);

	private static final ObjectMapper MAPPER = new ObjectMapper();

	public static <T> T deserialize(String json, TypeReference<T> type) {
		try {
			return MAPPER.readValue(json, type);
		} catch (Exception e) {
			LOG.error("Exception:", e);
		}
		return null;
	}
		
	public static <T> T deserializeEvent(Event event) {
        TypeReference<Map<String, Object>> mapType = new TypeReference<Map<String,Object>>(){};
        TypeReference<Map<String, Number>> numberMapType = new TypeReference<Map<String,Number>>(){};
        Map<String,Object> map = new HashMap<String,Object>();
        try {
                map.putAll(deserialize(event.getUser().toString(), mapType));
                map.putAll(deserialize(event.getMetrics().toString(), numberMapType));
                map.putAll(deserialize(event.getPayLoadObject().toString(), mapType));
                map.putAll(deserialize(event.getContext().toString(), mapType));
                map.putAll(deserialize(event.getSession().toString(), mapType));                
                map.put(Constants.EVENT_NAME,event.getEventName());
                map.put(Constants.EVENT_ID,event.getEventId());
                map.put(Constants.START_TIME,event.getStartTime());
                map.put(Constants.END_TIME,event.getEndTime());
        } catch (Exception e) {
        		LOG.info("Exception in event : {}",event.getFields());
                LOG.error("Exception:", e);
        }
        return (T) map;
	}

}
