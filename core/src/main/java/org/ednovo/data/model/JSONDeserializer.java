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

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

import org.logger.event.cassandra.loader.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class JSONDeserializer implements Constants {

	private static final Logger logger = LoggerFactory.getLogger(JSONDeserializer.class);

	private static Gson gson = new Gson();

	
	public static <T> T deserialize(String json, TypeReference<T> type) {
		ObjectMapper mapper = new ObjectMapper();
		try {
			return mapper.readValue(json, type);
		} catch (Exception e) {
			logger.error("Exception:", e);
		}
		return null;
	}
		
	public static <T> T deserializeEvent(Event event) {
        Type mapType = new TypeToken <HashMap<String, Object>>() {}.getType();
        Type numberMapType = new TypeToken <HashMap<String, Number>>() {}.getType();
        Map<String,Object> map = new HashMap<String,Object>();
        try {
                map.putAll((Map<? extends String, ? extends Object>) gson.fromJson(event.getUser(), mapType));
                map.putAll((Map<? extends String, ? extends Object>) gson.fromJson(event.getMetrics(), numberMapType));
                map.putAll((Map<? extends String, ? extends Object>) gson.fromJson(event.getPayLoadObject(), mapType));
                map.putAll((Map<? extends String, ? extends Object>) gson.fromJson(event.getContext(), mapType));
                map.putAll((Map<? extends String, ? extends Object>) gson.fromJson(event.getSession(), mapType));
                map.put(EVENT_NAME,event.getEventName());
                map.put(EVENT_ID,event.getEventId());
                map.put(START_TIME,event.getStartTime());
                map.put(END_TIME,event.getEndTime());
        } catch (Exception e) {
                logger.error("Exception:", e);
        }
        return (T) map;
	}

}
