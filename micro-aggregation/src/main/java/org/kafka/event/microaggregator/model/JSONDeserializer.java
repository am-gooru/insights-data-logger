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
package org.kafka.event.microaggregator.model;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.json.JSONException;

public class JSONDeserializer {

	public static <T> T deserialize(String json, TypeReference<T> type) {
		ObjectMapper mapper = new ObjectMapper();
		try {
			return mapper.readValue(json, type);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public static <T> T deserializeEventObject(EventObject eventObject) throws JSONException {
		
		ObjectMapper mapper = new ObjectMapper();
		Map<String,String> map = new HashMap<String,String>();
		try {
			map.putAll((Map<? extends String, ? extends String>) mapper.readValue(eventObject.getUser().toString(), new TypeReference<HashMap<String,String>>(){}));
			map.putAll((Map<? extends String, ? extends String>) mapper.readValue(eventObject.getMetrics().toString(), new TypeReference<HashMap<String,String>>(){}));
			map.putAll((Map<? extends String, ? extends String>) mapper.readValue(eventObject.getPayLoadObject().toString(), new TypeReference<HashMap<String,String>>(){}));
			map.putAll((Map<? extends String, ? extends String>) mapper.readValue(eventObject.getContext().toString(), new TypeReference<HashMap<String,String>>(){}));
			map.putAll((Map<? extends String, ? extends String>) mapper.readValue(eventObject.getSession().toString(), new TypeReference<HashMap<String,String>>(){}));
		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return (T) map;
	}

}
