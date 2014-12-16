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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONException;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

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
			map.putAll((Map<? extends String, ? extends String>) mapper.readValue(eventObject.getUser(), new TypeReference<HashMap<String,Object>>(){}));
			map.putAll((Map<? extends String, ? extends String>) mapper.readValue(eventObject.getMetrics(), new TypeReference<HashMap<String,Object>>(){}));
			map.putAll((Map<? extends String, ? extends String>) mapper.readValue(eventObject.getPayLoadObject(), new TypeReference<HashMap<String,Object>>(){}));
			map.putAll((Map<? extends String, ? extends String>) mapper.readValue(eventObject.getContext(), new TypeReference<HashMap<String,Object>>(){}));
			map.putAll((Map<? extends String, ? extends String>) mapper.readValue(eventObject.getSession(), new TypeReference<HashMap<String,Object>>(){}));
		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return (T) map;
	}
	public static <T> T deserializeEventObjectv2(EventObject eventObject) throws JSONException {
		
		ObjectMapper mapper = new ObjectMapper();
		Map<String,Object> map = new HashMap<String,Object>();
		try {
			map.putAll((Map<? extends String, ? extends Object>) mapper.readValue(eventObject.getUser(), new TypeReference<HashMap<String,String>>(){}));
			map.putAll((Map<? extends String, ? extends Object>) mapper.readValue(eventObject.getMetrics(), new TypeReference<HashMap<String,String>>(){}));
			map.putAll((Map<? extends String, ? extends Object>) mapper.readValue(eventObject.getPayLoadObject(), new TypeReference<HashMap<String,String>>(){}));
			map.putAll((Map<? extends String, ? extends Object>) mapper.readValue(eventObject.getContext(), new TypeReference<HashMap<String,String>>(){}));
			map.putAll((Map<? extends String, ? extends Object>) mapper.readValue(eventObject.getSession(), new TypeReference<HashMap<String,String>>(){}));
		
		} catch (Exception e) {
			e.printStackTrace();
		}
		return (T) map;
	}
}
