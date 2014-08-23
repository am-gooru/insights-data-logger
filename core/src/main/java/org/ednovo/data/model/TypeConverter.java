/*******************************************************************************
 * TypeConverter.java
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

import java.util.Date;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;

import com.google.gson.Gson;

public class TypeConverter {

	public static <T> T stringToIntArray(String arrayAsString){
		if(arrayAsString != null){

			String[] items = arrayAsString.replaceAll("\\[", "").replaceAll("\\]", "").split(",");

			int[] results = new int[items.length];

			for (int i = 0; i < items.length; i++) {
				try {
					results[i] = Integer.parseInt(items[i]);
				} catch (NumberFormatException nfe) {};
			}
			return (T) results;
		}
		return null;
	}
	
	public static <T> T stringToAny(String value , String type){
		Object result = null;
		try{
			if(value != null && type != null){
				if(type.equals("Long")){
					result = Long.valueOf(value);
				}else if(type.equals("Double")){
					result = Double.valueOf(value);
				}else if(type.equals("Integer")){
					result = Integer.valueOf(value);
				}else if(type.equals("JSONObject")){
					result = new JSONObject(value);
				}else if(type.equals("Date")){
					//accepting timestamp
					result =  new Date(Long.valueOf(value));
				}else if(type.equals("Boolean")){
					result = Boolean.valueOf(value);
				}
				else if(type.equals("String")){
					result = value;
				}else if(type.equals("IntegerArray")){

					String[] items = value.replaceAll("\\[", "").replaceAll("\\]", "").split(",");

					int[] results = new int[items.length];

					for (int i = 0; i < items.length; i++) {
						try {
							results[i] = Integer.parseInt(items[i]);
						} catch (NumberFormatException nfe) {};
					}
					result =  results;
				}
				else if(type.equals("StringArray")){

					String[] items = value.replaceAll("\\[", "").replaceAll("\\]", "").split(",");

					String[] results = new String[items.length];

					for (int i = 0; i < items.length; i++) {
						try {
							results[i] = items[i];
						} catch (Exception nfe) {
						};
					}
					result =  results;
				}
				else if(type.equals("JSONArray")){
					result =  new JSONArray(value);
				}else{
					throw new RuntimeException("Unsupported type " + type + ". Please Contact Admin!!");
				}
				
				return (T) result;
			}
		}catch(Exception e){
			return (T) e;
		}
		return null;
	}
	
	public static String convertMapToJsonString(Map<String, String> map) {
        Gson gson = new Gson();
        String json = gson.toJson(map);
        return json;
    }
}
