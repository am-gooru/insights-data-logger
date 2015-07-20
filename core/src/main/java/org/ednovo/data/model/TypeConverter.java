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

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

public class TypeConverter {

	private static final Logger logger = LoggerFactory.getLogger(TypeConverter.class);

	public static <T> T stringToIntArray(String arrayAsString) {
		if (arrayAsString != null) {
			int[] results = new int[0];
			String value = arrayAsString.replaceAll("\\[", "").replaceAll("\\]", "");
			if (StringUtils.isNotBlank(value)) {
				String[] items = value.split(",");
				results = new int[items.length];
				for (int i = 0; i < items.length; i++) {
					try {
						results[i] = Integer.parseInt(items[i]);
					} catch (NumberFormatException nfe) {
						logger.error("Exception", nfe);
					}
				}
			}
			return (T) results;
		}
		return null;
	}

	public static <T> T stringToAny(String value, String type) {
		Object result = null;
		if (value != null && type != null) {
			if (type.equalsIgnoreCase("Long")) {
				try {
					result = Long.valueOf(value.trim());
				} catch (NumberFormatException nfel) {
					result = 0L;
					return (T) result;
				}
			} else if (type.equalsIgnoreCase("Double")) {
				try {
					result = Double.valueOf(value.trim());
				} catch (NumberFormatException nfel) {
					result = 0.0;
					return (T) result;
				}
			} else if (type.equalsIgnoreCase("Integer")) {
				try {
					result = Integer.valueOf(value.trim());
				} catch (NumberFormatException nfel) {
					result = 0;
					return (T) result;
				}
			} else if (type.equalsIgnoreCase("JSONObject")) {
				try {
					result = new JSONObject(value.trim());
				} catch (JSONException e) {
					logger.error("Unable to convert to JSONObject");
					return (T) new JSONObject();
				}
			} else if (type.equalsIgnoreCase("Date")) {
				// accepting timestamp
				SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd kk:mm:ss+0000");
				SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd kk:mm:ss");
				SimpleDateFormat formatter3 = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy");
				SimpleDateFormat formatter4 = new SimpleDateFormat("yyyy/MM/dd kk:mm:ss.000");
				try {
					result = new Date(Long.valueOf(value));
				} catch (Exception e) {
					try {
						result = formatter.parse(value);
					} catch (Exception e1) {
						try {
							result = formatter2.parse(value);
						} catch (Exception e2) {
							try {
								result = formatter3.parse(value);
							} catch (Exception e3) {
								try {
									result = formatter4.parse(value);
								} catch (Exception e4) {
									logger.error("Error while convert " + value + " to date");
								}
							}
						}
					}
				}

			} else if (type.equalsIgnoreCase("Boolean")) {
				result = Boolean.valueOf(value.trim());
			} else if (type.equalsIgnoreCase("String")) {
				result = value.trim();
			} else if (type.equals("IntegerArray")) {

				String[] items = value.trim().replaceAll("\\[", "").replaceAll("\\]", "").replaceAll("\"", "").split(",");

				int[] results = new int[items.length];

				for (int i = 0; i < items.length; i++) {
					try {
						if (!items[i].trim().isEmpty() && Integer.parseInt(items[i].trim()) != 0) {
							results[i] = Integer.parseInt(items[i].trim());
						}
					} catch (NumberFormatException nfe) {
						logger.error("Exeption : " + nfe);
					}
					;
				}
				result = results;
			} else if (type.equalsIgnoreCase("StringArray")) {

				String[] items = value.trim().replaceAll("\\[", "").replaceAll("\\]", "").replaceAll("\"", "").split(",");

				String[] results = new String[items.length];

				for (int i = 0; i < items.length; i++) {
					try {
						results[i] = items[i].trim();
					} catch (Exception nfe) {
						logger.error("Exeption : " + nfe);
					}
					;
				}
				result = results;
			} else if (type.equalsIgnoreCase("JSONArray")) {
				try {
					result = new JSONArray(value);
				} catch (JSONException e) {
					logger.error("Unable to convert to JSONArray");
					return (T) new JSONArray();
				}
			} else if (type.equalsIgnoreCase("Timestamp")) {
				// accepting timestamp
				try {
					result = new Timestamp(Long.valueOf(value));
				} catch (Exception e) {
					logger.error("Error while convert " + value + " to timestamp");
				}

			} else {
				throw new RuntimeException("Unsupported type " + type + ". Please Contact Admin!!");
			}

			return (T) result;
		}
		return null;
	}

	public static String convertMapToJsonString(Map<String, String> map) {
		Gson gson = new Gson();
		String json = gson.toJson(map);
		return json;
	}
}
