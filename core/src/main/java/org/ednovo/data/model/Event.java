/*******************************************************************************
 * EventObject.java
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

import org.json.JSONException;
import org.json.JSONObject;

public class Event extends JSONObject {

	public Event() {
		super();
	}

	public Event(String json) throws JSONException {
		super(json);
		this.build();
	}

	/**
	 * @author daniel
	 */

	private String eventId;

	private String eventName;

	private JSONObject context;

	private JSONObject user;

	private JSONObject payLoadObject;

	private JSONObject metrics;

	private JSONObject session;
	
	private JSONObject version;
	
	private Long startTime;

	private Long endTime;

	private String apiKey;

	private String contentGooruId;

	private String fields;

	public String getEventId() {
		return eventId;
	}

	public void setEventId(String eventId) {
		this.eventId = eventId;
	}

	public String getEventName() {
		return eventName;
	}

	public void setEventName(String eventName) {
		this.eventName = eventName;
	}

	public JSONObject getContext() {
		return context;
	}

	public void setContext(JSONObject context) {
		this.context = context;
	}

	public JSONObject getUser() {
		return user;
	}

	public void setUser(JSONObject user) {
		this.user = user;
	}

	public JSONObject getPayLoadObject() {
		return payLoadObject;
	}

	public void setPayLoadObject(JSONObject payLoadObject) {
		this.payLoadObject = payLoadObject;
	}

	public JSONObject getMetrics() {
		return metrics;
	}

	public void setMetrics(JSONObject metrics) {
		this.metrics = metrics;
	}

	public JSONObject getSession() {
		return session;
	}

	public void setSession(JSONObject session) {
		this.session = session;
	}

	public Long getStartTime() {
		return startTime;
	}

	public void setStartTime(Long startTime) {
		this.startTime = startTime;
	}

	public Long getEndTime() {
		return endTime;
	}

	public void setEndTime(Long endTime) {
		this.endTime = endTime;
	}

	public String getApiKey() {
		return apiKey;
	}

	public void setApiKey(String apiKey) {
		this.apiKey = apiKey;
	}

	public String getContentGooruId() {
		return contentGooruId;
	}

	public void setContentGooruId(String contentGooruId) {
		this.contentGooruId = contentGooruId;
	}

	public String getFields() {
		return fields;
	}

	public void setFields(String fields) {
		this.fields = fields;
	}

	public JSONObject getVersion() {
		return version;
	}

	public void setVersion(JSONObject version) {
		this.version = version;
	}

	private Event build() {
		try {
			this.context = getJSONObject("context");
			this.user = getJSONObject("user");
			this.payLoadObject = getJSONObject("payLoadObject");
			this.metrics = getJSONObject("metrics");
			this.session = getJSONObject("session");
			this.version = getJSONObject("version");
			this.startTime = getLong("startTime");
			this.endTime = getLong("endTime");
			this.eventId = getString("eventId");
			this.eventName = getString("eventName");
			this.apiKey = session.getString("apiKey");
			this.contentGooruId = context.getString("contentGooruId");

			if (eventName.matches("collection.play|collection.resource.play|resource.play")) {
				long views = 1L;
				long timeSpent = (endTime - startTime);
				String collectionType = context.getString("collectionType");
				String eventType = context.getString("type");
				if (("start".equals(eventType) && "assessment".equalsIgnoreCase(collectionType)) || ("stop".equals(eventType) && "collection".equalsIgnoreCase(collectionType))) {
					views = 0L;
				}
				if (timeSpent > 7200000 || timeSpent < 0) {
					timeSpent = 7200000;
				}
				metrics.put("viewsCount", views);
				metrics.put("totalTimeSpentInMs", timeSpent);
			}
		} catch (JSONException e) {
		}
		return this;
	}
}
