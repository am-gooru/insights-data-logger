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
package org.kafka.event.microaggregator.model;

import java.io.Serializable;


public class EventObject implements Serializable {
	
	/**
	 * @author daniel
	 */
	private static final long serialVersionUID = 1L;

	private String context;
	
	private String user;
	
	private String payLoadObject;
	
	private String metrics;
	
	private String session;

	private Long startTime;
	
	private Long endTime;
	
	private String eventName;
	
	private String contentGooruId;
	
	private String parentGooruId;
	
	private Long timeInMillSec;
	
	private String fields;
	
	private String eventId;
	
	private String parentEventId;
	
	private String version;

	private String eventType;
	
	public String getEventType() {
		return eventType;
	}

	public void setEventType(String eventType) {
		this.eventType = eventType;
	}

	public String getParentEventId() {
		return parentEventId;
	}

	public void setParentEventId(String parentEventId) {
		this.parentEventId = parentEventId;
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

	public String getEventName() {
		return eventName;
	}

	public void setEventName(String eventName) {
		this.eventName = eventName;
	}

	public String getContentGooruId() {
		return contentGooruId;
	}

	public void setContentGooruId(String contentGooruId) {
		this.contentGooruId = contentGooruId;
	}

	public String getParentGooruId() {
		return parentGooruId;
	}

	public void setParentGooruId(String parentGooruId) {
		this.parentGooruId = parentGooruId;
	}

	public Long getTimeInMillSec() {
		return timeInMillSec;
	}

	public void setTimeInMillSec(Long timeInMillSec) {
		this.timeInMillSec = timeInMillSec;
	}

	public String getFields() {
		return fields;
	}

	public void setFields(String fields) {
		this.fields = fields;
	}

	public String getEventId() {
		return eventId;
	}

	public void setEventId(String eventId) {
		this.eventId = eventId;
	}

	public String getContext() {
		return context;
	}

	public void setContext(String context) {
		this.context = context;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getPayLoadObject() {
		return payLoadObject;
	}

	public void setPayLoadObject(String payLoadObject) {
		this.payLoadObject = payLoadObject;
	}

	public String getMetrics() {
		return metrics;
	}

	public void setMetrics(String metrics) {
		this.metrics = metrics;
	}

	public String getSession() {
		return session;
	}

	public void setSession(String session) {
		this.session = session;
	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}
}
