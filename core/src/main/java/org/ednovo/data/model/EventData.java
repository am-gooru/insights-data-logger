/*******************************************************************************
 * EventData.java
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

import java.io.Serializable;


public class EventData implements Serializable{

	private static final long serialVersionUID = 3729840991708746034L;
	private Long startTime;
	private Long endTime;
	private String eventName;
	private String userIp;
	private String sessionToken;
	private String organizationUid;
	private String eventId;
	private String parentEventId;
	private String eventType;
	private String type;
	private String contentGooruId;
	private String parentGooruId;
	private String contentId;
	private String gooruOId;
	private String gooruOid;
	private String query;
	private String reactionType;
	private String serverId;
	private String shelfId;
	private String userAgent;
	private String requestId;
	private String requestSessionId;
	private String requestApiKey;
	private Long timeInMillSec;
	private String userId;
	private String gooruUId;
	private String timeCodeMi;
	private String apiKey;
	private String fields;
	private Long expireTime;
	private String eventSource;
	private String eventKeyUUID;
	private Long timeSpentInMs;
	private String city;
	private String country;
	private String state;
	private int attemptNumberOfTrySequence;
	private String attemptFirstStatus;
	private int answerFirstId;
	private int[] attemptTrySequence;
	private int[] attemptStatus;
	private int[] answerId;
	private String openEndedText;
	private String contextInfo;
	private String collaboratorIds;
	private boolean mobileData;
	private Integer hintId;
	private String requestMethod ;
	private String context ;
	private String version;
	
	public String getOpenEndedText() {
		return openEndedText;
	}
	public void setOpenEndedText(String openEndedText) {
		this.openEndedText = openEndedText;
	}
	public String getGooruOid() {
		return gooruOid;
	}
	public void setGooruOid(String gooruOid) {
		this.gooruOid = gooruOid;
	}
	public int getAttemptNumberOfTrySequence() {
		return attemptNumberOfTrySequence;
	}
	public void setAttemptNumberOfTrySequence(int attemptNumberOfTrySequence) {
		this.attemptNumberOfTrySequence = attemptNumberOfTrySequence;
	}
	
	public String getState() {
		return state;
	}
	public void setState(String state) {
		this.state = state;
	}    
	public Long getExpireTime() {
		return expireTime;
	}
	public void setExpireTime(Long expireTime) {
		this.expireTime = expireTime;
	}
	public String getFields() {
		return fields;
	}
	public String getApiKey() {
		return apiKey;
	}
	public String getUserId() {
		return userId;
	}
	public void setUserId(String userId) {
		this.userId = userId;
	}
	public Long getStartTime() {
		return startTime;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
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
	public String getContext() {
		return context;
	}
	public void setContext(String context) {
		this.context = context;
	}
	public String getUserIp() {
		return userIp;
	}
	public void setUserIp(String userIp) {
		this.userIp = userIp;
	}
	public String getSessionToken() {
		return sessionToken;
	}
	public void setSessionToken(String sessionToken) {
		this.sessionToken = sessionToken;
	}
	public String getOrganizationUid() {
		return organizationUid;
	}
	public void setOrganizationUid(String organizationUid) {
		this.organizationUid = organizationUid;
	}
	public String getEventId() {
		return eventId;
	}
	public void setEventId(String eventId) {
		this.eventId = eventId;
	}
	public String getEventType() {
		return eventType;
	}
	public void setEventType(String eventType) {
		this.eventType = eventType;
	}
	public String getContentGooruId() {
		return contentGooruId;
	}
	public void setContentGooruId(String contentGooruId) {
		this.contentGooruId = contentGooruId;
	}
	public String getServerId() {
		return serverId;
	}
	public void setServerId(String serverId) {
		this.serverId = serverId;
	}
	public String getShelfId() {
		return shelfId;
	}
	public void setShelfId(String shelfId) {
		this.shelfId = shelfId;
	}
	public String getUserAgent() {
		return userAgent;
	}
	public void setUserAgent(String userAgent) {
		this.userAgent = userAgent;
	}
	public String getRequestId() {
		return requestId;
	}
	public void setRequestId(String requestId) {
		this.requestId = requestId;
	}
	public String getRequestSessionId() {
		return requestSessionId;
	}
	public void setRequestSessionId(String requestSessionId) {
		this.requestSessionId = requestSessionId;
	}
	public String getRequestApiKey() {
		return requestApiKey;
	}
	public void setRequestApiKey(String requestApiKey) {
		this.requestApiKey = requestApiKey;
	}
	public Long getTimeInMillSec() {
		return timeInMillSec;
	}
	public void setTimeInMillSec(Long timeInMillSec) {
		this.timeInMillSec = timeInMillSec;
	}
	public void setParentGooruId(String parentGooruId) {
		this.parentGooruId = parentGooruId;
	}
	public String getParentGooruId() {
		return parentGooruId;
	}
	public void setTimeCodeMi(String timeCodeMi) {
		this.timeCodeMi = timeCodeMi;
	}
	public String getTimeCodeMi() {
		return timeCodeMi;
	}
	public void setQuery(String query) {
		this.query = query;
	}
	public String getQuery() {
		return query;
	}
	public void setApiKey(String apiKey) {
		this.apiKey = apiKey;
	}
	public void setFields(String fields) {
		this.fields = fields;
	}
	public void setEventSource(String eventSource) {
		this.eventSource = eventSource;
	}
	public String getEventSource() {
		return eventSource;
	}
	public String getGooruOId() {
		return gooruOId;
	}
	public void setGooruOId(String gooruOId) {
		this.gooruOId = gooruOId;
	}
	public String getContentId() {
		return contentId;
	}
	public void setContentId(String contentId) {
		this.contentId = contentId;
	}
	public String getEventKeyUUID() {
	    return eventKeyUUID;
	}
	public void setEventKeyUUID(String eventKeyUUID) {
	    this.eventKeyUUID = eventKeyUUID;
	}
	public String getGooruUId() {
		return gooruUId;
	}
	public void setGooruUId(String gooruUId) {
		this.gooruUId = gooruUId;
	}
	/**
	 * @param eventKeyUUID the eventKeyUUID to set
	 */
	public String getGooruId() {
		return gooruOid;
	}
	public void setGooruId(String gooruOid) {
		this.gooruOid = gooruOid;
	}
	public String getParentEventId() {
		return parentEventId;
	}
	public void setParentEventId(String parentEventId) {
		this.parentEventId = parentEventId;
	}	
	/**
	 * @return the eventKeyUUID
	 */
	public String getReactionType() {
		return reactionType;
	}
	public void setReactionType(String reactionType) {
		this.reactionType = reactionType;
	}

	public String getCountry() {
		return country;
	}
	public void setCountry(String country) {
		this.country = country;
	}
	public String getCity() {
		return city;
	}
	public void setCity(String city) {
		this.city = city;
	}
	public Long getTimeSpentInMs() {
		return timeSpentInMs;
	}
	public void setTimeSpentInMs(Long timeSpentInMs) {
		this.timeSpentInMs = timeSpentInMs;
	}
	public String getRequestMethod() {
		return requestMethod;
	}
	public void setRequestMethod(String requestMethod) {
		this.requestMethod = requestMethod;
	}
	public String getAttemptFirstStatus() {
		return attemptFirstStatus;
	}
	public void setAttemptFirstStatus(String attemptFirstStatus) {
		this.attemptFirstStatus = attemptFirstStatus;
	}
	public int getAnswerFirstId() {
		return answerFirstId;
	}
	public void setAnswerFirstId(int answerFirstId) {
		this.answerFirstId = answerFirstId;
	}
	public int[] getAttemptTrySequence() {
		return attemptTrySequence;
	}
	public void setAttemptTrySequence(int[] attemptTrySequence) {
		this.attemptTrySequence = attemptTrySequence;
	}
	public int[] getAttemptStatus() {
		return attemptStatus;
	}
	public void setAttemptStatus(int[] attemptStatus) {
		this.attemptStatus = attemptStatus;
	}
	public int[] getAnswerId() {
		return answerId;
	}
	public void setAnswerId(int[] answerId) {
		this.answerId = answerId;
	}
	public String getContextInfo() {
		return contextInfo;
	}
	public void setContextInfo(String contextInfo) {
		this.contextInfo = contextInfo;
	}
	public String getCollaboratorIds() {
		return collaboratorIds;
	}
	public void setCollaboratorIds(String collaboratorIds) {
		this.collaboratorIds = collaboratorIds;
	}
	public boolean isMobileData() {
		return mobileData;
	}
	public void setMobileData(boolean mobileData) {
		this.mobileData = mobileData;
	}
	public Integer getHintId() {
		return hintId;
	}
	public void setHintId(Integer hintId) {
		this.hintId = hintId;
	}
	public String getVersion() {
		return version;
	}
	public void setVersion(String version) {
		this.version = version;
	}
}
