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

import org.apache.commons.lang.StringUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.logger.event.cassandra.loader.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventBuilder {
	private static final Logger LOG = LoggerFactory.getLogger(EventBuilder.class);
	
	private JSONObject event;

	public EventBuilder(String json) throws JSONException {
		this.event = new JSONObject(json);
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

	private String parentEventId;
	
	private String gooruUUID;
		
	private String lessonGooruId;
	
	private String unitGooruId;
	
	private String courseGooruId;
	
	private String classGooruId;
	
	private String parentGooruId;
	
	private String collectionItemId;
	
	private String sessionId;
	
	private String eventType;
	
	private String collectionType;
	
	private String questionType;
	
	private String resourceType;
	
	private Object answerObject;
	
	private String answerStatus;
	
	private String gradeType;
	
	private String gradeStatus;

	private String teacherId;
	
	private JSONArray taxonomyIds;
	
	private JSONArray collaborators;
	
	private String contentFormat;
	
	private long eventTime;
	
	private long score;
	
	private long timespent;
	
	private long views;
	
	private int attempts;
	
	private long reaction;
	
	private String reportsContext;
	
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

	public String getGooruUUID() {
		return gooruUUID;
	}

	public void setGooruUUID(String gooruUUID) {
		this.gooruUUID = gooruUUID;
	}

	public JSONObject getEvent() {
		return event;
	}

	public void setEvent(JSONObject event) {
		this.event = event;
	}

	public String getLessonGooruId() {
		return lessonGooruId;
	}

	public void setLessonGooruId(String lessonGooruId) {
		this.lessonGooruId = lessonGooruId;
	}

	public String getUnitGooruId() {
		return unitGooruId;
	}

	public void setUnitGooruId(String unitGooruId) {
		this.unitGooruId = unitGooruId;
	}

	public String getCourseGooruId() {
		return courseGooruId;
	}

	public void setCourseGooruId(String courseGooruId) {
		this.courseGooruId = courseGooruId;
	}

	public String getClassGooruId() {
		return classGooruId;
	}

	public void setClassGooruId(String classGooruId) {
		this.classGooruId = classGooruId;
	}

	public String getParentGooruId() {
		return parentGooruId;
	}

	public void setParentGooruId(String parentGooruId) {
		this.parentGooruId = parentGooruId;
	}

	public String getCollectionItemId() {
		return collectionItemId;
	}

	public void setCollectionItemId(String collectionItemId) {
		this.collectionItemId = collectionItemId;
	}

	public String getSessionId() {
		return sessionId;
	}

	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}

	public String getEventType() {
		return eventType;
	}

	public void setEventType(String eventType) {
		this.eventType = eventType;
	}

	public String getCollectionType() {
		return collectionType;
	}

	public void setCollectionType(String collectionType) {
		this.collectionType = collectionType;
	}

	public String getQuestionType() {
		return questionType;
	}

	public void setQuestionType(String questionType) {
		this.questionType = questionType;
	}

	public String getResourceType() {
		return resourceType;
	}

	public void setResourceType(String resourceType) {
		this.resourceType = resourceType;
	}

	public Object getAnswerObject() {
		return answerObject;
	}

	public void setAnswerObject(Object answerObject) {
		this.answerObject = answerObject;
	}

	public String getAnswerStatus() {
		return answerStatus;
	}

	public void setAnswerStatus(String answerStatus) {
		this.answerStatus = answerStatus;
	}

	public String getGradeType() {
		return gradeType;
	}

	public void setGradeType(String gradeType) {
		this.gradeType = gradeType;
	}

	public JSONArray getTaxonomyIds() {
		return taxonomyIds;
	}

	public void setTaxonomyIds(JSONArray taxonomyIds) {
		this.taxonomyIds = taxonomyIds;
	}

	public long getEventTime() {
		return eventTime;
	}

	public void setEventTime(long eventTime) {
		this.eventTime = eventTime;
	}

	public long getScore() {
		return score;
	}

	public void setScore(long score) {
		this.score = score;
	}

	public long getTimespent() {
		return timespent;
	}

	public void setTimespent(long timespent) {
		this.timespent = timespent;
	}

	public long getViews() {
		return views;
	}

	public void setViews(long views) {
		this.views = views;
	}

	public int getAttempts() {
		return attempts;
	}

	public void setAttempts(int attempts) {
		this.attempts = attempts;
	}

	public long getReaction() {
		return reaction;
	}

	public void setReaction(long reaction) {
		this.reaction = reaction;
	}
	public String getGradeStatus() {
		return gradeStatus;
	}

	public void setGradeStatus(String gradeStatus) {
		this.gradeStatus = gradeStatus;
	}

	public String getTeacherId() {
		return teacherId;
	}

	public void setTeacherId(String teacherId) {
		this.teacherId = teacherId;
	}

	public EventBuilder build() {
		try {
			this.context = this.event.getJSONObject(Constants.CONTEXT);
			this.user = this.event.getJSONObject(Constants.USER);
			this.payLoadObject = this.event.getJSONObject(Constants.PAY_LOAD);
			this.metrics = this.event.getJSONObject(Constants.METRICS);
			this.session = this.event.getJSONObject(Constants.SESSION);
			this.version = this.event.getJSONObject(Constants.VERSION);
			this.startTime = this.event.getLong(Constants.START_TIME);
			this.endTime = this.event.getLong(Constants.END_TIME);
			this.eventId = this.event.getString(Constants.EVENT_ID);
			this.eventName = this.event.getString(Constants.EVENT_NAME);
			this.apiKey = session.isNull(Constants.API_KEY) ? Constants.NA : session.getString(Constants.API_KEY);
			this.contentGooruId = context.isNull(Constants.CONTENT_GOORU_OID) ? Constants.NA : context.getString(Constants.CONTENT_GOORU_OID);
			this.gooruUUID = user.getString(Constants.GOORUID);
			this.lessonGooruId = context.isNull(Constants.LESSON_GOORU_OID) ? Constants.NA : context.getString(Constants.LESSON_GOORU_OID);
			this.unitGooruId = context.isNull(Constants.UNIT_GOORU_OID) ? Constants.NA : context.getString(Constants.UNIT_GOORU_OID);
			this.courseGooruId = context.isNull(Constants.COURSE_GOORU_OID) ? Constants.NA : context.getString(Constants.COURSE_GOORU_OID);
			this.classGooruId = context.isNull(Constants.CLASS_GOORU_OID) ? Constants.NA : context.getString(Constants.CLASS_GOORU_OID);
			this.parentGooruId = context.isNull(Constants.PARENT_GOORU_OID) ? Constants.NA : context.getString(Constants.PARENT_GOORU_OID);
			this.parentEventId = context.isNull(Constants.PARENT_EVENT_ID) ? Constants.NA : context.getString(Constants.PARENT_EVENT_ID);
			if(!context.isNull(Constants.COLLECTION_TYPE)){
				this.collectionType = context.getString(Constants.COLLECTION_TYPE).equals(Constants.COLLECTION) ? Constants.COLLECTION : Constants.ASSESSMENT;
			}
			this.resourceType = context.isNull(Constants.RESOURCE_TYPE) ? Constants.NA : context.getString(Constants.RESOURCE_TYPE);
			this.eventType =  context.isNull(Constants.TYPE) ? Constants.NA: context.getString(Constants.TYPE);
			this.sessionId = session.isNull(Constants.SESSION_ID) ? Constants.NA : session.getString(Constants.SESSION_ID);
			this.questionType = payLoadObject.isNull(Constants.QUESTION_TYPE) ? Constants.NA : payLoadObject.getString(Constants.QUESTION_TYPE);
			this.answerObject = payLoadObject.isNull(Constants.ANSWER_OBECT) ? Constants.NA : payLoadObject.get(Constants.ANSWER_OBECT);
			this.reportsContext = payLoadObject.isNull(Constants.REPORTS_CONTEXT) ? Constants.NA : payLoadObject.getString(Constants.REPORTS_CONTEXT);
			this.answerStatus = Constants.NA;
			this.gradeType = payLoadObject.isNull(Constants.GRADE_TYPE) ? Constants.SYSTEM : payLoadObject.getString(Constants.GRADE_TYPE);
			this.gradeStatus = payLoadObject.isNull(Constants.GRADE_STATUS) ? Constants.NA : payLoadObject.getString(Constants.GRADE_STATUS);
			this.teacherId = payLoadObject.isNull(Constants.TEACHER_ID) ? Constants.NA : payLoadObject.getString(Constants.TEACHER_ID);
			this.contentFormat = payLoadObject.isNull(Constants.CONTENT_FORMAT) ? Constants.NA : payLoadObject.getString(Constants.CONTENT_FORMAT);
			this.collaborators = payLoadObject.isNull(Constants.COLLABORATORS) ? new JSONArray() : (JSONArray) payLoadObject.get(Constants.COLLABORATORS);
			this.taxonomyIds = payLoadObject.isNull(Constants.TAXONOMYIDS) ? new JSONArray() : (JSONArray) payLoadObject.get(Constants.TAXONOMYIDS);
			
			this.eventTime = endTime;
			this.collectionItemId = Constants.NA;
			this.score = 0;
			this.reaction = context.isNull(Constants.REACTION_TYPE) ? 0 : context.getLong(Constants.REACTION_TYPE);

			if (eventName.matches(Constants.PLAY_EVENTS)) {
				this.views = 1L;
				this.timespent = (endTime - startTime);
				String collectionType = context.getString(Constants.COLLECTION_TYPE);
				if ((eventName.equalsIgnoreCase(Constants.REACTION_CREATE)) || (Constants.START.equals(eventType) && Constants.ASSESSMENT.equalsIgnoreCase(collectionType)) || (Constants.STOP.equals(eventType) && Constants.COLLECTION.equalsIgnoreCase(collectionType))) {
					views = 0L;
				}
				if (timespent > 7200000 || timespent < 0) {
					timespent = 7200000;
				}
				metrics.put(Constants.VIEWS_COUNT, views);
				metrics.put(Constants.TOTALTIMEINMS, timespent);
			}

			if (Constants.QUESTION.equals(resourceType) && (Constants.STOP.equals(eventType))) {
				answerStatus = payLoadObject.isNull(Constants.ATTEMPT_STATUS) ? Constants.ATTEMPTED : payLoadObject.getString(Constants.ATTEMPT_STATUS);
				
				if(StringUtils.isBlank(answerStatus)){
					answerStatus = Constants.ATTEMPTED;
					score = 0;
				} else if (answerStatus.equalsIgnoreCase(Constants.INCORRECT)) {
					score = 0;
				} else if (answerStatus.equalsIgnoreCase(Constants.CORRECT)) {
					score = 100;
				}
				LOG.info("answerStatus : " + answerStatus);

			}
		} catch (Exception e) {
			LOG.error("Exception:", e);
		}
		return this;
	}

	public String getParentEventId() {
		return parentEventId;
	}

	public void setParentEventId(String parentEventId) {
		this.parentEventId = parentEventId;
	}

	public String getContentFormat() {
		return contentFormat;
	}

	public void setContentFormat(String contentFormat) {
		this.contentFormat = contentFormat;
	}

	public JSONArray getCollaborators() {
		return collaborators;
	}

	public void setCollaborators(JSONArray collaborators) {
		this.collaborators = collaborators;
	}

	public String getReportsContext() {
		return reportsContext;
	}

	public void setReportsContext(String reportsContext) {
		this.reportsContext = reportsContext;
	}

}
