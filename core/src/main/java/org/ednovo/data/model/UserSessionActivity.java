package org.ednovo.data.model;

import java.io.Serializable;

public class UserSessionActivity implements Serializable, Cloneable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private String sessionId;
	
	private String gooruOid;
	
	private String parentGooruOid;
	
	private String collectionItemId;
	
	private String collectionType;

	private String resourceType;
 	
	private String questionType;
 	
	private String eventType;
	
	private String answerStatus;
	
	private long views;

	private long timeSpent;
	
	private long score;
	
	private long attempts;
	
	private long reaction;
	
	private String answerObject;

	public Object clone()throws CloneNotSupportedException{  
		return super.clone();  	
	}  
	
	public String getSessionId() {
		return sessionId;
	}

	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}

	public String getGooruOid() {
		return gooruOid;
	}

	public void setGooruOid(String gooruOid) {
		this.gooruOid = gooruOid;
	}

	public String getCollectionItemId() {
		return collectionItemId;
	}

	public void setCollectionItemId(String collectionItemId) {
		this.collectionItemId = collectionItemId;
	}

	public String getResourceType() {
		return resourceType;
	}

	public void setResourceType(String resourceType) {
		this.resourceType = resourceType;
	}

	public String getAnswerStatus() {
		return answerStatus;
	}

	public void setAnswerStatus(String answerStatus) {
		this.answerStatus = answerStatus;
	}

	public long getViews() {
		return views;
	}

	public void setViews(long views) {
		this.views = views;
	}

	public long getTimeSpent() {
		return timeSpent;
	}

	public void setTimeSpent(long timeSpent) {
		this.timeSpent = timeSpent;
	}

	public long getScore() {
		return score;
	}

	public void setScore(long score) {
		this.score = score;
	}

	public long getAttempts() {
		return attempts;
	}

	public void setAttempts(long attempts) {
		this.attempts = attempts;
	}

	public long getReaction() {
		return reaction;
	}

	public void setReaction(long reaction) {
		this.reaction = reaction;
	}

	public String getAnswerObject() {
		return answerObject;
	}

	public void setAnswerObject(String answerObject) {
		this.answerObject = answerObject;
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

	public String getParentGooruOid() {
		return parentGooruOid;
	}

	public void setParentGooruOid(String parentGooruOid) {
		this.parentGooruOid = parentGooruOid;
	}

}
