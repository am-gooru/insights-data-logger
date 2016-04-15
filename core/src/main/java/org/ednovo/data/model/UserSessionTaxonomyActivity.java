package org.ednovo.data.model;

import java.io.Serializable;

import org.json.JSONArray;

public class UserSessionTaxonomyActivity implements Serializable, Cloneable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private String sessionId;
	
	private String gooruOid;
	
	private String subjectId;
	
	private String courseId;
	
	private String domainId;
	
	private String standardsId;

	private String learningTargetsId;

	private String resourceType;
 	
	private String questionType;
 		
	private String answerStatus;
	
	private long views;

	private long timeSpent;
	
	private long score;

	private long reaction;

	private JSONArray taxonomyIds;
	
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

	public String getSubjectId() {
		return subjectId;
	}

	public void setSubjectId(String subjectId) {
		this.subjectId = subjectId;
	}

	public String getCourseId() {
		return courseId;
	}

	public void setCourseId(String courseId) {
		this.courseId = courseId;
	}

	public String getDomainId() {
		return domainId;
	}

	public void setDomainId(String domainId) {
		this.domainId = domainId;
	}

	public String getStandardsId() {
		return standardsId;
	}

	public void setStandardsId(String standardsId) {
		this.standardsId = standardsId;
	}

	public String getLearningTargetsId() {
		return learningTargetsId;
	}

	public void setLearningTargetsId(String learningTargetsId) {
		this.learningTargetsId = learningTargetsId;
	}

	public String getResourceType() {
		return resourceType;
	}

	public void setResourceType(String resourceType) {
		this.resourceType = resourceType;
	}

	public String getQuestionType() {
		return questionType;
	}

	public void setQuestionType(String questionType) {
		this.questionType = questionType;
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

	public long getReaction() {
		return reaction;
	}

	public void setReaction(long reaction) {
		this.reaction = reaction;
	}

	public JSONArray getTaxonomyIds() {
		return taxonomyIds;
	}

	public void setTaxonomyIds(JSONArray taxonomyIds) {
		this.taxonomyIds = taxonomyIds;
	}
}
