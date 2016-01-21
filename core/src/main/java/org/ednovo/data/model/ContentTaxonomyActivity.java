package org.ednovo.data.model;

public class ContentTaxonomyActivity {

	private String userUid;
	
	private String subjectId;
	
	private String courseId;
	
	private String domainId;
	
	private String subDomainId;
	
	private String standardsId;

	private String learningTargetsId;
	
	private String gooruOid;
	
	private String classUid;
	
	private String resourceType;

	private String questionType;
	
	private String[] taxonomyIds;
	
	private long views;

	private long timeSpent;

	private long score;

	public Object clone()throws CloneNotSupportedException{  
		return super.clone();  	
	}  
	
	public String getUserUid() {
		return userUid;
	}

	public void setUserUid(String userUid) {
		this.userUid = userUid;
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

	public String getSubDomainId() {
		return subDomainId;
	}

	public void setSubDomainId(String subDomainId) {
		this.subDomainId = subDomainId;
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

	public String getGooruOid() {
		return gooruOid;
	}

	public void setGooruOid(String gooruOid) {
		this.gooruOid = gooruOid;
	}

	public String getClassUid() {
		return classUid;
	}

	public void setClassUid(String classUid) {
		this.classUid = classUid;
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

	public String[] getTaxonomyIds() {
		return taxonomyIds;
	}

	public void setTaxonomyIds(String[] taxonomyIds) {
		this.taxonomyIds = taxonomyIds;
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
	
}
