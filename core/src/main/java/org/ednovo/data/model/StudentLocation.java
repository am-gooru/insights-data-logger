package org.ednovo.data.model;

import java.io.Serializable;

public class StudentLocation implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private String userUid;
	
	private String classUid;

	private String courseUid;

	private String unitUid;

	private String lessonUid;
	
	private String collectionUid;
	
	private String collectionType;
	
	private String resourceUid;
	
	private long sessionTime;

	public String getUserUid() {
		return userUid;
	}

	public void setUserUid(String userUid) {
		this.userUid = userUid;
	}

	public String getClassUid() {
		return classUid;
	}

	public void setClassUid(String classUid) {
		this.classUid = classUid;
	}

	public String getCourseUid() {
		return courseUid;
	}

	public void setCourseUid(String courseUid) {
		this.courseUid = courseUid;
	}

	public String getUnitUid() {
		return unitUid;
	}

	public void setUnitUid(String unitUid) {
		this.unitUid = unitUid;
	}

	public String getLessonUid() {
		return lessonUid;
	}

	public void setLessonUid(String lessonUid) {
		this.lessonUid = lessonUid;
	}

	public String getCollectionUid() {
		return collectionUid;
	}

	public void setCollectionUid(String collectionUid) {
		this.collectionUid = collectionUid;
	}

	public String getResourceUid() {
		return resourceUid;
	}

	public void setResourceUid(String resourceUid) {
		this.resourceUid = resourceUid;
	}

	public long getSessionTime() {
		return sessionTime;
	}

	public void setSessionTime(long sessionTime) {
		this.sessionTime = sessionTime;
	}

	public String getCollectionType() {
		return collectionType;
	}

	public void setCollectionType(String collectionType) {
		this.collectionType = collectionType;
	}

}
