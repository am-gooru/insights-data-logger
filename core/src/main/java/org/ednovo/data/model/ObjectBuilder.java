package org.ednovo.data.model;

import org.logger.event.cassandra.loader.Constants;
import org.logger.event.cassandra.loader.LoaderConstants;
import org.logger.event.cassandra.loader.dao.BaseCassandraRepo;
import org.logger.event.cassandra.loader.dao.BaseDAOCassandraImpl;

public class ObjectBuilder extends BaseDAOCassandraImpl {

	private  BaseCassandraRepo baseCassandraDao;
	
	private UserSessionActivity userSessionActivity = null;

	private UserSessionActivity userAllSessionActivity = null;
	
	private StudentsClassActivity studentsClassActivity = null;
	
	private ClassActivityDatacube classActivityDatacube = null;
	
	private StudentLocation studentLocation = null;
	
	private ContentTaxonomyActivity contentTaxonomyActivity = null;
	
	private EventBuilder event;
	
	public ObjectBuilder(EventBuilder event){
		baseCassandraDao = BaseCassandraRepo.instance();
		this.event = event;
		objectCreator();
		sessionActivityObjectCreator();
		studentLocationObjectCreator();
		classActivityObjectCreator();
		classDataCubeObjectCreator();
		allSessionActivityCreator();
		contentTaxonomyActivityObjectCreator();
	}
	private void objectCreator(){
		if(event.getEventName().matches(Constants.PLAY_EVENTS)){
			userSessionActivity = new UserSessionActivity();
			userAllSessionActivity = new UserSessionActivity();
			studentLocation = new StudentLocation();
			if(LoaderConstants.CPV1.getName().equalsIgnoreCase(event.getEventName()) || (LoaderConstants.CRPV1.getName().equalsIgnoreCase(event.getEventName()) || event.getCollectionType().equalsIgnoreCase(Constants.COLLECTION))){
				studentsClassActivity = new StudentsClassActivity();
				classActivityDatacube = new ClassActivityDatacube();
			}
			if(LoaderConstants.CRPV1.getName().equalsIgnoreCase(event.getEventName()) && event.getEventType().equalsIgnoreCase(Constants.STOP)){
				contentTaxonomyActivity = new ContentTaxonomyActivity();
			}
		}
	}

	private void sessionActivityObjectCreator(){
		/**
		 * Build UserSessionActivity
		 */
		if(userSessionActivity != null){
			userSessionActivity.setSessionId(event.getSessionId());
			userSessionActivity.setGooruOid(event.getContentGooruId());
			userSessionActivity.setParentGooruOid(event.getParentGooruId());
			userSessionActivity.setCollectionItemId(event.getCollectionItemId());
			if(event.getAnswerObject() != null && !event.getAnswerObject().equals(Constants.NA)){				
				userSessionActivity.setAnswerObject(event.getAnswerObject());
			}
			userSessionActivity.setAttempts(0);
			userSessionActivity.setCollectionType(event.getCollectionType());
			if(LoaderConstants.CPV1.getName().equalsIgnoreCase(event.getEventName())){
				userSessionActivity.setResourceType(event.getCollectionType());
			}else{
				userSessionActivity.setResourceType(event.getResourceType());
			}
			userSessionActivity.setQuestionType(event.getQuestionType());
			userSessionActivity.setEventType(event.getEventType());
			userSessionActivity.setAnswerStatus(event.getAnswerStatus());
			userSessionActivity.setAnswerObject(event.getAnswerObject());
			userSessionActivity.setReaction(event.getReaction());
			userSessionActivity.setTimeSpent(event.getTimespent());
			userSessionActivity.setViews(event.getViews());
			userSessionActivity.setScore(event.getScore());
			if ((LoaderConstants.CPV1.getName().equalsIgnoreCase(event.getEventName())) && Constants.STOP.equals(event.getEventType())) {
				if(event.getGradeType().equalsIgnoreCase(Constants.SYSTEM)){
					baseCassandraDao.getSessionScore(userSessionActivity,event.getEventName());
					userSessionActivity.setScore(userSessionActivity.getScore());
				}
			}
			
		}
	}
	
	private void studentLocationObjectCreator(){

		/**
		 * Build studentLocation
		 */
		if(studentLocation != null){
			studentLocation.setUserUid(event.getGooruUUID());
			studentLocation.setClassUid(event.getClassGooruId());
			studentLocation.setCourseUid(event.getCourseGooruId());
			studentLocation.setUnitUid(event.getUnitGooruId());
			studentLocation.setLessonUid(event.getLessonGooruId());
			if (event.getEventName().equalsIgnoreCase(LoaderConstants.CRPV1.getName()) || event.getEventName().equalsIgnoreCase(LoaderConstants.CRAV1.getName()) ) {
				studentLocation.setCollectionUid(event.getParentGooruId());
			} else if(event.getEventName().equalsIgnoreCase(LoaderConstants.CPV1.getName())){
				studentLocation.setCollectionUid(event.getContentGooruId());
			}
			studentLocation.setCollectionType(event.getCollectionType());
			studentLocation.setResourceUid(event.getContentGooruId());
			studentLocation.setSessionTime(event.getEventTime());
		}
	}
	
	private void classActivityObjectCreator(){

		/**
		 * Build studentClassActivity
		 */
		
		if(studentsClassActivity != null){
			studentsClassActivity.setClassUid(event.getClassGooruId());
			studentsClassActivity.setCourseUid(event.getCourseGooruId());
			studentsClassActivity.setUnitUid(event.getUnitGooruId());
			studentsClassActivity.setLessonUid(event.getLessonGooruId());
			studentsClassActivity.setCollectionType(event.getCollectionType());
			if (event.getEventName().equalsIgnoreCase(LoaderConstants.CRPV1.getName()) || event.getEventName().equalsIgnoreCase(LoaderConstants.CRAV1.getName())) {
				studentsClassActivity.setCollectionUid(event.getParentGooruId());
				studentsClassActivity.setViews(0);
			} else if(event.getEventName().equalsIgnoreCase(LoaderConstants.CPV1.getName())){
				studentsClassActivity.setCollectionUid(event.getContentGooruId());
				studentsClassActivity.setViews(event.getViews());
			}
			studentsClassActivity.setAttemptStatus(Constants.INPROGRESS);
			if ((LoaderConstants.CPV1.getName().equalsIgnoreCase(event.getEventName())) && Constants.STOP.equals(event.getEventType())) {
				studentsClassActivity.setAttemptStatus(Constants.ATTEMPTED);
				if(event.getGradeType().equalsIgnoreCase(Constants.SYSTEM)){
					studentsClassActivity.setAttemptStatus(Constants.COMPLETED);
					baseCassandraDao.getSessionScore(userSessionActivity,event.getEventName());
					event.setScore(userSessionActivity.getScore());
				}
			}
			studentsClassActivity.setUserUid(event.getGooruUUID());
			studentsClassActivity.setScore(event.getScore());
			studentsClassActivity.setTimeSpent(event.getTimespent());
		}
	}
	
	private void classDataCubeObjectCreator(){
		/**
		 * Build classActivityDatacube
		 */
		if(classActivityDatacube != null){
			classActivityDatacube.setRowKey(appendTildaSeperator(studentsClassActivity.getClassUid(), studentsClassActivity.getCourseUid(), studentsClassActivity.getUnitUid(),
					studentsClassActivity.getLessonUid()));
			classActivityDatacube.setLeafNode(studentsClassActivity.getCollectionUid());
			classActivityDatacube.setUserUid(studentsClassActivity.getUserUid());
			classActivityDatacube.setCollectionType(studentsClassActivity.getCollectionType());
			classActivityDatacube.setViews(studentsClassActivity.getViews());
			classActivityDatacube.setTimeSpent(studentsClassActivity.getTimeSpent());
			classActivityDatacube.setScore(studentsClassActivity.getScore());
		}
	}
	
	private void contentTaxonomyActivityObjectCreator() {
		if (contentTaxonomyActivity != null) {
			contentTaxonomyActivity.setClassUid(event.getClassGooruId());
			contentTaxonomyActivity.setGooruOid(event.getContentGooruId());
			contentTaxonomyActivity.setTaxonomyIds(event.getTaxonomyIds());
			contentTaxonomyActivity.setUserUid(event.getGooruUUID());
			contentTaxonomyActivity.setTimeSpent(event.getTimespent());
			contentTaxonomyActivity.setViews(event.getViews());
			contentTaxonomyActivity.setScore(event.getScore());
		}
	}
	
	private void allSessionActivityCreator(){
		if(userAllSessionActivity != null){
			try {
				userAllSessionActivity = (UserSessionActivity) userSessionActivity.clone();
			} catch (CloneNotSupportedException e) {
				e.printStackTrace();
			}
			userAllSessionActivity.setSessionId(appendTildaSeperator(Constants.AS, userSessionActivity.getParentGooruOid(), event.getGooruUUID()));
		}		
	}
	
	public UserSessionActivity getUserSessionActivity() {
		return userSessionActivity;
	}


	public void setUserSessionActivity(UserSessionActivity userSessionActivity) {
		this.userSessionActivity = userSessionActivity;
	}


	public UserSessionActivity getUserAllSessionActivity() {
		return userAllSessionActivity;
	}


	public void setUserAllSessionActivity(UserSessionActivity userAllSessionActivity) {
		this.userAllSessionActivity = userAllSessionActivity;
	}


	public StudentsClassActivity getStudentsClassActivity() {
		return studentsClassActivity;
	}


	public void setStudentsClassActivity(StudentsClassActivity studentsClassActivity) {
		this.studentsClassActivity = studentsClassActivity;
	}


	public ClassActivityDatacube getClassActivityDatacube() {
		return classActivityDatacube;
	}


	public void setClassActivityDatacube(ClassActivityDatacube classActivityDatacube) {
		this.classActivityDatacube = classActivityDatacube;
	}


	public StudentLocation getStudentLocation() {
		return studentLocation;
	}


	public void setStudentLocation(StudentLocation studentLocation) {
		this.studentLocation = studentLocation;
	}


	public ContentTaxonomyActivity getContentTaxonomyActivity() {
		return contentTaxonomyActivity;
	}


	public void setContentTaxonomyActivity(ContentTaxonomyActivity contentTaxonomyActivity) {
		this.contentTaxonomyActivity = contentTaxonomyActivity;
	}
}
