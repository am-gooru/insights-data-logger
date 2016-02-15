package org.ednovo.data.model;

import java.util.Map;

import org.logger.event.cassandra.loader.Constants;
import org.logger.event.cassandra.loader.DataUtils;
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
	
	private String gooruUUID;
	
	private String eventName;
	
	private String contentGooruId;
	
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
	
	private String answerObject;
	
	private String answerStatus;
	
	private String gradeType;
	
	private String[] taxonomyIds;
	
	private long eventTime;
	
	private long score;
	
	private long timespent;
	
	private long views;
	
	private int attempts;
	
	private long reaction;
	
	public ObjectBuilder(Map<String, Object> eventMap){
		baseCassandraDao = BaseCassandraRepo.instance();
		gooruUUID = setNullIfEmpty(eventMap, Constants.GOORUID);
		eventName = setNullIfEmpty(eventMap, Constants.EVENT_NAME);
		contentGooruId = setNAIfNull(eventMap, Constants.CONTENT_GOORU_OID);
		lessonGooruId = setNAIfNull(eventMap, Constants.LESSON_GOORU_OID);
		unitGooruId = setNAIfNull(eventMap, Constants.UNIT_GOORU_OID);
		courseGooruId = setNAIfNull(eventMap, Constants.COURSE_GOORU_OID);
		classGooruId = setNAIfNull(eventMap, Constants.CLASS_GOORU_OID);
		parentGooruId = setNAIfNull(eventMap, Constants.PARENT_GOORU_OID);
		collectionItemId = setNAIfNull(eventMap, Constants.COLLECTION_ITEM_ID);
		sessionId = setNAIfNull(eventMap, Constants.SESSION_ID);
		eventType = setNAIfNull(eventMap, Constants.TYPE);
		collectionType = eventMap.get(Constants.COLLECTION_TYPE).equals(Constants.COLLECTION) ? Constants.COLLECTION : Constants.ASSESSMENT;
		questionType = setNAIfNull(eventMap, Constants.QUESTION_TYPE);
		resourceType = setNAIfNull(eventMap, Constants.RESOURCE_TYPE);
		answerObject = setNAIfNull(eventMap, Constants.ANSWER_OBECT);
		answerStatus = Constants.NA;
		gradeType = eventMap.containsValue(Constants.GRADE_TYPE) ? (String)eventMap.get(Constants.GRADE_TYPE) : Constants.SYSTEM;
		taxonomyIds = (String[]) (eventMap.containsKey("taxonomyIds") ? TypeConverter.stringToIntArray((String) eventMap.get("taxonomyIds")) : null);
		eventTime = ((Number) eventMap.get(Constants.END_TIME)).longValue();
		score = 0;
		timespent = 0;
		if (!(LoaderConstants.CPV1.getName().equalsIgnoreCase(eventName) && Constants.COLLECTION.equalsIgnoreCase(collectionType)
				&& eventType.equalsIgnoreCase(Constants.STOP))) {
			timespent = setLongZeroIfNull(eventMap, Constants.TOTALTIMEINMS);
		}
		views = setLongZeroIfNull(eventMap, Constants.VIEWS_COUNT);
		attempts = setIntegerZeroIfNull(eventMap, Constants.ATTEMPT_COUNT);
		reaction = eventMap.containsKey(Constants.REACTION_TYPE) ? DataUtils.formatReactionString((String) eventMap.get(Constants.REACTION_TYPE)) : 0;
		objectCreator();
		sessionActivityObjectCreator();
		studentLocationObjectCreator();
		classActivityObjectCreator();
		classDataCubeObjectCreator();
		allSessionActivityCreator();
	}
	
	private void objectCreator(){
		if(eventName.matches(Constants.PLAY_EVENTS)){
			userSessionActivity = new UserSessionActivity();
			userAllSessionActivity = new UserSessionActivity();
			studentLocation = new StudentLocation();
			if(LoaderConstants.CPV1.getName().equalsIgnoreCase(eventName) || (LoaderConstants.CRPV1.getName().equalsIgnoreCase(eventName) || collectionType.equalsIgnoreCase(Constants.COLLECTION))){
				studentsClassActivity = new StudentsClassActivity();
				classActivityDatacube = new ClassActivityDatacube();
			}
			if(LoaderConstants.CRPV1.getName().equalsIgnoreCase(eventName) && eventType.equalsIgnoreCase(Constants.STOP)){
				contentTaxonomyActivity = new ContentTaxonomyActivity();
			}
		}
	}

	private void sessionActivityObjectCreator(){
		/**
		 * Build UserSessionActivity
		 */
		if(userSessionActivity != null){
			userSessionActivity.setSessionId(sessionId);
			userSessionActivity.setGooruOid(contentGooruId);
			userSessionActivity.setParentGooruOid(parentGooruId);
			userSessionActivity.setCollectionItemId(collectionItemId);
			userSessionActivity.setAnswerObject(answerObject);
			userSessionActivity.setAttempts(attempts);
			userSessionActivity.setCollectionType(collectionType);
			if(LoaderConstants.CPV1.getName().equalsIgnoreCase(eventName)){
				userSessionActivity.setResourceType(collectionType);
			}else{
				userSessionActivity.setResourceType(resourceType);
			}
			userSessionActivity.setQuestionType(questionType);
			userSessionActivity.setEventType(eventType);
			userSessionActivity.setAnswerStatus(answerStatus);
			userSessionActivity.setReaction(reaction);
			userSessionActivity.setTimeSpent(timespent);
			userSessionActivity.setViews(views);
			if ((LoaderConstants.CPV1.getName().equalsIgnoreCase(eventName)) && Constants.STOP.equals(eventType)) {
				if(gradeType.equalsIgnoreCase(Constants.SYSTEM)){
					baseCassandraDao.getSessionScore(userSessionActivity,eventName);
				}
			}
			userSessionActivity.setScore(userSessionActivity.getScore());
			
		}
	}
	
	private void studentLocationObjectCreator(){

		/**
		 * Build studentLocation
		 */
		if(studentLocation != null){
			studentLocation.setUserUid(gooruUUID);
			studentLocation.setClassUid(classGooruId);
			studentLocation.setCourseUid(courseGooruId);
			studentLocation.setUnitUid(unitGooruId);
			studentLocation.setLessonUid(lessonGooruId);
			if (eventName.equalsIgnoreCase(LoaderConstants.CRPV1.getName()) || eventName.equalsIgnoreCase(LoaderConstants.CRAV1.getName()) ) {
				studentLocation.setCollectionUid(parentGooruId);
			} else if(eventName.equalsIgnoreCase(LoaderConstants.CPV1.getName())){
				studentLocation.setCollectionUid(contentGooruId);
			}
			studentLocation.setCollectionType(collectionType);
			studentLocation.setResourceUid(contentGooruId);
			studentLocation.setSessionTime(eventTime);
		}
	}
	
	private void classActivityObjectCreator(){

		/**
		 * Build studentClassActivity
		 */
		
		if(studentsClassActivity != null){
			studentsClassActivity.setClassUid(classGooruId);
			studentsClassActivity.setCourseUid(courseGooruId);
			studentsClassActivity.setUnitUid(unitGooruId);
			studentsClassActivity.setLessonUid(lessonGooruId);
			studentsClassActivity.setCollectionType(collectionType);
			if (eventName.equalsIgnoreCase(LoaderConstants.CRPV1.getName()) || eventName.equalsIgnoreCase(LoaderConstants.CRAV1.getName())) {
				studentsClassActivity.setCollectionUid(parentGooruId);
				studentsClassActivity.setViews(0);
			} else if(eventName.equalsIgnoreCase(LoaderConstants.CPV1.getName())){
				studentsClassActivity.setCollectionUid(contentGooruId);
				studentsClassActivity.setViews(views);
			}
			studentsClassActivity.setAttemptStatus(Constants.INPROGRESS);
			if ((LoaderConstants.CPV1.getName().equalsIgnoreCase(eventName)) && Constants.STOP.equals(eventType)) {
				studentsClassActivity.setAttemptStatus(Constants.ATTEMPTED);
				if(gradeType.equalsIgnoreCase(Constants.SYSTEM)){
					studentsClassActivity.setAttemptStatus(Constants.COMPLETED);
					baseCassandraDao.getSessionScore(userSessionActivity,eventName);
					score = userSessionActivity.getScore();
				}
			}
			studentsClassActivity.setUserUid(gooruUUID);
			studentsClassActivity.setScore(score);
			studentsClassActivity.setTimeSpent(timespent);
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
	
	private void allSessionActivityCreator(){
		if(userAllSessionActivity != null){
			try {
				userAllSessionActivity = (UserSessionActivity) userSessionActivity.clone();
			} catch (CloneNotSupportedException e) {
				e.printStackTrace();
			}
			userAllSessionActivity.setSessionId(appendTildaSeperator(Constants.AS, userSessionActivity.getParentGooruOid(), gooruUUID));
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
