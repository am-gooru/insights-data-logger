package org.logger.event.cassandra.loader.dao;

/*******************************************************************************
 * 
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

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang.StringUtils;
import org.ednovo.data.model.ClassActivityDatacube;
import org.ednovo.data.model.ContentTaxonomyActivity;
import org.ednovo.data.model.StudentLocation;
import org.ednovo.data.model.StudentsClassActivity;
import org.ednovo.data.model.TypeConverter;
import org.ednovo.data.model.UserSessionActivity;
import org.json.JSONObject;
import org.logger.event.cassandra.loader.ColumnFamilySet;
import org.logger.event.cassandra.loader.Constants;
import org.logger.event.cassandra.loader.DataUtils;
import org.logger.event.cassandra.loader.LoaderConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;

public class MicroAggregatorDAOmpl extends BaseDAOCassandraImpl implements MicroAggregatorDAO, Constants {

	private static final Logger LOG = LoggerFactory.getLogger(MicroAggregatorDAOmpl.class);
	
	private final BaseCassandraRepoImpl baseCassandraDao;

	
	ExecutorService service = Executors.newFixedThreadPool(10);

	public MicroAggregatorDAOmpl() {
		this.baseCassandraDao = new BaseCassandraRepoImpl();
	}

	public void eventProcessor(Map<String, Object> eventMap) {
		try {
			String eventName = setNAIfNull(eventMap, EVENT_NAME);
			String[] taxonomyIds = (String[]) (eventMap.containsKey("taxonomyIds") ? TypeConverter.stringToIntArray((String) eventMap.get("taxonomyIds")) : null);
			if (eventName.matches(PLAY_EVENTS)) {
				UserSessionActivity userSessionActivity = new UserSessionActivity();
				StudentsClassActivity studentsClassActivity = new StudentsClassActivity();
				ClassActivityDatacube classActivityDatacube = new ClassActivityDatacube();
				StudentLocation studentLocation = new StudentLocation();
				UserSessionActivity userAllSessionActivity = null;
				ContentTaxonomyActivity contentTaxonomyActivity = new ContentTaxonomyActivity();

				generateDAOs(eventMap, userSessionActivity, studentsClassActivity, classActivityDatacube, studentLocation);
				userAllSessionActivity = (UserSessionActivity) userSessionActivity.clone();
				userAllSessionActivity.setSessionId(appendTildaSeperator(AS, studentsClassActivity.getCollectionUid(),studentsClassActivity.getUserUid()));

				if (LoaderConstants.CPV1.getName().equalsIgnoreCase(eventName) && COLLECTION.equalsIgnoreCase(userSessionActivity.getCollectionType())
						&& userSessionActivity.getEventType().equalsIgnoreCase(STOP)) {
					/**
					 * Collection timespent is already calculated in resource level. This custom code will avoid duplicate timespent addition for collections.
					 */
					userSessionActivity.setTimeSpent(0L);
					studentsClassActivity.setTimeSpent(0L);
				}
				baseCassandraDao.compareAndMergeUserSessionActivity(userSessionActivity);

				baseCassandraDao.saveUserSessionActivity(userSessionActivity);

				baseCassandraDao.compareAndMergeUserSessionActivity(userAllSessionActivity);

				baseCassandraDao.saveUserSessionActivity(userAllSessionActivity);

				if (LoaderConstants.CPV1.getName().equalsIgnoreCase(eventName)) {
					baseCassandraDao.saveUserSession(userSessionActivity.getSessionId(), studentsClassActivity.getClassUid(), studentsClassActivity.getCourseUid(), studentsClassActivity.getUnitUid(),
							studentsClassActivity.getLessonUid(), studentsClassActivity.getCollectionUid(), studentsClassActivity.getUserUid(), userSessionActivity.getCollectionType(),
							userSessionActivity.getEventType(), studentLocation.getSessionTime());
				}

				if (COLLECTION.equalsIgnoreCase(userSessionActivity.getCollectionType()) && LoaderConstants.CRPV1.getName().equalsIgnoreCase(eventName)
						&& userSessionActivity.getEventType().equalsIgnoreCase(STOP)) {
					UserSessionActivity userCollectionData = baseCassandraDao.getUserSessionActivity(userSessionActivity.getSessionId(), userSessionActivity.getParentGooruOid(), NA);
					UserSessionActivity userAllSessionCollectionActivity = baseCassandraDao.getUserSessionActivity(userAllSessionActivity.getSessionId(), userAllSessionActivity.getParentGooruOid(),
							NA);
					baseCassandraDao.getSessionScore(userSessionActivity, eventName);
					if (userCollectionData != null) {
						userCollectionData.setTimeSpent(userCollectionData.getTimeSpent() + userSessionActivity.getTimeSpent());
						userCollectionData.setScore(userSessionActivity.getScore());
						userCollectionData.setReaction(userSessionActivity.getReaction());
					}
					userAllSessionCollectionActivity.setTimeSpent(userAllSessionCollectionActivity.getTimeSpent() + userSessionActivity.getTimeSpent());
					userAllSessionCollectionActivity.setScore(userSessionActivity.getScore());
					studentsClassActivity.setScore(userSessionActivity.getScore());
					studentsClassActivity.setReaction(userSessionActivity.getReaction());
					userAllSessionCollectionActivity.setReaction(userSessionActivity.getScore());
					baseCassandraDao.saveUserSessionActivity(userCollectionData);
					baseCassandraDao.saveUserSessionActivity(userAllSessionCollectionActivity);
					studentsClassActivity.setTimeSpent(userSessionActivity.getTimeSpent());
					studentsClassActivity.setScore(userCollectionData.getScore());
					studentsClassActivity.setReaction(userCollectionData.getReaction());
				}
				if (!studentsClassActivity.getClassUid().equalsIgnoreCase(NA) && studentsClassActivity.getClassUid() != null) {

					baseCassandraDao.saveStudentLocation(studentLocation);

					// TODO Add validation using grading type
					if (LoaderConstants.CPV1.getName().equalsIgnoreCase(eventName)) {
						callClassActitivityDataCubeGenerator(studentsClassActivity, classActivityDatacube);
					}

					if (LoaderConstants.CRPV1.getName().equalsIgnoreCase(eventName) && userSessionActivity.getEventType().equalsIgnoreCase(STOP)) {
						if (COLLECTION.equalsIgnoreCase(studentsClassActivity.getCollectionType())) {
							callClassActitivityDataCubeGenerator(studentsClassActivity, classActivityDatacube);
						}
						contentTaxonomyActivity.setUserUid(studentsClassActivity.getUserUid());
						contentTaxonomyActivity.setViews(userSessionActivity.getViews());
						contentTaxonomyActivity.setTimeSpent(userSessionActivity.getTimeSpent());
						contentTaxonomyActivity.setScore(userSessionActivity.getScore());
						contentTaxonomyActivity.setTaxonomyIds(taxonomyIds);
						contentTaxonomyActivity.setClassUid(studentsClassActivity.getClassUid());
						service.submit(new MastryGenerator(contentTaxonomyActivity, baseCassandraDao));
					}
				}
				if (eventName.equalsIgnoreCase(LoaderConstants.CRAV1.getName())) {
					long reaction = DataUtils.formatReactionString((String) eventMap.get(REACTION_TYPE));
					userSessionActivity.setReaction(reaction);
					userAllSessionActivity.setReaction(reaction);
					baseCassandraDao.updateReaction(userAllSessionActivity);
					baseCassandraDao.updateReaction(userSessionActivity);
				}
			}
			if (eventName.equalsIgnoreCase(LoaderConstants.QUESTION_GRADE.getName())) {
				if (eventMap.get("gradeStatus").equals("save")) {
					baseCassandraDao.saveQuestionGrade((String) eventMap.get("teacherId"), (String) eventMap.get(GOORUID), (String) eventMap.get(SESSION_ID), (String) eventMap.get(CONTENT_GOORU_OID),
							((Number) eventMap.get(SCORE)).longValue());
				} else if (eventMap.get("gradeStatus").equals("submit")) {
					Rows<String, String> questionScores = baseCassandraDao.getQuestionsGradeBySessionId((String) eventMap.get("teacherId"), (String) eventMap.get(GOORUID),
							(String) eventMap.get(SESSION_ID));
					if (questionScores != null && questionScores.size() > 0) {
						for (Row<String, String> questionScore : questionScores) {
							ColumnList<String> score = questionScore.getColumns();
							baseCassandraDao.saveQuestionGradeInSession((String) eventMap.get(SESSION_ID), (String) eventMap.get(CONTENT_GOORU_OID), NA, score.getLongValue(SCORE, 0L));
						}
					}
					UserSessionActivity userCollectionData = baseCassandraDao.getUserSessionActivity((String) eventMap.get(SESSION_ID), (String) eventMap.get(PARENT_GOORU_OID), NA);
					baseCassandraDao.getSessionScore(userCollectionData, LoaderConstants.CPV1.getName());
					baseCassandraDao.saveQuestionGradeInSession((String) eventMap.get(SESSION_ID), (String) eventMap.get(PARENT_GOORU_OID), NA, userCollectionData.getScore());
				}
			}
		} catch (Exception e) {
			LOG.error("Exception:", e);
		}
	}

	private void callClassActitivityDataCubeGenerator(StudentsClassActivity studentsClassActivity, ClassActivityDatacube classActivityDatacube){
		baseCassandraDao.compareAndMergeStudentsClassActivity(studentsClassActivity);
		baseCassandraDao.saveStudentsClassActivity(studentsClassActivity);
		classActivityDatacube.setViews(studentsClassActivity.getViews());
		classActivityDatacube.setTimeSpent(studentsClassActivity.getTimeSpent());
		classActivityDatacube.setScore(studentsClassActivity.getScore());
		classActivityDatacube.setReaction(studentsClassActivity.getReaction());
		if(studentsClassActivity.getAttemptStatus().equalsIgnoreCase(COMPLETED)){
			classActivityDatacube.setCompletedCount(1L);
		}else{
			classActivityDatacube.setCompletedCount(0L);
		}
		baseCassandraDao.saveClassActivityDataCube(classActivityDatacube);
		service.submit(new ClassActivityDataCubeGenerator(studentsClassActivity,baseCassandraDao));	
	}
	/**
	 * Append string with ~ seperator
	 * @param columns
	 * @return
	 */
	private String appendTildaSeperator(String... columns) {
		StringBuilder columnKey = new StringBuilder();
		for (String column : columns) {
			if (StringUtils.isNotBlank(column)) {
				columnKey.append(columnKey.length() > 0 ? SEPERATOR : EMPTY);
				columnKey.append(column);
			}
		}
		return columnKey.toString();

	}

	/**
	 * Find if user already answered correct or in-correct
	 * @param key
	 * @param columnPrefix
	 * @return
	 */
	public boolean hasUserAlreadyAnswered(String key, String columnPrefix) {
		ColumnList<String> counterColumns = baseCassandraDao.readWithKey(ColumnFamilySet.SESSION_ACTIVITY.getColumnFamily(), key);
		boolean status = false;
		String attemptStatus = counterColumns.getColumnByName(columnPrefix + SEPERATOR + _QUESTION_STATUS) != null ? counterColumns.getStringValue(columnPrefix + SEPERATOR + _QUESTION_STATUS, null)
				: null;
		if (attemptStatus != null && attemptStatus.matches(ANSWERING_STATUS)) {
			status = true;
		}
		return status;
	}
	

	private void generateDAOs(Map<String, Object> eventMap, UserSessionActivity userSessionActivity, StudentsClassActivity studentsClassActivity, ClassActivityDatacube classActivityDataCube,
			StudentLocation studentLocation) {

		String gooruUUID = setNullIfEmpty(eventMap, GOORUID);
		String eventName = setNullIfEmpty(eventMap, EVENT_NAME);
		String contentGooruId = setNAIfNull(eventMap, CONTENT_GOORU_OID);
		String lessonGooruId = setNAIfNull(eventMap, LESSON_GOORU_OID);
		String unitGooruId = setNAIfNull(eventMap, UNIT_GOORU_OID);
		String courseGooruId = setNAIfNull(eventMap, COURSE_GOORU_OID);
		String classGooruId = setNAIfNull(eventMap, CLASS_GOORU_OID);
		String parentGooruId = setNAIfNull(eventMap, PARENT_GOORU_OID);
		String collectionItemId = setNAIfNull(eventMap, COLLECTION_ITEM_ID);
		String sessionId = setNAIfNull(eventMap, SESSION_ID);
		String eventType = setNAIfNull(eventMap, TYPE);
		String collectionType = eventMap.get(COLLECTION_TYPE).equals(COLLECTION) ? COLLECTION : ASSESSMENT;
		String questionType = setNAIfNull(eventMap, QUESTION_TYPE);
		String resourceType = setNAIfNull(eventMap, RESOURCE_TYPE);
		String answerObject = setNAIfNull(eventMap, ANSWER_OBECT);
		String answerStatus = "NA";
		String gradeType = eventMap.containsValue(GRADE_TYPE) ? (String)eventMap.get(GRADE_TYPE) : SYSTEM;
		
		long eventTime = ((Number) eventMap.get(END_TIME)).longValue();
		long score = 0;
		long timespent = setLongZeroIfNull(eventMap, TOTALTIMEINMS);
		long views = setLongZeroIfNull(eventMap, VIEWS_COUNT);
		int attempts = setIntegerZeroIfNull(eventMap, ATTEMPT_COUNT);
		long reaction = 0;

		if (QUESTION.equals(resourceType) && (STOP.equals(eventType))) {
			int attemptSeq = 0;
			int[] attempStatus = TypeConverter.stringToIntArray((String) eventMap.get(ATTEMPT_STATUS));

			if (attempts != 0) {
				attemptSeq = attempts - 1;
			}
			if (attempStatus.length == 0) {
				answerStatus = LoaderConstants.SKIPPED.getName();
			} else if (attempStatus[attemptSeq] == 0) {
				answerStatus = LoaderConstants.INCORRECT.getName();
				score = 0;
			} else if (attempStatus[attemptSeq] == 1) {
				answerStatus = LoaderConstants.CORRECT.getName();
				score = 100;
			}
			if (OE.equals(questionType)) {
				try {
					JSONObject answerObj = new JSONObject(answerObject);
					if (StringUtils.isNotBlank(answerObj.getString(TEXT))) {
						answerStatus = LoaderConstants.ATTEMPTED.getName();
					}
				} catch (Exception e) {
					LOG.error("Exception", e);
				}
			}
			LOG.info("answerStatus : " + answerStatus);

		}

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
		studentsClassActivity.setAttemptStatus(INPROGRESS);
		if ((LoaderConstants.CPV1.getName().equalsIgnoreCase(eventName)) && STOP.equals(eventType)) {
			studentsClassActivity.setAttemptStatus(ATTEMPTED);
			if(gradeType.equalsIgnoreCase(SYSTEM)){
				studentsClassActivity.setAttemptStatus(COMPLETED);
				baseCassandraDao.getSessionScore(userSessionActivity,eventName);
				score = userSessionActivity.getScore();
			}
		}
		userSessionActivity.setScore(score);

		studentsClassActivity.setClassUid(classGooruId);
		studentsClassActivity.setCourseUid(courseGooruId);
		studentsClassActivity.setUnitUid(unitGooruId);
		studentsClassActivity.setLessonUid(lessonGooruId);
		studentsClassActivity.setCollectionType(collectionType);
		if (eventName.equalsIgnoreCase(LoaderConstants.CRPV1.getName())) {
			studentsClassActivity.setCollectionUid(parentGooruId);
			studentsClassActivity.setViews(0);
		} else if(eventName.equalsIgnoreCase(LoaderConstants.CPV1.getName())){
			studentsClassActivity.setCollectionUid(contentGooruId);
			studentsClassActivity.setViews(views);
		}
		studentsClassActivity.setUserUid(gooruUUID);
		studentsClassActivity.setScore(score);
		studentsClassActivity.setTimeSpent(timespent);

		/**
		 * Assessment/Collection wise
		 */
		classActivityDataCube.setRowKey(appendTildaSeperator(studentsClassActivity.getClassUid(), studentsClassActivity.getCourseUid(), studentsClassActivity.getUnitUid(),
				studentsClassActivity.getLessonUid()));
		classActivityDataCube.setLeafNode(studentsClassActivity.getCollectionUid());
		classActivityDataCube.setUserUid(studentsClassActivity.getUserUid());
		classActivityDataCube.setCollectionType(studentsClassActivity.getCollectionType());
		classActivityDataCube.setViews(studentsClassActivity.getViews());
		classActivityDataCube.setTimeSpent(studentsClassActivity.getTimeSpent());
		classActivityDataCube.setScore(studentsClassActivity.getScore());

		studentLocation.setUserUid(gooruUUID);
		studentLocation.setClassUid(classGooruId);
		studentLocation.setCourseUid(courseGooruId);
		studentLocation.setUnitUid(unitGooruId);
		studentLocation.setLessonUid(lessonGooruId);
		if (eventName.equalsIgnoreCase(LoaderConstants.CRPV1.getName())) {
			studentLocation.setCollectionUid(parentGooruId);
		} else if(eventName.equalsIgnoreCase(LoaderConstants.CPV1.getName())){
			studentLocation.setCollectionUid(contentGooruId);
		}
		studentLocation.setCollectionType(collectionType);
		studentLocation.setResourceUid(contentGooruId);
		studentLocation.setSessionTime(eventTime);
	}
	private String setNAIfNull(Map<String, Object> eventMap,String fieldName) {
		if(eventMap.containsKey(fieldName) && eventMap.get(fieldName) != null && StringUtils.isNotBlank((String)eventMap.get(fieldName))){
			return (String) eventMap.get(fieldName);
		}
		return NA;
	}
	private String setNullIfEmpty(Map<String, Object> eventMap,String fieldName) {
		if(eventMap.containsKey(fieldName) && eventMap.get(fieldName) != null && StringUtils.isNotBlank((String)eventMap.get(fieldName))){
			return (String) eventMap.get(fieldName);
		}
		return null;
	}
	private long setLongZeroIfNull(Map<String, Object> eventMap,String fieldName) {
		if(eventMap.containsKey(fieldName) && eventMap.get(fieldName) != null){
			return ((Number) eventMap.get(fieldName)).longValue();
		}
		return 0L;
	}
	private int setIntegerZeroIfNull(Map<String, Object> eventMap,String fieldName) {
		if(eventMap.containsKey(fieldName) && eventMap.get(fieldName) != null){
			return ((Number) eventMap.get(fieldName)).intValue();
		}
		return 0;
	}
}
