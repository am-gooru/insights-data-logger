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

import org.ednovo.data.model.ClassActivityDatacube;
import org.ednovo.data.model.ContentTaxonomyActivity;
import org.ednovo.data.model.ObjectBuilder;
import org.ednovo.data.model.StudentLocation;
import org.ednovo.data.model.StudentsClassActivity;
import org.ednovo.data.model.UserSessionActivity;
import org.logger.event.cassandra.loader.Constants;
import org.logger.event.cassandra.loader.LoaderConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;

public class MicroAggregatorDAOImpl extends BaseDAOCassandraImpl implements MicroAggregatorDAO {

	private static final Logger LOG = LoggerFactory.getLogger(MicroAggregatorDAOImpl.class);

	private BaseCassandraRepo baseCassandraDao;

	private final ExecutorService service = Executors.newFixedThreadPool(10);

	public MicroAggregatorDAOImpl() {
		baseCassandraDao = BaseCassandraRepo.instance();
	}

	/**
	 * This is method to handle and processing all the player events and to generate data
	 * @param eventMap
	 */
	@Override
	public void eventProcessor(Map<String, Object> eventMap) {

		try {
			String eventName = setNullIfEmpty(eventMap, Constants.EVENT_NAME);
			ObjectBuilder objectBuilderHandler = new ObjectBuilder(eventMap);
			UserSessionActivity userSessionActivity = objectBuilderHandler.getUserSessionActivity();
			UserSessionActivity userAllSessionActivity = objectBuilderHandler.getUserAllSessionActivity();
			StudentsClassActivity studentsClassActivity = objectBuilderHandler.getStudentsClassActivity();
			ClassActivityDatacube classActivityDatacube = objectBuilderHandler.getClassActivityDatacube();
			StudentLocation studentLocation = objectBuilderHandler.getStudentLocation();
			ContentTaxonomyActivity contentTaxonomyActivity = objectBuilderHandler.getContentTaxonomyActivity();

			if (userSessionActivity != null) {
				baseCassandraDao.compareAndMergeUserSessionActivity(userSessionActivity);
				baseCassandraDao.saveUserSessionActivity(userSessionActivity);
				LOG.info("store session activity completed : {} ", userSessionActivity.getSessionId());
			}
			
			if (userAllSessionActivity != null) {
				baseCassandraDao.compareAndMergeUserSessionActivity(userAllSessionActivity);
				baseCassandraDao.saveUserSessionActivity(userAllSessionActivity);
				LOG.info("store all session activity completed : {} ", userSessionActivity.getSessionId());
			}

			saveUserSessions(eventName, userSessionActivity, studentsClassActivity, studentLocation);

			saveCollectionDataFromResourcePlay(eventName, userSessionActivity, userAllSessionActivity, studentsClassActivity);
			
			if (studentLocation != null) {
				baseCassandraDao.saveStudentLocation(studentLocation);
				LOG.info("store students completed : {} ", userSessionActivity.getSessionId());
			}

			if (classActivityDatacube != null && studentsClassActivity != null) {
				callClassActitivityDataCubeGenerator(studentsClassActivity, classActivityDatacube);
				LOG.info("datacube generator completed : {} ", userSessionActivity.getSessionId());
			}

			if (contentTaxonomyActivity != null) {
				service.submit(new MastryGenerator(contentTaxonomyActivity, baseCassandraDao));
				LOG.info("calling smastery generator : {} ", userSessionActivity.getSessionId());
			}
			saveQuestionGrade(eventName, eventMap);

		} catch (Exception e) {
			LOG.error("Exception:", e);
		}

	}

	/**
	 * Generate Datacube from player events.
	 * @param studentsClassActivity
	 * @param classActivityDatacube
	 */
	private void callClassActitivityDataCubeGenerator(StudentsClassActivity studentsClassActivity, ClassActivityDatacube classActivityDatacube) {
		baseCassandraDao.compareAndMergeStudentsClassActivity(studentsClassActivity);
		baseCassandraDao.saveStudentsClassActivity(studentsClassActivity);
		classActivityDatacube.setViews(studentsClassActivity.getViews());
		classActivityDatacube.setTimeSpent(studentsClassActivity.getTimeSpent());
		classActivityDatacube.setScore(studentsClassActivity.getScore());
		classActivityDatacube.setReaction(studentsClassActivity.getReaction());
		if (studentsClassActivity.getAttemptStatus().equalsIgnoreCase(Constants.COMPLETED)) {
			classActivityDatacube.setCompletedCount(1L);
		} else {
			classActivityDatacube.setCompletedCount(0L);
		}
		baseCassandraDao.saveClassActivityDataCube(classActivityDatacube);
		service.submit(new ClassActivityDataCubeGenerator(studentsClassActivity, baseCassandraDao));
	}
	
	/**
	 * Save question grade.
	 * @param eventName
	 * @param eventMap
	 */
	private void saveQuestionGrade(String eventName, Map<String, Object> eventMap) {
		if (eventName.equalsIgnoreCase(LoaderConstants.QUESTION_GRADE.getName())) {
			if (eventMap.get("gradeStatus").equals("save")) {
				baseCassandraDao.saveQuestionGrade((String) eventMap.get("teacherId"), (String) eventMap.get(Constants.GOORUID), (String) eventMap.get(Constants.SESSION_ID),
						(String) eventMap.get(Constants.CONTENT_GOORU_OID), ((Number) eventMap.get(Constants.SCORE)).longValue());
			} else if (eventMap.get("gradeStatus").equals("submit")) {
				Rows<String, String> questionScores = baseCassandraDao.getQuestionsGradeBySessionId((String) eventMap.get("teacherId"), (String) eventMap.get(Constants.GOORUID),
						(String) eventMap.get(Constants.SESSION_ID));
				if (questionScores != null && questionScores.size() > 0) {
					for (Row<String, String> questionScore : questionScores) {
						ColumnList<String> score = questionScore.getColumns();
						baseCassandraDao.saveQuestionGradeInSession((String) eventMap.get(Constants.SESSION_ID), (String) eventMap.get(Constants.CONTENT_GOORU_OID), Constants.NA,
								Constants.COMPLETED,score.getLongValue(Constants.SCORE, 0L));
					}
				}
				UserSessionActivity userCollectionData = baseCassandraDao.getUserSessionActivity((String) eventMap.get(Constants.SESSION_ID), (String) eventMap.get(Constants.PARENT_GOORU_OID),
						Constants.NA);
				baseCassandraDao.getSessionScore(userCollectionData, LoaderConstants.CPV1.getName());
				baseCassandraDao
						.saveQuestionGradeInSession((String) eventMap.get(Constants.SESSION_ID), (String) eventMap.get(Constants.PARENT_GOORU_OID), Constants.NA, Constants.COMPLETED,userCollectionData.getScore());
			}
			LOG.info("store question grade completed : {} ", eventMap.get(Constants.SESSION_ID));
		}
	}

	/**
	 * Save sessions in user_session CF
	 * @param eventName
	 * @param userSessionActivity
	 * @param studentsClassActivity
	 * @param studentLocation
	 */
	private void saveUserSessions(String eventName, UserSessionActivity userSessionActivity, StudentsClassActivity studentsClassActivity, StudentLocation studentLocation) {
		if (LoaderConstants.CPV1.getName().equalsIgnoreCase(eventName)) {
			LOG.info("saving sessions...");
			baseCassandraDao.saveUserSession(userSessionActivity.getSessionId(), studentsClassActivity.getClassUid(), studentsClassActivity.getCourseUid(), studentsClassActivity.getUnitUid(),
					studentsClassActivity.getLessonUid(), studentsClassActivity.getCollectionUid(), studentsClassActivity.getUserUid(), userSessionActivity.getCollectionType(),
					userSessionActivity.getEventType(), studentLocation.getSessionTime());
		}
	}

	/**
	 * Aggregate resource.play event to collection level.
	 * @param eventName
	 * @param userSessionActivity
	 * @param userAllSessionActivity
	 * @param studentsClassActivity
	 */
	private void saveCollectionDataFromResourcePlay(String eventName, UserSessionActivity userSessionActivity, UserSessionActivity userAllSessionActivity, StudentsClassActivity studentsClassActivity) {
		if (Constants.COLLECTION.equalsIgnoreCase(userSessionActivity.getCollectionType()) && LoaderConstants.CRPV1.getName().equalsIgnoreCase(eventName)
				&& userSessionActivity.getEventType().equalsIgnoreCase(Constants.STOP)) {
			UserSessionActivity userCollectionData = baseCassandraDao.getUserSessionActivity(userSessionActivity.getSessionId(), userSessionActivity.getParentGooruOid(), Constants.NA);
			UserSessionActivity userAllSessionCollectionActivity = baseCassandraDao.getUserSessionActivity(userAllSessionActivity.getSessionId(), userAllSessionActivity.getParentGooruOid(),
					Constants.NA);
			baseCassandraDao.getSessionScore(userSessionActivity, eventName);
			if (userCollectionData != null) {
				userCollectionData.setTimeSpent(userCollectionData.getTimeSpent() + userSessionActivity.getTimeSpent());
				userCollectionData.setScore(userSessionActivity.getScore());
				userCollectionData.setReaction(userSessionActivity.getReaction());
			}
			userAllSessionCollectionActivity.setTimeSpent(userAllSessionCollectionActivity.getTimeSpent() + userSessionActivity.getTimeSpent());
			userAllSessionCollectionActivity.setScore(userSessionActivity.getScore());
			userAllSessionCollectionActivity.setReaction(userSessionActivity.getScore());
			baseCassandraDao.saveUserSessionActivity(userCollectionData);
			baseCassandraDao.saveUserSessionActivity(userAllSessionCollectionActivity);
			studentsClassActivity.setTimeSpent(userCollectionData.getTimeSpent());
			studentsClassActivity.setScore(userCollectionData.getScore());
			studentsClassActivity.setReaction(userCollectionData.getReaction());
			LOG.info("aggregate to collection level from resource play : {}", userAllSessionActivity.getSessionId());
		}
	}

}
