package org.logger.event.cassandra.loader.dao;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.lang.StringUtils;
import org.ednovo.data.model.ClassActivityDatacube;
import org.ednovo.data.model.ContentTaxonomyActivity;
import org.ednovo.data.model.EventBuilder;
import org.ednovo.data.model.ObjectBuilder;
import org.ednovo.data.model.StudentLocation;
import org.ednovo.data.model.StudentsClassActivity;
import org.ednovo.data.model.UserSessionActivity;
import org.logger.event.cassandra.loader.Constants;
import org.logger.event.cassandra.loader.LoaderConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ResultSet;

public class MicroAggregatorDAOImpl extends BaseDAOCassandraImpl implements MicroAggregatorDAO {

	private static final Logger LOG = LoggerFactory.getLogger(MicroAggregatorDAOImpl.class);

	private BaseCassandraRepo baseCassandraDao;

	private final ExecutorService service = Executors.newFixedThreadPool(10);

	public MicroAggregatorDAOImpl() {
		baseCassandraDao = BaseCassandraRepo.instance();
	}

	/**
	 * This is method to handle and processing all the player events and to generate data
	 * 
	 * @param eventMap
	 */
	@Override
	public void eventProcessor(EventBuilder event) {

		try {
			String eventName = event.getEventName();
			ObjectBuilder objectBuilderHandler = new ObjectBuilder(event);
			UserSessionActivity userSessionActivity = objectBuilderHandler.getUserSessionActivity();
			UserSessionActivity userAllSessionActivity = objectBuilderHandler.getUserAllSessionActivity();
			StudentsClassActivity studentsClassActivity = objectBuilderHandler.getStudentsClassActivity();
			ClassActivityDatacube classActivityDatacube = objectBuilderHandler.getClassActivityDatacube();
			StudentLocation studentLocation = objectBuilderHandler.getStudentLocation();
			ContentTaxonomyActivity contentTaxonomyActivity = objectBuilderHandler.getContentTaxonomyActivity();

			if (userSessionActivity != null && !LoaderConstants.CRAV1.getName().equalsIgnoreCase(eventName)) {
				baseCassandraDao.compareAndMergeUserSessionActivity(userSessionActivity);
				baseCassandraDao.saveUserSessionActivity(userSessionActivity);
				LOG.info("store session activity completed : {} ", userSessionActivity.getSessionId());
			}

			if (userAllSessionActivity != null && !LoaderConstants.CRAV1.getName().equalsIgnoreCase(eventName)) {
				baseCassandraDao.compareAndMergeUserSessionActivity(userAllSessionActivity);
				baseCassandraDao.saveUserSessionActivity(userAllSessionActivity);
				LOG.info("store all session activity completed : {} ", userSessionActivity.getSessionId());
			}
			if (LoaderConstants.CRAV1.getName().equalsIgnoreCase(eventName)) {
				baseCassandraDao.updateReaction(userSessionActivity);
			}

			saveUserSessions(eventName, userSessionActivity, studentsClassActivity, studentLocation);

			saveLastSessions(eventName, userSessionActivity, studentsClassActivity, userAllSessionActivity);

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
				LOG.info("calling mastery generator : {} ", userSessionActivity.getSessionId());
			}
			saveQuestionGrade(event);

		} catch (Exception e) {
			LOG.error("Exception:", e);
		}

	}

	/**
	 * Generate Datacube from player events.
	 * 
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
	 * 
	 * @param eventName
	 * @param eventMap
	 */
	private void saveQuestionGrade(EventBuilder event) {
		if (event.getEventName().equalsIgnoreCase(LoaderConstants.QUESTION_GRADE.getName())) {
			if (event.getGradeType().equals("save")) {
				baseCassandraDao.saveQuestionGrade(event.getTeacherId(), event.getGooruUUID(), event.getSessionId(), event.getContentGooruId(), event.getScore());
			} else if (event.getGradeStatus().equals("submit")) {
				ResultSet questionScores = baseCassandraDao.getQuestionsGradeBySessionId(event.getTeacherId(), event.getGooruUUID(), event.getSessionId());
				if (questionScores != null) {
					for (com.datastax.driver.core.Row score : questionScores) {
						baseCassandraDao.saveQuestionGradeInSession(event.getSessionId(), event.getContentGooruId(), Constants.NA, Constants.COMPLETED, score.getLong(Constants.SCORE));
					}
				}
				UserSessionActivity userCollectionData = baseCassandraDao.getUserSessionActivity((String) event.getSessionId(), event.getParentGooruId(), Constants.NA);
				baseCassandraDao.getSessionScore(userCollectionData, LoaderConstants.CPV1.getName());
				baseCassandraDao.saveQuestionGradeInSession((String) event.getSessionId(), event.getParentGooruId(), Constants.NA, Constants.COMPLETED, userCollectionData.getScore());
			}
			LOG.info("store question grade completed : {} ", event.getSessionId());
		}
	}

	/**
	 * Save sessions in user_session CF
	 * 
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

	private void saveLastSessions(String eventName, UserSessionActivity userSessionActivity, StudentsClassActivity studentsClassActivity, UserSessionActivity userAllSessionActivity) {
		if (LoaderConstants.CPV1.getName().equalsIgnoreCase(eventName) && StringUtils.isNotBlank(studentsClassActivity.getClassUid()) && !studentsClassActivity.getClassUid().equals(Constants.NA)) {
			LOG.info("saving latest sessions...");
			if (Constants.START.equals(userSessionActivity.getEventType()) && Constants.COLLECTION.equalsIgnoreCase(userSessionActivity.getCollectionType())) {
				baseCassandraDao.saveLastSession(studentsClassActivity.getClassUid(), studentsClassActivity.getCourseUid(), studentsClassActivity.getUnitUid(), studentsClassActivity.getLessonUid(),
						studentsClassActivity.getCollectionUid(), studentsClassActivity.getUserUid(), userAllSessionActivity.getSessionId());
			} else if (Constants.STOP.equals(userSessionActivity.getEventType()) && Constants.ASSESSMENT.equalsIgnoreCase(userSessionActivity.getCollectionType())) {
				baseCassandraDao.saveLastSession(studentsClassActivity.getClassUid(), studentsClassActivity.getCourseUid(), studentsClassActivity.getUnitUid(), studentsClassActivity.getLessonUid(),
						studentsClassActivity.getCollectionUid(), studentsClassActivity.getUserUid(), userSessionActivity.getSessionId());
			}
		}
	}

	/**
	 * Aggregate resource.play event to collection level.
	 * 
	 * @param eventName
	 * @param userSessionActivity
	 * @param userAllSessionActivity
	 * @param studentsClassActivity
	 */
	private void saveCollectionDataFromResourcePlay(String eventName, UserSessionActivity userSessionActivity, UserSessionActivity userAllSessionActivity,
			StudentsClassActivity studentsClassActivity) {
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

	private void countStatisticalData(final EventBuilder event) {
		if (event.getViews() > 0) {
			baseCassandraDao.updateStatisticalCounterData(event.getContentGooruId(), Constants.VIEWS, event.getViews());
		}
		if (event.getTimespent() > 0) {
			baseCassandraDao.updateStatisticalCounterData(event.getContentGooruId(), Constants.TOTALTIMEINMS, event.getTimespent());
		}
	}

	public void reComputeData(final EventBuilder event) {
		ObjectBuilder objectBuilderHandler = new ObjectBuilder(event);
		StudentsClassActivity studentsClassActivity = objectBuilderHandler.getStudentsClassActivity();
		ResultSet classMembers = baseCassandraDao.getClassMembers(event.getClassGooruId());
		generateDeleteTasks(studentsClassActivity, classMembers.one().getSet("members", String.class));

	}

	private void generateDeleteTasks(final StudentsClassActivity studentsClassActivity, final Set<String> studentsIds) {
		try {
			Set<Callable<String>> deleteTasks = new HashSet<Callable<String>>();
			deleteTasks.add(new Callable<String>() {
				public String call() throws Exception {
					switch (studentsClassActivity.getCollectionType()) {
					case Constants.COURSE:
						for (final String studentId : studentsIds) {
							baseCassandraDao.deleteCourseUsage(studentsClassActivity, studentId, Constants.COLLECTION);
							baseCassandraDao.deleteCourseUsage(studentsClassActivity, studentId, Constants.ASSESSMENT);
						}
						for (String classId : studentsClassActivity.getClassUid().split(Constants.COMMA)) {
							baseCassandraDao.deleteClassActivityDataCube(appendTildaSeperator(classId, studentsClassActivity.getCourseUid()));
						}
						break;
					case Constants.UNIT:

						for (String classId : studentsClassActivity.getClassUid().split(Constants.COMMA)) {
							baseCassandraDao.deleteClassActivityDataCube(appendTildaSeperator(classId, studentsClassActivity.getCourseUid(), studentsClassActivity.getUnitUid()));
						}
						for (final String studentId : studentsIds) {
							baseCassandraDao.deleteUnitUsage(studentsClassActivity, studentId, Constants.COLLECTION);
							baseCassandraDao.deleteUnitUsage(studentsClassActivity, studentId, Constants.ASSESSMENT);
							ClassActivityDatacube aggregatedCourseActivityC = baseCassandraDao
									.getStudentsClassActivityDatacube(appendTildaSeperator(studentsClassActivity.getClassUid(), studentsClassActivity.getCourseUid()), studentId, Constants.COLLECTION);
							aggregatedCourseActivityC.setRowKey(studentsClassActivity.getClassUid());
							aggregatedCourseActivityC.setLeafNode(studentsClassActivity.getCourseUid());
							baseCassandraDao.saveClassActivityDataCube(aggregatedCourseActivityC);

							ClassActivityDatacube aggregatedCourseActivityA = baseCassandraDao
									.getStudentsClassActivityDatacube(appendTildaSeperator(studentsClassActivity.getClassUid(), studentsClassActivity.getCourseUid()), studentId, Constants.ASSESSMENT);
							aggregatedCourseActivityA.setRowKey(studentsClassActivity.getClassUid());
							aggregatedCourseActivityA.setLeafNode(studentsClassActivity.getCourseUid());
							baseCassandraDao.saveClassActivityDataCube(aggregatedCourseActivityA);

						}

						break;
					case Constants.LESSON:

						for (String classId : studentsClassActivity.getClassUid().split(Constants.COMMA)) {
							baseCassandraDao.deleteClassActivityDataCube(
									appendTildaSeperator(classId, studentsClassActivity.getCourseUid(), studentsClassActivity.getUnitUid(), studentsClassActivity.getLessonUid()));
						}
						for (final String studentId : studentsIds) {
							baseCassandraDao.deleteLessonUsage(studentsClassActivity, studentId, Constants.COLLECTION);
							baseCassandraDao.deleteLessonUsage(studentsClassActivity, studentId, Constants.ASSESSMENT);

							ClassActivityDatacube aggregatedUnitActivityC = baseCassandraDao.getStudentsClassActivityDatacube(
									appendTildaSeperator(studentsClassActivity.getClassUid(), studentsClassActivity.getCourseUid(), studentsClassActivity.getUnitUid()), studentId,
									Constants.COLLECTION);
							aggregatedUnitActivityC.setRowKey(appendTildaSeperator(studentsClassActivity.getClassUid(), studentsClassActivity.getCourseUid()));
							aggregatedUnitActivityC.setLeafNode(studentsClassActivity.getUnitUid());
							baseCassandraDao.saveClassActivityDataCube(aggregatedUnitActivityC);

							ClassActivityDatacube aggregatedUnitActivityA = baseCassandraDao.getStudentsClassActivityDatacube(
									appendTildaSeperator(studentsClassActivity.getClassUid(), studentsClassActivity.getCourseUid(), studentsClassActivity.getUnitUid()), studentId,
									Constants.ASSESSMENT);
							aggregatedUnitActivityA.setRowKey(appendTildaSeperator(studentsClassActivity.getClassUid(), studentsClassActivity.getCourseUid()));
							aggregatedUnitActivityA.setLeafNode(studentsClassActivity.getUnitUid());
							baseCassandraDao.saveClassActivityDataCube(aggregatedUnitActivityA);

						}
						break;
					case Constants.COLLECTION:
						for (String classId : studentsClassActivity.getClassUid().split(Constants.COMMA)) {
							baseCassandraDao.deleteClassActivityDataCube(appendTildaSeperator(classId, studentsClassActivity.getCourseUid(), studentsClassActivity.getUnitUid(),
									studentsClassActivity.getLessonUid(), studentsClassActivity.getCollectionUid()));

						}
						for (final String studentId : studentsIds) {
							baseCassandraDao.deleteLessonUsage(studentsClassActivity, studentId, Constants.COLLECTION);
							ClassActivityDatacube aggregatedLessonActivity = baseCassandraDao.getStudentsClassActivityDatacube(appendTildaSeperator(studentsClassActivity.getClassUid(),
									studentsClassActivity.getCourseUid(), studentsClassActivity.getUnitUid(), studentsClassActivity.getLessonUid()), studentId, Constants.COLLECTION);
							aggregatedLessonActivity.setRowKey(appendTildaSeperator(studentsClassActivity.getClassUid(), studentsClassActivity.getCourseUid(), studentsClassActivity.getUnitUid()));
							aggregatedLessonActivity.setLeafNode(studentsClassActivity.getLessonUid());
							baseCassandraDao.saveClassActivityDataCube(aggregatedLessonActivity);
						}

						break;
					case Constants.ASSESSMENT:
						for (String classId : studentsClassActivity.getClassUid().split(Constants.COMMA)) {
							baseCassandraDao.deleteClassActivityDataCube(appendTildaSeperator(classId, studentsClassActivity.getCourseUid(), studentsClassActivity.getUnitUid(),
									studentsClassActivity.getLessonUid(), studentsClassActivity.getCollectionUid()));
						}
						for (final String studentId : studentsIds) {
							baseCassandraDao.deleteLessonUsage(studentsClassActivity, studentId, Constants.ASSESSMENT);
							ClassActivityDatacube aggregatedLessonActivity = baseCassandraDao.getStudentsClassActivityDatacube(appendTildaSeperator(studentsClassActivity.getClassUid(),
									studentsClassActivity.getCourseUid(), studentsClassActivity.getUnitUid(), studentsClassActivity.getLessonUid()), studentId, Constants.ASSESSMENT);
							aggregatedLessonActivity.setRowKey(appendTildaSeperator(studentsClassActivity.getClassUid(), studentsClassActivity.getCourseUid(), studentsClassActivity.getUnitUid()));
							aggregatedLessonActivity.setLeafNode(studentsClassActivity.getLessonUid());
							baseCassandraDao.saveClassActivityDataCube(aggregatedLessonActivity);
						}
						break;
					}
					return Constants.COMPLETED;
				}
			});

			List<Future<String>> taskStatues = service.invokeAll(deleteTasks);
			for (Future<String> taskStatus : taskStatues) {
				LOG.info(taskStatus.get());
			}
		} catch (Exception e) {
			LOG.error("Exception:", e);
		}
	}

	@Override
	public void statAndRawUpdate(final EventBuilder event) {
		try {
			Set<Callable<String>> tasks = new HashSet<Callable<String>>();
			tasks.add(new Callable<String>() {
				public String call() throws Exception {
					switch (event.getEventName()) {

					case Constants.COLLECTION_PLAY:
						countStatisticalData(event);
						break;
					case Constants.COLLECTION_RESOURCE_PLAY:
						countStatisticalData(event);
						break;
					case Constants.RESOURCE_PLAY:
						countStatisticalData(event);
						break;
					case Constants.CLASS_JOIN:
						HashSet<String> members = new HashSet<String>();
						members.add(event.getGooruUUID());
						baseCassandraDao.saveClassMembers(event.getClassGooruId(), members);
						break;
					case Constants.ITEM_CREATE:
						if (event.getContentFormat().equalsIgnoreCase(Constants.CLASS)) {
							baseCassandraDao.updateContentCreators(event.getContentGooruId(), event.getGooruUUID());
						}
						break;
					case Constants.COLLABORATORS_UPDATE:
						HashSet<String> collaborators = new HashSet<String>();
						for (int index = 0; index < (event.getCollaborators()).length(); index++) {
							collaborators.add(event.getCollaborators().getString(index));
						}
						baseCassandraDao.updateCollaborators(event.getContentGooruId(), collaborators);
						break;
					}
					return Constants.COMPLETED;
				}
			});

			List<Future<String>> status = service.invokeAll(tasks);
			for (Future<String> taskStatus : status) {
				LOG.info(taskStatus.get());
			}
		} catch (Exception e) {
			LOG.error("Exception:", e);
		}
	}
}
