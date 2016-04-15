package org.logger.event.cassandra.loader.dao;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang.StringUtils;
import org.ednovo.data.model.ClassActivityDatacube;
import org.ednovo.data.model.ContentTaxonomyActivity;
import org.ednovo.data.model.EventBuilder;
import org.ednovo.data.model.ObjectBuilder;
import org.ednovo.data.model.StudentLocation;
import org.ednovo.data.model.StudentsClassActivity;
import org.ednovo.data.model.UserSessionActivity;
import org.ednovo.data.model.UserSessionTaxonomyActivity;
import org.json.JSONException;
import org.logger.event.cassandra.loader.Constants;
import org.logger.event.cassandra.loader.LoaderConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

public class ReportsGeneratorDAOImpl extends BaseDAOCassandraImpl implements ReportsGeneratorDAO {

	private static final Logger LOG = LoggerFactory.getLogger(ReportsGeneratorDAOImpl.class);

	private BaseCassandraRepo baseCassandraDao;

	private final ExecutorService service = Executors.newFixedThreadPool(10);

	public ReportsGeneratorDAOImpl() {
		baseCassandraDao = BaseCassandraRepo.instance();
	}

	/**
	 * This is method to handle and processing all the player events and to generate reports.
	 * 
	 * @param event
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
			UserSessionTaxonomyActivity userSessionTaxonomyActivity = objectBuilderHandler.getUserSessionTaxonomyActivity();

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
			
			saveUserSessionTaxonomyActivity(userSessionTaxonomyActivity);

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

			if (classActivityDatacube != null && studentsClassActivity != null && event.getReportsContext().contains(Constants.PERFORMANCE)) {
				callClassActitivityDataCubeGenerator(studentsClassActivity, classActivityDatacube);
				LOG.info("datacube generator completed : {} ", userSessionActivity.getSessionId());
			}

			if (contentTaxonomyActivity != null && event.getReportsContext().contains(Constants.PROFILE)) {
				service.submit(new MastryGenerator(contentTaxonomyActivity, baseCassandraDao));
				LOG.info("calling mastery generator : {} ", userSessionActivity.getSessionId());
			}
			saveQuestionGrade(event);

		} catch (Exception e) {
			LOG.error("Exception:", e);
		}

	}

	private void saveUserSessionTaxonomyActivity(UserSessionTaxonomyActivity userSessionTaxonomyActivity) {
		if (userSessionTaxonomyActivity != null && (userSessionTaxonomyActivity.getTaxonomyIds().length() > 0)) {
			for (int index = 0; index < (userSessionTaxonomyActivity.getTaxonomyIds()).length(); index++) {
				ResultSet taxRows = null;
				try {
					taxRows = baseCassandraDao.getTaxonomy(userSessionTaxonomyActivity.getTaxonomyIds().getString(index));
				} catch (JSONException e) {
					LOG.error("Invalid taxonomy ids JSON format ");
				}
				if (taxRows != null) {
					for (Row taxColumns : taxRows) {
						userSessionTaxonomyActivity.setSubjectId(taxColumns.getString(Constants.SUBJECT_ID));
						userSessionTaxonomyActivity.setCourseId(taxColumns.getString(Constants.COURSE_ID));
						userSessionTaxonomyActivity.setDomainId(taxColumns.getString(Constants.DOMAIN_ID));
						userSessionTaxonomyActivity.setStandardsId(taxColumns.getString(Constants.STANDARDS_ID));
						userSessionTaxonomyActivity.setLearningTargetsId(taxColumns.getString(Constants.LEARNING_TARGETS_ID));
						baseCassandraDao.mergeUserSessionTaxonomyActivity(userSessionTaxonomyActivity);
						baseCassandraDao.insertUserSessionTaxonomyActivity(userSessionTaxonomyActivity);
					}
				}
			}
		}
	}

	/**
	 * Generate datacube for performance reports
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
	 * Save data while teacher grading user responses.
	 * 
	 * @param event
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
	 * Save session id details in each sessions
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

	/**
	 * Save most recent session id for each collection/assessment play happen
	 * 
	 * @param eventName
	 * @param userSessionActivity
	 * @param studentsClassActivity
	 * @param userAllSessionActivity
	 */
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
	 * Rolling up to reosurces usage data to collection level from collection.resource.play event
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

}
