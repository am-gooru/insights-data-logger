package org.logger.event.cassandra.loader.dao;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
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
				baseCassandraDao.updateReaction(userAllSessionActivity);
			}

			saveUserSessions(event, userSessionActivity);

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
				this.saveContentClassTaxonomyActivity(contentTaxonomyActivity);
				LOG.info("calling mastery generator : {} ", userSessionActivity.getSessionId());
			}
			saveQuestionGrade(event);

		} catch (Exception e) {
			LOG.error("Exception:", e);
		}

	}

	private void saveUserSessionTaxonomyActivity(UserSessionTaxonomyActivity userSessionTaxonomyActivity) {
		if (userSessionTaxonomyActivity != null) {
			Iterator<String> internalTaxonomyCodes = userSessionTaxonomyActivity.getTaxonomyIds().keys();
			while (internalTaxonomyCodes.hasNext()) {
				try {
					String internalTaxonomyCode = internalTaxonomyCodes.next();
					String displayCode = userSessionTaxonomyActivity.getTaxonomyIds().getString(internalTaxonomyCode);
					userSessionTaxonomyActivity.setDisplayCode(displayCode);
					Map<String, String> taxObject = new HashMap<String, String>();
					splitByTaxonomyCode(internalTaxonomyCode, taxObject);
					String subject = taxObject.get(Constants.SUBJECT), course = taxObject.get(Constants.COURSE), domain = taxObject.get(Constants.DOMAIN), standards = taxObject.get(Constants.STANDARDS), learningTarget = taxObject.get(Constants.LEARNING_TARGETS);
					userSessionTaxonomyActivity.setSubjectId(StringUtils.isNotBlank(subject) ? subject : Constants.NA);
					userSessionTaxonomyActivity.setCourseId(StringUtils.isNotBlank(course) ? appendHyphen(subject, course) : Constants.NA);
					userSessionTaxonomyActivity.setDomainId(StringUtils.isNotBlank(domain) ? appendHyphen(subject, course, domain) : Constants.NA);
					userSessionTaxonomyActivity.setStandardsId(StringUtils.isNotBlank(standards) ? appendHyphen(subject, course, domain, standards) : Constants.NA);
					userSessionTaxonomyActivity.setLearningTargetsId(StringUtils.isNotBlank(learningTarget) ? appendHyphen(subject, course, domain, standards, learningTarget) : Constants.NA);
					baseCassandraDao.mergeUserSessionTaxonomyActivity(userSessionTaxonomyActivity);
					baseCassandraDao.insertUserSessionTaxonomyActivity(userSessionTaxonomyActivity);
				} catch (Exception e) {
					LOG.error("Exception while split and save taxonmy usage: ", e);
				}

			}
		}

	}

	private void saveContentClassTaxonomyActivity(ContentTaxonomyActivity contentTaxonomyActivity) throws CloneNotSupportedException {
		if (contentTaxonomyActivity != null) {
			try {
				ContentTaxonomyActivity contentClassTaxonomyActivityInstance = (ContentTaxonomyActivity) contentTaxonomyActivity.clone();
				Iterator<String> internalTaxonomyCodes = contentTaxonomyActivity.getTaxonomyIds().keys();
				while (internalTaxonomyCodes.hasNext()) {
					String internalTaxonomyCode = internalTaxonomyCodes.next();
					String displayCode;
					displayCode = contentTaxonomyActivity.getTaxonomyIds().getString(internalTaxonomyCode);
					contentTaxonomyActivity.setDisplayCode(displayCode);
					Map<String, String> taxObject = new HashMap<String, String>();
					splitByTaxonomyCode(internalTaxonomyCode, taxObject);
					String subject = taxObject.get(Constants.SUBJECT), course = taxObject.get(Constants.COURSE), domain = taxObject.get(Constants.DOMAIN), standards = taxObject.get(Constants.STANDARDS), learningTarget = taxObject.get(Constants.LEARNING_TARGETS);
					contentTaxonomyActivity.setSubjectId(StringUtils.isNotBlank(subject) ? subject : Constants.NA);
					contentTaxonomyActivity.setCourseId(StringUtils.isNotBlank(course) ? appendHyphen(subject, course) : Constants.NA);
					contentTaxonomyActivity.setDomainId(StringUtils.isNotBlank(domain) ? appendHyphen(subject, course, domain) : Constants.NA);
					contentTaxonomyActivity.setStandardsId(StringUtils.isNotBlank(standards) ? appendHyphen(subject, course, domain, standards) : Constants.NA);
					contentTaxonomyActivity.setLearningTargetsId(StringUtils.isNotBlank(learningTarget) ? appendHyphen(subject, course, domain, standards, learningTarget) : Constants.NA);

					contentClassTaxonomyActivityInstance.setSubjectId(StringUtils.isNotBlank(subject) ? subject : Constants.NA);
					contentClassTaxonomyActivityInstance.setCourseId(StringUtils.isNotBlank(course) ? appendHyphen(subject, course) : Constants.NA);
					contentClassTaxonomyActivityInstance.setDomainId(StringUtils.isNotBlank(domain) ? appendHyphen(subject, course, domain) : Constants.NA);
					contentClassTaxonomyActivityInstance.setStandardsId(StringUtils.isNotBlank(standards) ? appendHyphen(subject, course, domain, standards) : Constants.NA);
					contentClassTaxonomyActivityInstance.setLearningTargetsId(StringUtils.isNotBlank(learningTarget) ? appendHyphen(subject, course, domain, standards, learningTarget) : Constants.NA);

					ResultSet taxActivityRows = baseCassandraDao.getContentTaxonomyActivity(contentTaxonomyActivity);
					if (taxActivityRows != null) {
						for (Row taxActivityColumns : taxActivityRows) {
							contentTaxonomyActivity.setViews(contentTaxonomyActivity.getViews() + taxActivityColumns.getLong(Constants.VIEWS));
							contentTaxonomyActivity.setTimeSpent(contentTaxonomyActivity.getTimeSpent() + taxActivityColumns.getLong(Constants._TIME_SPENT));
						}
					}
					contentTaxonomyActivity.setScore(contentTaxonomyActivity.getScore());
					baseCassandraDao.saveContentTaxonomyActivity(contentTaxonomyActivity);

					ResultSet taxClassActivityRows = baseCassandraDao.getContentClassTaxonomyActivity(contentClassTaxonomyActivityInstance);
					if (taxClassActivityRows != null) {
						for (Row taxClassActivityColumns : taxClassActivityRows) {
							contentClassTaxonomyActivityInstance.setViews(contentTaxonomyActivity.getViews() + taxClassActivityColumns.getLong(Constants.VIEWS));
							contentClassTaxonomyActivityInstance.setTimeSpent(contentTaxonomyActivity.getTimeSpent() + taxClassActivityColumns.getLong(Constants._TIME_SPENT));
						}
					}
					contentClassTaxonomyActivityInstance.setScore(contentClassTaxonomyActivityInstance.getScore());
					baseCassandraDao.saveContentClassTaxonomyActivity(contentClassTaxonomyActivityInstance);
				}
			} catch (Exception e) {
				LOG.error("Exception while split and save content& class taxonmy usage: ", e);
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
	private void saveUserSessions(EventBuilder event, UserSessionActivity userSessionActivity) {
		if (LoaderConstants.CPV1.getName().equalsIgnoreCase(event.getEventName())) {
			LOG.info("saving sessions.....");
			baseCassandraDao.saveUserSession(userSessionActivity.getSessionId(), event.getClassGooruId(), event.getCourseGooruId(), event.getUnitGooruId(),
					event.getLessonGooruId(), event.getContentGooruId(), event.getGooruUUID(), userSessionActivity.getCollectionType(),
					userSessionActivity.getEventType(), event.getEventTime());
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

	private void splitByTaxonomyCode(String taxonomyCode, Map<String, String> taxObject){
		int index = 0;
		for(String value : taxonomyCode.split(Constants.HASH)){
			   switch(index){
			   case 0:
				   taxObject.put(Constants.SUBJECT, value);
				   break;
			   case 1:
				   taxObject.put(Constants.COURSE, value);
				   break;
			   case 2:
				   taxObject.put(Constants.DOMAIN, value);
				   break;
			   case 3:
				   taxObject.put(Constants.STANDARDS, value);
				   break;
			   case 4:
				   taxObject.put(Constants.LEARNING_TARGETS, value);
				   break;
			   }
			   index++;
		   }
	}
	public static String appendHash(String... texts) {
		StringBuffer sb = new StringBuffer();
		for (String text : texts) {
			if (StringUtils.isNotBlank(text)) {
				if (sb.length() > 0) {
					sb.append(Constants.HASH);
				}
				sb.append(text);
			}
		}
		return sb.toString();
	}
	public static String appendHyphen(String... texts) {
		StringBuffer sb = new StringBuffer();
		for (String text : texts) {
			if (StringUtils.isNotBlank(text)) {
				if (sb.length() > 0) {
					sb.append(Constants.HYPHEN);
				}
				sb.append(text);
			}
		}
		return sb.toString();
	}
}
