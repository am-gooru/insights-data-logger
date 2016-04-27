package org.logger.event.cassandra.loader.dao;

import java.util.Set;

import org.ednovo.data.model.AppDO;
import org.ednovo.data.model.ClassActivityDatacube;
import org.ednovo.data.model.ContentTaxonomyActivity;
import org.ednovo.data.model.StudentLocation;
import org.ednovo.data.model.StudentsClassActivity;
import org.ednovo.data.model.TaxonomyActivityDataCube;
import org.ednovo.data.model.UserSessionActivity;
import org.ednovo.data.model.UserSessionTaxonomyActivity;

import com.datastax.driver.core.ResultSet;

public interface BaseCassandraRepo {

	static BaseCassandraRepo instance() {
		return new BaseCassandraRepoImpl();

	}
	
	boolean saveUserSession(String sessionId, String classUid, String courseUid, String unitUid, String lessonUid, String collectionUid, String userUid, String collectionType, String eventType,
			long eventTime);

	boolean saveUserSessionActivity(UserSessionActivity userSessionActivity);

	boolean saveStudentsClassActivity(StudentsClassActivity studentsClassActivity);

	boolean saveContentTaxonomyActivity(ContentTaxonomyActivity contentTaxonomyActivity);

	boolean saveContentClassTaxonomyActivity(ContentTaxonomyActivity contentTaxonomyActivity);

	boolean saveStudentLocation(StudentLocation studentLocation);

	UserSessionActivity compareAndMergeUserSessionActivity(UserSessionActivity userSessionActivity);

	UserSessionActivity getUserSessionActivity(String sessionId, String gooruOid, String collectionItemId);

	StudentsClassActivity compareAndMergeStudentsClassActivity(StudentsClassActivity studentsClassActivity);

	boolean updateReaction(UserSessionActivity userSessionActivity);

	boolean hasClassActivity(StudentsClassActivity studentsClassActivity);

	UserSessionActivity getSessionScore(UserSessionActivity userSessionActivity, String eventName);

	boolean saveClassActivityDataCube(ClassActivityDatacube studentsClassActivity);

	ClassActivityDatacube getStudentsClassActivityDatacube(String rowKey, String userUid, String collectionType);

	ResultSet getTaxonomy(String rowKey);

	ResultSet getContentTaxonomyActivity(ContentTaxonomyActivity contentTaxonomyActivity);

	ResultSet getContentClassTaxonomyActivity(ContentTaxonomyActivity contentTaxonomyActivity);

	ResultSet getContentTaxonomyActivityDataCube(String rowKey, String columnKey);

	long getContentTaxonomyActivityScore(String rowKey);

	boolean saveTaxonomyActivityDataCube(TaxonomyActivityDataCube taxonomyActivityDataCube);

	boolean saveQuestionGrade(String teacherId, String userId, String sessionId, String questionId, long score);

	ResultSet getQuestionsGradeBySessionId(String teacherId, String userId, String sessionId);

	ResultSet getQuestionsGradeByQuestionId(String teacherId, String userId, String sessionId, String questionId);

	boolean saveQuestionGradeInSession(String sessionId, String questionId, String collectionItemId, String status, long score);

	boolean saveLastSession(String classUid, String courseUid, String unitUid, String lessonUid, String collectionUid, String userUid, String sessionId);

	boolean insertEvents(String eventId, String event);

	boolean insertEventsTimeline(String eventTime, String eventId);

	boolean incrementStatisticalCounterData(String clusteringKey, String metricsName, Object metricsValue);

	AppDO getApiKeyDetails(String apiKey);

	ResultSet getClassMembers(String classId);

	boolean deleteCourseUsage(StudentsClassActivity studentsClassActivity, String studentId, String collectionType);

	boolean deleteAssessmentOrCollectionUsage(StudentsClassActivity studentsClassActivity,String studentId);

	boolean deleteLessonUsage(StudentsClassActivity studentsClassActivity, String studentId, String collectionType);

	boolean deleteUnitUsage(StudentsClassActivity studentsClassActivity, String studentId, String collectionType);

	boolean deleteClassActivityDataCube(String rowKey, String collectionType, String studentId, String leafNode);

	boolean deleteClassActivityDataCube(String rowKey);

	void saveClassMembers(String classId, Set<String> studentId);

	void removeClassMembers(String classId, Set<String> studentId);

	void updateCollaborators(String classId, Set<String> collaborators);

	void updateContentCreators(String classId, String creator);

	boolean balanceCounterData(String clusteringKey, String metricsName, Long metricsValue);

	boolean addStatPublisherQueue(String metricsName, String gooruOid, String resourceType, long eventTime);

	boolean incrementUserStatisticalCounterData(String clusteringKey, String userUid, String metricsName, Object metricsValue);

	boolean balanceUserCounterData(String clusteringKey, String userUid, String metricsName, Long metricsValue);

	boolean insertUserSessionTaxonomyActivity(UserSessionTaxonomyActivity userSessionTaxonomyActivity);

	ResultSet mergeUserSessionTaxonomyActivity(UserSessionTaxonomyActivity userSessionTaxonomyActivity);

	boolean decrementUserStatisticalCounterData(String clusteringKey, String userUid, String metricsName, Object metricsValue);

	boolean decrementStatisticalCounterData(String clusteringKey, String metricsName, Object metricsValue);

}
