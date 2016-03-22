package org.logger.event.cassandra.loader.dao;

import org.ednovo.data.model.AppDO;
import org.ednovo.data.model.ClassActivityDatacube;
import org.ednovo.data.model.ContentTaxonomyActivity;
import org.ednovo.data.model.StudentLocation;
import org.ednovo.data.model.StudentsClassActivity;
import org.ednovo.data.model.TaxonomyActivityDataCube;
import org.ednovo.data.model.UserSessionActivity;
import org.json.JSONObject;
import org.logger.event.cassandra.loader.Constants;
import org.logger.event.cassandra.loader.LoaderConstants;
import org.logger.event.cassandra.loader.PreparedQueries;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;

public class BaseCassandraRepoImpl extends BaseDAOCassandraImpl implements BaseCassandraRepo {

	private static final Logger LOG = LoggerFactory.getLogger(BaseCassandraRepoImpl.class);
	
	private static PreparedQueries queries = PreparedQueries.getInstance();

	
	/**
	 * Store all the session IDs if user playing collection from inside and outside of the class
	 * @param sessionId
	 * @param classUid
	 * @param courseUid
	 * @param unitUid
	 * @param lessonUid
	 * @param collectionUid
	 * @param userUid
	 * @param eventType
	 * @param eventTime
	 * @return true/false -- meaning operation success/fail
	 */
	@Override
	public boolean saveUserSession(String sessionId,String classUid,String courseUid,String unitUid,String lessonUid,String collectionUid,String userUid,String collectionType, String eventType,long eventTime) {
		try {
			BoundStatement boundStatement = new BoundStatement(queries.insertUserSession());
			boundStatement.bind(userUid,collectionUid,collectionType,classUid,courseUid,unitUid,lessonUid,eventTime,eventType,sessionId);
			getCassSession().execute(boundStatement);
		} catch (Exception e) {
			LOG.error("Error while storing user sessions" ,e);
			return false;
		}
		return true;
	}
	
	@Override
	public boolean saveLastSession(String classUid,String courseUid,String unitUid,String lessonUid,String collectionUid,String userUid,String sessionId) {
		try {
			BoundStatement boundStatement = new BoundStatement(queries.insertUserLastSession());
			boundStatement.bind(classUid,courseUid,unitUid,lessonUid,collectionUid,userUid,sessionId);
			getCassSession().execute(boundStatement);
		} catch (Exception e) {
			LOG.error("Error while storing user last sessions" ,e);
			return false;
		}
		return true;
	}
	/**
	 * Usage metrics will be store here by session wise 
	 * @param sessionId
	 * @param gooruOid
	 * @param collectionItemId
	 * @param answerObject
	 * @param attempts
	 * @param reaction
	 * @param resourceFormat
	 * @param resourceType
	 * @param score
	 * @param timeSpent
	 * @param views
	 * @return true/false -- meaning - operation success/fail
	 */
	@Override
	public boolean saveUserSessionActivity(UserSessionActivity userSessionActivity) {
		try {
			
			BoundStatement boundStatement = new BoundStatement(queries.insertUserSessionActivity());
			boundStatement.bind(userSessionActivity.getSessionId() ,userSessionActivity.getGooruOid() ,userSessionActivity.getCollectionItemId() ,userSessionActivity.getAnswerObject().toString() ,userSessionActivity.getAttempts() ,userSessionActivity.getCollectionType() ,userSessionActivity.getResourceType() ,userSessionActivity.getQuestionType() ,userSessionActivity.getAnswerStatus() ,userSessionActivity.getEventType() ,userSessionActivity.getParentEventId() ,userSessionActivity.getReaction() ,userSessionActivity.getScore() ,userSessionActivity.getTimeSpent() ,userSessionActivity.getViews());
			getCassSession().execute(boundStatement);
		} catch (Exception e) {
			LOG.error("Error while storing user sessions activity" ,e);
			return false;
		}
		return true;
	}
	
	/**
	 * CULA/C aggregated metrics will be stored.
	 * @param classUid
	 * @param courseUid
	 * @param unitUid
	 * @param lessonUid
	 * @param collectionUid
	 * @param userUid
	 * @param collectionType
	 * @param score
	 * @param timeSpent
	 * @param views
	 * @return true/false -- meaning operation success/fail
	 */
	@Override
	public boolean saveStudentsClassActivity(StudentsClassActivity studentsClassActivity) {
		try {
			BoundStatement boundStatement = new BoundStatement(queries.insertStudentsClassActivity());
			boundStatement.bind(studentsClassActivity.getClassUid() ,studentsClassActivity.getCourseUid() ,studentsClassActivity.getUnitUid() ,studentsClassActivity.getLessonUid() ,studentsClassActivity.getCollectionUid() ,studentsClassActivity.getUserUid() ,studentsClassActivity.getCollectionType() ,studentsClassActivity.getAttemptStatus() ,studentsClassActivity.getScore() ,studentsClassActivity.getTimeSpent() ,studentsClassActivity.getViews() ,studentsClassActivity.getReaction());
			getCassSession().execute(boundStatement);
		} catch (Exception e) {
			LOG.error("Error while storing class activity" ,e);
			return false;
		}
		return true;
	}
	
	@Override
	public boolean saveContentTaxonomyActivity(ContentTaxonomyActivity contentTaxonomyActivity) {
		try {
			BoundStatement boundStatement = new BoundStatement(queries.insertContentTaxonomyActivity());
			boundStatement.bind(contentTaxonomyActivity.getUserUid() ,contentTaxonomyActivity.getSubjectId() ,contentTaxonomyActivity.getCourseId() ,contentTaxonomyActivity.getDomainId() ,contentTaxonomyActivity.getStandardsId() ,contentTaxonomyActivity.getLearningTargetsId() ,contentTaxonomyActivity.getGooruOid() ,contentTaxonomyActivity.getResourceType() ,contentTaxonomyActivity.getQuestionType() ,contentTaxonomyActivity.getScore() ,contentTaxonomyActivity.getTimeSpent() ,contentTaxonomyActivity.getViews());
			getCassSession().execute(boundStatement);
		} catch (Exception e) {
			LOG.error("Error while storing taxonomy activity" ,e);
			return false;
		}
		return true;
	}
	
	@Override
	public boolean saveContentClassTaxonomyActivity(ContentTaxonomyActivity contentTaxonomyActivity) {
		try {
			BoundStatement boundStatement = new BoundStatement(queries.insertContentClassTaxonomyActivty());
			boundStatement.bind(contentTaxonomyActivity.getUserUid() ,contentTaxonomyActivity.getClassUid() ,contentTaxonomyActivity.getSubjectId() ,contentTaxonomyActivity.getCourseId() ,contentTaxonomyActivity.getDomainId() ,contentTaxonomyActivity.getStandardsId() ,contentTaxonomyActivity.getLearningTargetsId() ,contentTaxonomyActivity.getGooruOid() , contentTaxonomyActivity.getResourceType() ,contentTaxonomyActivity.getQuestionType() ,contentTaxonomyActivity.getScore() ,contentTaxonomyActivity.getTimeSpent() ,contentTaxonomyActivity.getViews());
			getCassSession().execute(boundStatement);
		} catch (Exception e) {
			LOG.error("Error while storing taxonomy activity" ,e);
			return false;
		}
		return true;
	}
	
	/**
	 * Students current/left off CULA/CR will be stored
	 * @param userUid
	 * @param classUid
	 * @param courseUid
	 * @param unitUid
	 * @param lessonUid
	 * @param collectionUid
	 * @param resourceUid
	 * @param sessionTime
	 * @return true/false -- meaning operation success/fail
	 */
	@Override
	public boolean saveStudentLocation(StudentLocation studentLocation) {
		try {
			BoundStatement boundStatement = new BoundStatement(queries.insertUserLastLocation());
			boundStatement.bind(studentLocation.getUserUid() ,studentLocation.getClassUid() ,studentLocation.getCourseUid() ,studentLocation.getUnitUid() ,studentLocation.getLessonUid() ,studentLocation.getCollectionUid() ,studentLocation.getCollectionType() ,studentLocation.getResourceUid() ,studentLocation.getSessionTime());
			getCassSession().execute(boundStatement);
		} catch (Exception e) {
			LOG.error("Error while storing taxonomy activity" ,e);
			return false;
		}
		return true;
	}
	
	@Override
	public UserSessionActivity compareAndMergeUserSessionActivity(UserSessionActivity userSessionActivity) {
		try {
			BoundStatement boundStatement = new BoundStatement(queries.selectUserSessionActivity());
			boundStatement.bind(userSessionActivity.getSessionId(),userSessionActivity.getGooruOid(),userSessionActivity.getCollectionItemId());
			ResultSet result = getCassSession().execute(boundStatement);
			if (result != null) {
				for (com.datastax.driver.core.Row columns : result) {
					userSessionActivity.setAttempts((userSessionActivity.getAttempts()) + columns.getLong(Constants.ATTEMPTS));
					userSessionActivity.setTimeSpent((userSessionActivity.getTimeSpent() + columns.getLong(Constants._TIME_SPENT)));
					userSessionActivity.setViews((userSessionActivity.getViews() + columns.getLong(Constants.VIEWS)));
				}
			}
		} catch (Exception e) {
			LOG.error("Error while retreving user sessions activity" ,e);
		}
		return userSessionActivity;
	}
	
	@Override
	public UserSessionActivity getUserSessionActivity(String sessionId, String gooruOid, String collectionItemId) {
		UserSessionActivity userSessionActivity = new UserSessionActivity();
		try {
			BoundStatement boundStatement = new BoundStatement(queries.selectUserSessionActivity());
			boundStatement.bind(sessionId,gooruOid,collectionItemId);
			ResultSet result = getCassSession().execute(boundStatement);
			if (result != null) {
				for (com.datastax.driver.core.Row columns : result) {
					userSessionActivity.setSessionId(columns.getString(Constants._SESSION_ID));
					userSessionActivity.setGooruOid(columns.getString(Constants._GOORU_OID));
					userSessionActivity.setCollectionItemId(columns.getString(Constants._COLLECTION_ITEM_ID));
					userSessionActivity.setAnswerObject(new JSONObject(columns.getString(Constants._ANSWER_OBECT)));
					userSessionActivity.setAnswerStatus(columns.getString(Constants._ANSWER_STATUS));
					userSessionActivity.setResourceType(columns.getString(Constants._RESOURCE_TYPE));
					userSessionActivity.setCollectionType(columns.getString(Constants._COLLECTION_TYPE));
					userSessionActivity.setQuestionType(columns.getString(Constants._QUESTION_TYPE));
					userSessionActivity.setAttempts(columns.getLong(Constants.ATTEMPTS));
					userSessionActivity.setEventType(columns.getString(Constants._EVENT_TYPE));
					userSessionActivity.setReaction(columns.getLong(Constants.REACTION));
					userSessionActivity.setScore(columns.getLong(Constants.SCORE));
					userSessionActivity.setTimeSpent(columns.getLong(Constants._TIME_SPENT));
					userSessionActivity.setViews(columns.getLong(Constants.VIEWS));
				}
			}
		} catch (Exception e) {
			LOG.error("Error while retreving user sessions activity" ,e);
		}
		return userSessionActivity;
	}
	@Override
	public StudentsClassActivity compareAndMergeStudentsClassActivity(StudentsClassActivity studentsClassActivity) {
		try {
			BoundStatement boundStatement = new BoundStatement(queries.selectStudentClassActivity());
			boundStatement.bind(studentsClassActivity.getClassUid() ,studentsClassActivity.getUserUid() ,studentsClassActivity.getCollectionType() ,studentsClassActivity.getCourseUid() ,studentsClassActivity.getUnitUid() ,studentsClassActivity.getLessonUid() ,studentsClassActivity.getCollectionUid());
			ResultSet result = getCassSession().execute(boundStatement);
			if (result != null) {
				for (com.datastax.driver.core.Row columns : result) {
					studentsClassActivity.setTimeSpent((studentsClassActivity.getTimeSpent() + columns.getLong(Constants._TIME_SPENT)));
					studentsClassActivity.setViews((studentsClassActivity.getViews())+columns.getLong(Constants.VIEWS));
				}
			}
		} catch (Exception e) {
			LOG.error("Error while retreving students class activity" ,e);
		}
		return studentsClassActivity;
	}
	
	@Override
	public boolean updateReaction(UserSessionActivity userSessionActivity) {
		try {
			BoundStatement boundStatement = new BoundStatement(queries.updateReaction());
			boundStatement.bind(userSessionActivity.getSessionId(),userSessionActivity.getGooruOid(),userSessionActivity.getCollectionItemId(),userSessionActivity.getReaction());
			getCassSession().execute(boundStatement);
		} catch (Exception e) {
			LOG.error("Error while storing user sessions activity" ,e);
			return false;
		}
		return true;
	}
	
	@Override
	public boolean  hasClassActivity(StudentsClassActivity studentsClassActivity) {
		boolean hasActivity = false;
		try {
			BoundStatement boundStatement = new BoundStatement(queries.selectStudentClassActivity());
			boundStatement.bind(studentsClassActivity.getClassUid() ,studentsClassActivity.getCourseUid() ,studentsClassActivity.getUnitUid() ,studentsClassActivity.getLessonUid() ,studentsClassActivity.getCollectionUid() ,studentsClassActivity.getCollectionType() ,studentsClassActivity.getUserUid());
			ResultSet result = getCassSession().execute(boundStatement);
			if(result != null){
				hasActivity = true;
			}
		} catch (Exception e) {
			LOG.error("Error while retreving students class activity" ,e);
		}
		return hasActivity;
	}

	@Override
	public UserSessionActivity getSessionScore(UserSessionActivity userSessionActivity, String eventName) {
		long score = 0L;
		try {
			String gooruOid = "";
			long questionCount = 0;
			long reactionCount = 0;
			long totalReaction = 0;
			if(LoaderConstants.CPV1.getName().equalsIgnoreCase(eventName)){
				gooruOid = userSessionActivity.getGooruOid();
			}else if (LoaderConstants.CRPV1.getName().equalsIgnoreCase(eventName)){
				gooruOid = userSessionActivity.getParentGooruOid();
			}
			
			BoundStatement boundStatement = new BoundStatement(queries.selectUserSessionActivityBySessionId());
			boundStatement.bind(userSessionActivity.getSessionId());
			ResultSet result = getCassSession().execute(boundStatement);
			
			if (result != null) {
				for (com.datastax.driver.core.Row columns : result) {
						if(!gooruOid.equalsIgnoreCase(columns.getString(Constants._GOORU_OID)) && Constants.QUESTION.equalsIgnoreCase(columns.getString("resource_type"))){
							questionCount++;
							score += columns.getLong(Constants.SCORE);
						}
						if(LoaderConstants.CPV1.getName().equalsIgnoreCase(eventName) && columns.getLong(Constants.REACTION) > 0){
							reactionCount++;
							totalReaction += columns.getLong(Constants.REACTION);
						}
				}
				userSessionActivity.setScore(questionCount > 0 ? (score/questionCount) : 0);
				userSessionActivity.setReaction(reactionCount > 0 ? (totalReaction/reactionCount) : userSessionActivity.getReaction());
			}
		} catch (Exception e) {
			LOG.error("Error while retreving user sessions activity" ,e);
		}
		return userSessionActivity;
	}

	@Override
	public boolean saveClassActivityDataCube(ClassActivityDatacube studentsClassActivity) {
		try {			
			BoundStatement boundStatement = new BoundStatement(queries.insertClassActivityDataCube());
			boundStatement.bind(studentsClassActivity.getRowKey() ,studentsClassActivity.getLeafNode() ,studentsClassActivity.getCollectionType() ,studentsClassActivity.getUserUid() ,studentsClassActivity.getScore() ,studentsClassActivity.getTimeSpent() ,studentsClassActivity.getViews() ,studentsClassActivity.getReaction() ,studentsClassActivity.getCompletedCount());
			getCassSession().execute(boundStatement);
		} catch (Exception e) {
			LOG.error("Error while storing class activity" ,e);
			return false;
		}
		return true;
	}
	
	@Override
	public ClassActivityDatacube getStudentsClassActivityDatacube(String rowKey, String userUid, String collectionType) {
		ClassActivityDatacube classActivityDatacube = new ClassActivityDatacube();
		long itemCount = 0L;
		
		try {			
			BoundStatement boundStatement = new BoundStatement(queries.selectAllClassActivityDataCube());
			boundStatement.bind(rowKey,collectionType,userUid);
			ResultSet result = getCassSession().execute(boundStatement);
				
			if (result != null) {
				long score = 0L;
				long views = 0L;
				long timeSpent = 0L;
				for (com.datastax.driver.core.Row columns : result) {
					String attemptStatus = columns.getString(Constants._ATTEMPT_STATUS);
					attemptStatus = attemptStatus == null ? Constants.COMPLETED : attemptStatus;
					if(attemptStatus.equals(Constants.COMPLETED)){
						itemCount++;
						score += columns.getLong(Constants.SCORE);
					}
					timeSpent += columns.getLong(Constants._TIME_SPENT);
					views += columns.getLong(Constants.VIEWS);
				}
				classActivityDatacube.setCollectionType(collectionType);
				classActivityDatacube.setUserUid(userUid);
				classActivityDatacube.setScore(itemCount > 0 ? (score/itemCount) : 0L);
				classActivityDatacube.setTimeSpent(timeSpent);
				classActivityDatacube.setViews(views);
				classActivityDatacube.setCompletedCount(itemCount);
			}
		} catch (Exception e) {
			LOG.error("Error while retreving students class activity", e);
		}
		return classActivityDatacube;
	}
	
	@Override
	public ResultSet getTaxonomy(String rowKey){
		ResultSet result = null;
		try {
			BoundStatement boundStatement = new BoundStatement(queries.selectTaxonomyParentNode());
			boundStatement.bind(rowKey);
			result = getCassSession().execute(boundStatement);
			
		} catch (Exception e) {
			LOG.error("Error while retreving txonomy tree", e);
		}
		return result;
	}
	
	@Override
	public ResultSet getContentTaxonomyActivity(ContentTaxonomyActivity contentTaxonomyActivity){
		ResultSet result = null;
		try {
			BoundStatement boundStatement = new BoundStatement(queries.selectContentTaxonomyActivity());
			boundStatement.bind(contentTaxonomyActivity.getUserUid() ,contentTaxonomyActivity.getSubjectId() ,contentTaxonomyActivity.getCourseId() ,contentTaxonomyActivity.getDomainId() ,contentTaxonomyActivity.getStandardsId() ,contentTaxonomyActivity.getLearningTargetsId() ,contentTaxonomyActivity.getGooruOid());
			result = getCassSession().execute(boundStatement);
			
		} catch (Exception e) {
			LOG.error("Error while retreving students class activity", e);
		}
		return result;
	}
	
	@Override
	public ResultSet getContentClassTaxonomyActivity(ContentTaxonomyActivity contentTaxonomyActivity){
		ResultSet result = null;
		try {
			BoundStatement boundStatement = new BoundStatement(queries.selectContentClassTaxonomyActivity());
			boundStatement.bind(contentTaxonomyActivity.getClassUid() ,contentTaxonomyActivity.getUserUid() ,contentTaxonomyActivity.getResourceType() ,contentTaxonomyActivity.getSubjectId() ,contentTaxonomyActivity.getCourseId() ,contentTaxonomyActivity.getDomainId() ,contentTaxonomyActivity.getStandardsId() ,contentTaxonomyActivity.getLearningTargetsId() ,contentTaxonomyActivity.getGooruOid());
			result = getCassSession().execute(boundStatement);
			
		} catch (Exception e) {
			LOG.error("Error while retreving students class activity", e);
		}
		return result;
	}
	
	@Override
	public ResultSet getContentTaxonomyActivityDataCube(String rowKey, String columnKey){
		ResultSet result = null;
		try {
			BoundStatement boundStatement = new BoundStatement(queries.selectTaxonomyActivityDataCube());
			boundStatement.bind(rowKey,columnKey);
			result = getCassSession().execute(boundStatement);
			
		} catch (Exception e) {
			LOG.error("Error while retreving students class activity", e);
		}
		return result;
	}
	@Override
	public long getContentTaxonomyActivityScore(String rowKey){
		ResultSet result = null;
		long questionCount = 0L;
		long score = 0L;
		try {
			BoundStatement boundStatement = new BoundStatement(queries.selectTaxonomyActivityDataCube());
			boundStatement.bind(rowKey);
			result = getCassSession().execute(boundStatement);
			
			if (result != null) {
				for (com.datastax.driver.core.Row columns : result) {
							questionCount++;
							score += columns.getLong(Constants.SCORE);
				}
				score = questionCount > 0 ? (score/questionCount) : 0;
			}
		} catch (Exception e) {
			LOG.error("Error while retreving students class activity", e);
		}
		return score;
	}
	
	@Override
	public boolean saveTaxonomyActivityDataCube(TaxonomyActivityDataCube taxonomyActivityDataCube) {
		try {
			BoundStatement boundStatement = new BoundStatement(queries.insertTaxonomyActivityDataCube());
			boundStatement.bind(taxonomyActivityDataCube.getRowKey(),taxonomyActivityDataCube.getLeafNode(),taxonomyActivityDataCube.getViews(),taxonomyActivityDataCube.getAttempts(),taxonomyActivityDataCube.getResourceTimespent(),taxonomyActivityDataCube.getQuestionTimespent(),taxonomyActivityDataCube.getScore());
			getCassSession().execute(boundStatement);
		} catch (Exception e) {
			LOG.error("Error while storing question grade" ,e);
			return false;
		}
		return true;
	}
	
	@Override
	public boolean saveQuestionGrade(String teacherId, String userId, String sessionId, String questionId, long score) {
		try {
			BoundStatement boundStatement = new BoundStatement(queries.insertUserQuestionGrade());
			boundStatement.bind(teacherId,userId,sessionId,questionId,score);
			getCassSession().execute(boundStatement);
		} catch (Exception e) {
			LOG.error("Error while storing question grade" ,e);
			return false;
		}
		return true;
	}

	@Override
	public ResultSet getQuestionsGradeBySessionId(String teacherId, String userId, String sessionId) {
		ResultSet result = null;
		try {
			BoundStatement boundStatement = new BoundStatement(queries.selectUserQuestionGradeBySession());
			boundStatement.bind(teacherId,userId,sessionId);
			result = getCassSession().execute(boundStatement);
			
		} catch (Exception e) {
			LOG.error("Exception while read questions grade by session", e);
		}
		return result;
	}
	
	@Override
	public ResultSet getQuestionsGradeByQuestionId(String teacherId, String userId, String sessionId, String questionId) {
		ResultSet result = null;
		try {
			BoundStatement boundStatement = new BoundStatement(queries.selectUserQuestionGradeByQuestion());
			boundStatement.bind(teacherId,userId,sessionId,questionId);
			result = getCassSession().execute(boundStatement);
			
		} catch (Exception e) {
			LOG.error("Exception while read questions grade by session", e);
		}
		return result;
	}
	@Override
	public boolean saveQuestionGradeInSession(String sessionId, String questionId, String collectionItemId, String status, long score) {
		try {			
			BoundStatement boundStatement = new BoundStatement(queries.updateSessionScore());
			boundStatement.bind(sessionId,questionId,collectionItemId,status,score);
			getCassSession().execute(boundStatement);
		} catch (Exception e) {
			LOG.error("Error while storing question grade in session" ,e);
			return false;
		}
		return true;
	}
	
	@Override
	public boolean insertEventsTimeline(String eventTime, String eventId) {
		try {			
			BoundStatement boundStatement = new BoundStatement(queries.insertEventsTimeline());
			boundStatement.bind(eventTime,eventId);
			getCassSession().execute(boundStatement);
		} catch (Exception e) {
			LOG.error("Error while storing events timeline" ,e);
			return false;
		}
		return true;
	}
	@Override
	public boolean insertEvents(String eventId, String event) {
		try {			
			BoundStatement boundStatement = new BoundStatement(queries.insertEvents());
			boundStatement.bind(eventId,event);
			getCassSession().execute(boundStatement);
		} catch (Exception e) {
			LOG.error("Error while storing events" ,e);
			return false;
		}
		return true;
	}
	@Override
	public boolean updateStatisticalCounterData(String clusteringKey, String metricsName, Object metricsValue) {
		try {			
			BoundStatement boundStatement = new BoundStatement(queries.updateStatustucalCounterData());
			boundStatement.bind(metricsValue,clusteringKey,metricsName);
			getCassSession().execute(boundStatement);
		} catch (Exception e) {
			LOG.error("Error while storing statistical data" ,e);
			return false;
		}
		return true;
	}

	@Override
	public AppDO getApiKeyDetails(String apiKey) {
		AppDO appDO = null;
		try {
			BoundStatement boundStatement = new BoundStatement(queries.selectApiKey());
			boundStatement.bind(apiKey);
			ResultSet result = getCassSession().execute(boundStatement);
			if (result != null) {
				appDO = new AppDO();
				for (com.datastax.driver.core.Row columns : result) {
					appDO.setApiKey(apiKey);
					appDO.setAppName(columns.getString("appName"));
					appDO.setEndPoint(columns.getString("endPoint"));
					appDO.setDataPushingIntervalInMillsecs(columns.getString("pushIntervalMs"));
				}
			}
		} catch (Exception e) {
			LOG.error("Error while retreving user sessions activity", e);
		}
		return appDO;
	}
}
