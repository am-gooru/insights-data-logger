package org.logger.event.cassandra.loader.dao;

import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.ednovo.data.model.AppDO;
import org.ednovo.data.model.ClassActivityDatacube;
import org.ednovo.data.model.ContentTaxonomyActivity;
import org.ednovo.data.model.StudentLocation;
import org.ednovo.data.model.StudentsClassActivity;
import org.ednovo.data.model.TaxonomyActivityDataCube;
import org.ednovo.data.model.UserSessionActivity;
import org.ednovo.data.model.UserSessionTaxonomyActivity;
import org.logger.event.cassandra.loader.Constants;
import org.logger.event.cassandra.loader.LoaderConstants;
import org.logger.event.cassandra.loader.PreparedQueries;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;

public class BaseCassandraRepoImpl extends BaseDAOCassandraImpl implements BaseCassandraRepo {

	private static final Logger LOG = LoggerFactory.getLogger(BaseCassandraRepoImpl.class);

	private static PreparedQueries queries = PreparedQueries.getInstance();

	/**
	 * Store all the session IDs if user playing collection from inside and outside of the class
	 * 
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
	public boolean saveUserSession(String sessionId, String classUid, String courseUid, String unitUid, String lessonUid, String collectionUid, String userUid, String collectionType, String eventType,
			long eventTime) {
		try {
			BoundStatement boundStatement = new BoundStatement(queries.insertUserSession());
			boundStatement.bind(userUid, collectionUid, collectionType, classUid, courseUid, unitUid, lessonUid, eventTime, eventType, sessionId);
			getAnalyticsCassSession().executeAsync(boundStatement);
		} catch (Exception e) {
			LOG.error("Error while storing user sessions", e);
			return false;
		}
		return true;
	}

	@Override
	public boolean saveLastSession(String classUid, String courseUid, String unitUid, String lessonUid, String collectionUid, String userUid, String sessionId) {
		try {
			BoundStatement boundStatement = new BoundStatement(queries.insertUserLastSession());
			boundStatement.bind(classUid, courseUid, unitUid, lessonUid, collectionUid, userUid, sessionId);
			getAnalyticsCassSession().executeAsync(boundStatement);
		} catch (Exception e) {
			LOG.error("Error while storing user last sessions", e);
			return false;
		}
		return true;
	}

	/**
	 * Usage metrics will be store here by session wise
	 * 
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
			boundStatement.bind(userSessionActivity.getSessionId(), userSessionActivity.getGooruOid(), userSessionActivity.getCollectionItemId(), (userSessionActivity.getAnswerObject() != null ? userSessionActivity.getAnswerObject().toString() : Constants.NA),
					userSessionActivity.getAttempts(), userSessionActivity.getCollectionType(), userSessionActivity.getResourceType(), userSessionActivity.getQuestionType(),
					userSessionActivity.getAnswerStatus(), userSessionActivity.getEventType(), userSessionActivity.getParentEventId(), userSessionActivity.getReaction(),
					userSessionActivity.getScore(), userSessionActivity.getTimeSpent(), userSessionActivity.getViews());
			getAnalyticsCassSession().executeAsync(boundStatement);
		} catch (Exception e) {
			LOG.error("Error while storing user sessions activity", e);
			return false;
		}
		return true;
	}

	/**
	 * CULA/C aggregated metrics will be stored.
	 * 
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
			boundStatement.bind(studentsClassActivity.getClassUid(), studentsClassActivity.getCourseUid(), studentsClassActivity.getUnitUid(), studentsClassActivity.getLessonUid(),
					studentsClassActivity.getCollectionUid(), studentsClassActivity.getCollectionType(),studentsClassActivity.getUserUid(), studentsClassActivity.getAttemptStatus(),
					studentsClassActivity.getScore(), studentsClassActivity.getTimeSpent(), studentsClassActivity.getViews(), studentsClassActivity.getReaction());
			getAnalyticsCassSession().executeAsync(boundStatement);
		} catch (Exception e) {
			LOG.error("Error while storing class activity", e);
			return false;
		}
		return true;
	}

	@Override
	public boolean saveContentTaxonomyActivity(ContentTaxonomyActivity contentTaxonomyActivity) {
		try {
			BoundStatement boundStatement = new BoundStatement(queries.insertContentTaxonomyActivity());
			boundStatement.bind(contentTaxonomyActivity.getUserUid(), contentTaxonomyActivity.getSubjectId(), contentTaxonomyActivity.getCourseId(), contentTaxonomyActivity.getDomainId(),
					contentTaxonomyActivity.getStandardsId(), contentTaxonomyActivity.getLearningTargetsId(), contentTaxonomyActivity.getGooruOid(), contentTaxonomyActivity.getDisplayCode(), contentTaxonomyActivity.getResourceType(),
					contentTaxonomyActivity.getQuestionType(), contentTaxonomyActivity.getScore(), contentTaxonomyActivity.getTimeSpent(), contentTaxonomyActivity.getViews());
			getAnalyticsCassSession().executeAsync(boundStatement);
		} catch (Exception e) {
			LOG.error("Error while storing taxonomy activity", e);
			return false;
		}
		return true;
	}

	@Override
	public boolean saveContentClassTaxonomyActivity(ContentTaxonomyActivity contentTaxonomyActivity) {
		try {
			BoundStatement boundStatement = new BoundStatement(queries.insertContentClassTaxonomyActivty());
			boundStatement.bind(contentTaxonomyActivity.getUserUid(), contentTaxonomyActivity.getClassUid(), contentTaxonomyActivity.getSubjectId(), contentTaxonomyActivity.getCourseId(),
					contentTaxonomyActivity.getDomainId(), contentTaxonomyActivity.getStandardsId(), contentTaxonomyActivity.getLearningTargetsId(), contentTaxonomyActivity.getGooruOid(),contentTaxonomyActivity.getDisplayCode(),
					contentTaxonomyActivity.getResourceType(), contentTaxonomyActivity.getQuestionType(), contentTaxonomyActivity.getScore(), contentTaxonomyActivity.getTimeSpent(),
					contentTaxonomyActivity.getViews());
			getAnalyticsCassSession().executeAsync(boundStatement);
		} catch (Exception e) {
			LOG.error("Error while storing taxonomy activity", e);
			return false;
		}
		return true;
	}

	/**
	 * Students current/left off CULA/CR will be stored
	 * 
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
			boundStatement.bind(studentLocation.getUserUid(), studentLocation.getClassUid(), studentLocation.getCourseUid(), studentLocation.getUnitUid(), studentLocation.getLessonUid(),
					studentLocation.getCollectionUid(), studentLocation.getCollectionType(), studentLocation.getResourceUid(), studentLocation.getSessionTime());
			getAnalyticsCassSession().executeAsync(boundStatement);
		} catch (Exception e) {
			LOG.error("Error while storing taxonomy activity", e);
			return false;
		}
		return true;
	}

	@Override
	public UserSessionActivity compareAndMergeUserSessionActivity(UserSessionActivity userSessionActivity) {
		try {
			BoundStatement boundStatement = new BoundStatement(queries.selectUserSessionActivity());
			boundStatement.bind(userSessionActivity.getSessionId(), userSessionActivity.getGooruOid(), userSessionActivity.getCollectionItemId());
			ResultSetFuture resultSetFuture = getAnalyticsCassSession().executeAsync(boundStatement);
			ResultSet result = resultSetFuture.get();
			if (result != null) {
				for (Row columns : result) {
					userSessionActivity.setAttempts((userSessionActivity.getAttempts()) + columns.getLong(Constants.ATTEMPTS));
					userSessionActivity.setTimeSpent((userSessionActivity.getTimeSpent() + columns.getLong(Constants._TIME_SPENT)));
					userSessionActivity.setViews((userSessionActivity.getViews() + columns.getLong(Constants.VIEWS)));
				}
			}
		} catch (Exception e) {
			LOG.error("Error while retreving user sessions activity", e);
		}
		return userSessionActivity;
	}

	@Override
	public UserSessionActivity getUserSessionActivity(String sessionId, String gooruOid, String collectionItemId) {
		UserSessionActivity userSessionActivity = new UserSessionActivity();
		try {
			BoundStatement boundStatement = new BoundStatement(queries.selectUserSessionActivity());
			boundStatement.bind(sessionId, gooruOid, collectionItemId);
			ResultSetFuture resultSetFuture = getAnalyticsCassSession().executeAsync(boundStatement);
			ResultSet result = resultSetFuture.get();
			if (result != null) {
				for (Row columns : result) {
					userSessionActivity.setSessionId(columns.getString(Constants._SESSION_ID));
					userSessionActivity.setGooruOid(columns.getString(Constants._GOORU_OID));
					userSessionActivity.setCollectionItemId(columns.getString(Constants._COLLECTION_ITEM_ID));
					userSessionActivity.setAnswerObject(columns.getString(Constants._ANSWER_OBECT));
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
			LOG.error("Error while retreving user sessions activity", e);
		}
		return userSessionActivity;
	}

	@Override
	public StudentsClassActivity compareAndMergeStudentsClassActivity(StudentsClassActivity studentsClassActivity) {
		try {
			BoundStatement boundStatement = new BoundStatement(queries.selectStudentClassActivity());
			boundStatement.bind(studentsClassActivity.getClassUid(), studentsClassActivity.getCourseUid(),
					studentsClassActivity.getUnitUid(), studentsClassActivity.getLessonUid(), studentsClassActivity.getCollectionUid(),studentsClassActivity.getCollectionType(),studentsClassActivity.getUserUid());
			ResultSetFuture resultSetFuture = getAnalyticsCassSession().executeAsync(boundStatement);
			ResultSet result = resultSetFuture.get();
			if (result != null) {
				for (Row columns : result) {
					studentsClassActivity.setTimeSpent((studentsClassActivity.getTimeSpent() + columns.getLong(Constants._TIME_SPENT)));
					studentsClassActivity.setViews((studentsClassActivity.getViews()) + columns.getLong(Constants.VIEWS));
				}
			}
		} catch (Exception e) {
			LOG.error("Error while retreving students class activity", e);
		}
		return studentsClassActivity;
	}

	@Override
	public boolean updateReaction(UserSessionActivity userSessionActivity) {
		try {
			BoundStatement boundStatement = new BoundStatement(queries.updateReaction());
			boundStatement.bind(userSessionActivity.getSessionId(), userSessionActivity.getGooruOid(), userSessionActivity.getCollectionItemId(), userSessionActivity.getReaction());
			getAnalyticsCassSession().executeAsync(boundStatement);
		} catch (Exception e) {
			LOG.error("Error while storing user sessions activity", e);
			return false;
		}
		return true;
	}

	@Override
	public boolean hasClassActivity(StudentsClassActivity studentsClassActivity) {
		boolean hasActivity = false;
		try {
			BoundStatement boundStatement = new BoundStatement(queries.selectStudentClassActivity());
			boundStatement.bind(studentsClassActivity.getClassUid(), studentsClassActivity.getCourseUid(), studentsClassActivity.getUnitUid(), studentsClassActivity.getLessonUid(),
					studentsClassActivity.getCollectionUid(), studentsClassActivity.getCollectionType(), studentsClassActivity.getUserUid());
			ResultSetFuture resultSetFuture = getAnalyticsCassSession().executeAsync(boundStatement);
			ResultSet result = resultSetFuture.get();
			if (result != null) {
				hasActivity = true;
			}
		} catch (Exception e) {
			LOG.error("Error while retreving students class activity", e);
		}
		return hasActivity;
	}

	@Override
	public UserSessionActivity getSessionScore(UserSessionActivity userSessionActivity, String eventName) {
		long score = 0L;
		try {
			String gooruOid = "";
			long attemptedQuestionCount = 0;
			long reactionCount = 0;
			long totalReaction = 0;
			if (LoaderConstants.CPV1.getName().equalsIgnoreCase(eventName)) {
				gooruOid = userSessionActivity.getGooruOid();
			} else if (LoaderConstants.CRPV1.getName().equalsIgnoreCase(eventName)) {
				gooruOid = userSessionActivity.getParentGooruOid();
			}

			BoundStatement boundStatement = new BoundStatement(queries.selectUserSessionActivityBySessionId());
			boundStatement.bind(userSessionActivity.getSessionId());
			ResultSetFuture resultSetFuture = getAnalyticsCassSession().executeAsync(boundStatement);
			ResultSet result = resultSetFuture.get();
			if (result != null) {
				for (Row columns : result) {
					if (!gooruOid.equalsIgnoreCase(columns.getString(Constants._GOORU_OID)) && Constants.QUESTION.equalsIgnoreCase(columns.getString("resource_type"))) {
						attemptedQuestionCount++;
						score += columns.getLong(Constants.SCORE);
					}
					if (LoaderConstants.CPV1.getName().equalsIgnoreCase(eventName) && columns.getLong(Constants.REACTION) > 0) {
						reactionCount++;
						totalReaction += columns.getLong(Constants.REACTION);
					}
				}
				userSessionActivity.setReaction(reactionCount > 0 ? (totalReaction / reactionCount) : userSessionActivity.getReaction());
			}
			LOG.info("{} questions attempted in {} questions ", attemptedQuestionCount, userSessionActivity.getQuestionCount());
			userSessionActivity.setScore(userSessionActivity.getQuestionCount() > 0 ? (score / userSessionActivity.getQuestionCount()) : 0);
		} catch (Exception e) {
			LOG.error("Error while retreving user sessions activity", e);
		}
		return userSessionActivity;
	}

	@Override
	public boolean saveClassActivityDataCube(ClassActivityDatacube studentsClassActivity) {
		try {
			BoundStatement boundStatement = new BoundStatement(queries.insertClassActivityDataCube());
			boundStatement.bind(studentsClassActivity.getRowKey(), studentsClassActivity.getLeafNode(), studentsClassActivity.getCollectionType(), studentsClassActivity.getUserUid(),
					studentsClassActivity.getScore(), studentsClassActivity.getTimeSpent(), studentsClassActivity.getViews(), studentsClassActivity.getReaction(),
					studentsClassActivity.getCompletedCount());
			getAnalyticsCassSession().executeAsync(boundStatement);
		} catch (Exception e) {
			LOG.error("Error while storing class activity", e);
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
			boundStatement.bind(rowKey, collectionType, userUid);
			ResultSetFuture resultSetFuture = getAnalyticsCassSession().executeAsync(boundStatement);
			ResultSet result = resultSetFuture.get();
			if (result != null) {
				long score = 0L;
				long views = 0L;
				long timeSpent = 0L;
				for (Row columns : result) {
					String attemptStatus = columns.getString(Constants._ATTEMPT_STATUS);
					attemptStatus = attemptStatus == null ? Constants.COMPLETED : attemptStatus;
					if (attemptStatus.equals(Constants.COMPLETED)) {
						itemCount++;
						score += columns.getLong(Constants.SCORE);
					}
					timeSpent += columns.getLong(Constants._TIME_SPENT);
					views += columns.getLong(Constants.VIEWS);
				}
				classActivityDatacube.setCollectionType(collectionType);
				classActivityDatacube.setUserUid(userUid);
				classActivityDatacube.setScore(itemCount > 0 ? (score / itemCount) : 0L);
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
	public ResultSet getTaxonomy(String rowKey) {
		ResultSet result = null;
		try {
			BoundStatement boundStatement = new BoundStatement(queries.selectTaxonomyParentNode());
			boundStatement.bind(rowKey);
			ResultSetFuture resultSetFuture = getAnalyticsCassSession().executeAsync(boundStatement);
			result = resultSetFuture.get();
		} catch (Exception e) {
			LOG.error("Error while retreving txonomy tree", e);
		}
		return result;
	}

	@Override
	public ResultSet getContentTaxonomyActivity(ContentTaxonomyActivity contentTaxonomyActivity) {
		ResultSet result = null;
		try {
			BoundStatement boundStatement = new BoundStatement(queries.selectContentTaxonomyActivity());
			boundStatement.bind(contentTaxonomyActivity.getUserUid(), contentTaxonomyActivity.getSubjectId(), contentTaxonomyActivity.getCourseId(), contentTaxonomyActivity.getDomainId(),
					contentTaxonomyActivity.getStandardsId(), contentTaxonomyActivity.getLearningTargetsId(), contentTaxonomyActivity.getGooruOid());
			ResultSetFuture resultSetFuture = getAnalyticsCassSession().executeAsync(boundStatement);
			result = resultSetFuture.get();
		} catch (Exception e) {
			LOG.error("Error while retreving students class activity", e);
		}
		return result;
	}

	@Override
	public ResultSet getContentClassTaxonomyActivity(ContentTaxonomyActivity contentTaxonomyActivity) {
		ResultSet result = null;
		try {
			BoundStatement boundStatement = new BoundStatement(queries.selectContentClassTaxonomyActivity());
			boundStatement.bind(contentTaxonomyActivity.getClassUid(), contentTaxonomyActivity.getUserUid(), contentTaxonomyActivity.getResourceType(), contentTaxonomyActivity.getSubjectId(),
					contentTaxonomyActivity.getCourseId(), contentTaxonomyActivity.getDomainId(), contentTaxonomyActivity.getStandardsId(), contentTaxonomyActivity.getLearningTargetsId(),
					contentTaxonomyActivity.getGooruOid());
			ResultSetFuture resultSetFuture = getAnalyticsCassSession().executeAsync(boundStatement);
			result = resultSetFuture.get();
		} catch (Exception e) {
			LOG.error("Error while retreving students class activity", e);
		}
		return result;
	}

	@Override
	public ResultSet getContentTaxonomyActivityDataCube(String rowKey, String columnKey) {
		ResultSet result = null;
		try {
			BoundStatement boundStatement = new BoundStatement(queries.selectTaxonomyActivityDataCube());
			boundStatement.bind(rowKey, columnKey);
			ResultSetFuture resultSetFuture = getAnalyticsCassSession().executeAsync(boundStatement);
			result = resultSetFuture.get();
		} catch (Exception e) {
			LOG.error("Error while retreving students class activity", e);
		}
		return result;
	}

	@Override
	public long getContentTaxonomyActivityScore(String rowKey) {
		ResultSet result = null;
		long questionCount = 0L;
		long score = 0L;
		try {
			BoundStatement boundStatement = new BoundStatement(queries.selectTaxonomyActivityDataCube());
			boundStatement.bind(rowKey);
			ResultSetFuture resultSetFuture = getAnalyticsCassSession().executeAsync(boundStatement);
			result = resultSetFuture.get();
			if (result != null) {
				for (Row columns : result) {
					questionCount++;
					score += columns.getLong(Constants.SCORE);
				}
				score = questionCount > 0 ? (score / questionCount) : 0;
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
			boundStatement.bind(taxonomyActivityDataCube.getRowKey(), taxonomyActivityDataCube.getLeafNode(), taxonomyActivityDataCube.getViews(), taxonomyActivityDataCube.getAttempts(),
					taxonomyActivityDataCube.getResourceTimespent(), taxonomyActivityDataCube.getQuestionTimespent(), taxonomyActivityDataCube.getScore());
			getAnalyticsCassSession().executeAsync(boundStatement);
		} catch (Exception e) {
			LOG.error("Error while storing question grade", e);
			return false;
		}
		return true;
	}

	@Override
	public boolean saveQuestionGrade(String teacherId, String userId, String sessionId, String questionId, long score) {
		try {
			BoundStatement boundStatement = new BoundStatement(queries.insertUserQuestionGrade());
			boundStatement.bind(teacherId, userId, sessionId, questionId, score);
			getAnalyticsCassSession().executeAsync(boundStatement);
		} catch (Exception e) {
			LOG.error("Error while storing question grade", e);
			return false;
		}
		return true;
	}

	@Override
	public ResultSet getQuestionsGradeBySessionId(String teacherId, String userId, String sessionId) {
		ResultSet result = null;
		try {
			BoundStatement boundStatement = new BoundStatement(queries.selectUserQuestionGradeBySession());
			boundStatement.bind(teacherId, userId, sessionId);
			ResultSetFuture resultSetFuture = getAnalyticsCassSession().executeAsync(boundStatement);
			result = resultSetFuture.get();
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
			boundStatement.bind(teacherId, userId, sessionId, questionId);
			ResultSetFuture resultSetFuture = getAnalyticsCassSession().executeAsync(boundStatement);
			result = resultSetFuture.get();
		} catch (Exception e) {
			LOG.error("Exception while read questions grade by session", e);
		}
		return result;
	}

	@Override
	public boolean saveQuestionGradeInSession(String sessionId, String questionId, String collectionItemId, String status, long score) {
		try {
			BoundStatement boundStatement = new BoundStatement(queries.updateSessionScore());
			boundStatement.bind(sessionId, questionId, collectionItemId, status, score);
			getAnalyticsCassSession().executeAsync(boundStatement);
		} catch (Exception e) {
			LOG.error("Error while storing question grade in session", e);
			return false;
		}
		return true;
	}

	@Override
	public boolean insertEventsTimeline(String eventTime, String eventId) {
		try {
			BoundStatement boundStatement = new BoundStatement(queries.insertEventsTimeline());
			boundStatement.bind(eventTime, eventId);
			getEventCassSession().executeAsync(boundStatement);
		} catch (Exception e) {
			LOG.error("Error while storing events timeline", e);
			return false;
		}
		return true;
	}

	@Override
	public boolean insertEvents(String eventId, String event) {
		try {
			BoundStatement boundStatement = new BoundStatement(queries.insertEvents());
			boundStatement.bind(eventId, event);
			getEventCassSession().executeAsync(boundStatement);
		} catch (Exception e) {
			LOG.error("Error while storing events", e);
			return false;
		}
		return true;
	}

	@Override
	public boolean incrementStatisticalCounterData(String clusteringKey, String metricsName ,Object metricsValue) {
		try {
			BoundStatement boundStatement = new BoundStatement(queries.incrementStatisticalCounterData());
			boundStatement.bind(((Number)metricsValue).longValue(), clusteringKey, metricsName);
			getAnalyticsCassSession().executeAsync(boundStatement);
		} catch (Exception e) {
			LOG.error("Error while storing statistical data", e);
			return false;
		}
		return true;
	}
	@Override
	public boolean decrementStatisticalCounterData(String clusteringKey, String metricsName ,Object metricsValue) {
		try {
			BoundStatement boundStatement = new BoundStatement(queries.decrementStatisticalCounterData());
			boundStatement.bind(((Number)metricsValue).longValue(), clusteringKey, metricsName);
			getAnalyticsCassSession().executeAsync(boundStatement);
		} catch (Exception e) {
			LOG.error("Error while storing statistical data", e);
			return false;
		}
		return true;
	}
	@Override
	public boolean balanceCounterData(String clusteringKey, String metricsName, Long metricsValue) {
		try {
			BoundStatement selectBoundStatement = new BoundStatement(queries.selectStatustucalCounterData());
			selectBoundStatement.bind(clusteringKey, metricsName);
			ResultSet result = getAnalyticsCassSession().execute(selectBoundStatement);
			long existingValue = 0;
			if (result != null) {
				for (Row resultRow : result) {
					existingValue = resultRow.getLong(Constants.METRICS);
				}
			}
			long balancedMatrics = ((Number) metricsValue).longValue() - existingValue;
			BoundStatement boundStatement = new BoundStatement(queries.incrementStatisticalCounterData());
			boundStatement.bind(balancedMatrics, clusteringKey, metricsName);
			getAnalyticsCassSession().executeAsync(boundStatement);
		} catch (Exception e) {
			LOG.error("Error while balancing statistical data", e);
			return false;
		}
		return true;
	}
	@Override
	public boolean incrementUserStatisticalCounterData(String clusteringKey, String userUid, String metricsName, Object metricsValue) {
		try {
			BoundStatement boundStatement = new BoundStatement(queries.incrementUserStatisticalCounterData());
			boundStatement.bind(((Number)metricsValue).longValue(), clusteringKey, userUid, metricsName);
			getAnalyticsCassSession().executeAsync(boundStatement);
		} catch (Exception e) {
			LOG.error("Error while storing statistical data", e);
			return false;
		}
		return true;
	}
	@Override
	public boolean decrementUserStatisticalCounterData(String clusteringKey, String userUid, String metricsName, Object metricsValue) {
		try {
			BoundStatement boundStatement = new BoundStatement(queries.decrementUserStatisticalCounterData());
			boundStatement.bind(((Number)metricsValue).longValue(), clusteringKey, userUid, metricsName);
			getAnalyticsCassSession().executeAsync(boundStatement);
		} catch (Exception e) {
			LOG.error("Error while storing statistical data", e);
			return false;
		}
		return true;
	}
	@Override
	public boolean balanceUserCounterData(String clusteringKey, String userUid, String metricsName, Long metricsValue) {
		try {
			BoundStatement selectBoundStatement = new BoundStatement(queries.selectUserStatisticalCounterData());
			selectBoundStatement.bind(clusteringKey, userUid, metricsName);
			ResultSet result =  getAnalyticsCassSession().execute(selectBoundStatement);
			long existingValue = 0;
			if(result != null){
			for(Row resultRow : result){
				existingValue = resultRow.getLong(Constants.METRICS);
			}
			}
			long balancedMatrics =  ((Number)metricsValue).longValue() - existingValue;			
			BoundStatement boundStatement = new BoundStatement(queries.incrementUserStatisticalCounterData());
			boundStatement.bind(balancedMatrics, clusteringKey, userUid, metricsName);
			getAnalyticsCassSession().executeAsync(boundStatement);
		} catch (Exception e) {
			LOG.error("Error while balancing statistical data", e);
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
			ResultSetFuture resultSetFuture = getAnalyticsCassSession().executeAsync(boundStatement);
			ResultSet result = resultSetFuture.get();
			if (result != null) {
				appDO = new AppDO();
				for (Row columns : result) {
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
	
	@Override
	public ResultSet getClassMembers(String classId) {
		ResultSet result = null;
		try {
			BoundStatement boundStatement = new BoundStatement(queries.selectClassMembers());
			boundStatement.bind(classId);
			ResultSetFuture resultSetFuture = getAnalyticsCassSession().executeAsync(boundStatement);
			result = resultSetFuture.get();
		} catch (Exception e) {
			LOG.error("Exception while read questions grade by session", e);
		}
		return result;
	}
	@Override
	public void saveClassMembers(String classId,Set<String> studentId) {
		try {
			BoundStatement boundStatement = new BoundStatement(queries.saveClassMembers());
			boundStatement.bind(studentId,classId);
			ResultSetFuture resultSetFuture = getAnalyticsCassSession().executeAsync(boundStatement);
			resultSetFuture.get();
		} catch (Exception e) {
			LOG.error("Exception while save class members", e);
		}
	}
	@Override
	public void removeClassMembers(String classId,Set<String> studentId) {
		try {
			BoundStatement boundStatement = new BoundStatement(queries.removeClassMembers());
			boundStatement.bind(studentId,classId);
			ResultSetFuture resultSetFuture = getAnalyticsCassSession().executeAsync(boundStatement);
			resultSetFuture.get();
		} catch (Exception e) {
			LOG.error("Exception while save class members", e);
		}
	}
	@Override
	public void updateCollaborators(String classId,Set<String> collaborators) {
		try {
			BoundStatement boundStatement = new BoundStatement(queries.updateCollaborators());
			boundStatement.bind(classId,collaborators);
			ResultSetFuture resultSetFuture = getAnalyticsCassSession().executeAsync(boundStatement);
			resultSetFuture.get();
		} catch (Exception e) {
			LOG.error("Exception while save collaborators", e);
		}
	}
	@Override
	public void updateContentCreators(String classId,String creator) {
		try {
			BoundStatement boundStatement = new BoundStatement(queries.updateContentCreator());
			boundStatement.bind(classId,creator);
			ResultSetFuture resultSetFuture = getAnalyticsCassSession().executeAsync(boundStatement);
			resultSetFuture.get();
		} catch (Exception e) {
			LOG.error("Exception while save content creators", e);
		}
	}
	@Override
	public boolean deleteCourseUsage(StudentsClassActivity studentsClassActivity, String studentId, String collectionType) {
		try {
			for (String classId : studentsClassActivity.getClassUid().split(Constants.COMMA)) {
				BoundStatement boundStatement = new BoundStatement(queries.deleteCourseUsage());
				boundStatement.bind(classId, (studentId == null ? studentsClassActivity.getUserUid():studentId), collectionType, studentsClassActivity.getCourseUid());
				ResultSetFuture resultSetFuture = getAnalyticsCassSession().executeAsync(boundStatement);
				resultSetFuture.get();
			}
		} catch (Exception e) {
			LOG.error("Exception while delete course usage", e);
			return false;
		}
		return true;
	}
	
	@Override
	public boolean deleteUnitUsage(StudentsClassActivity studentsClassActivity, String studentId, String collectionType) {
		try {
			for (String classId : studentsClassActivity.getClassUid().split(Constants.COMMA)) {
				BoundStatement boundStatement = new BoundStatement(queries.deleteUnitUsage());
				boundStatement.bind(classId, (studentId == null ? studentsClassActivity.getUserUid():studentId), collectionType, studentsClassActivity.getCourseUid(), studentsClassActivity.getUnitUid());
				ResultSetFuture resultSetFuture = getAnalyticsCassSession().executeAsync(boundStatement);
				resultSetFuture.get();
			}
		} catch (Exception e) {
			LOG.error("Exception while delete unit usage", e);
			return false;
		}
		return true;
	}
	
	@Override
	public boolean deleteLessonUsage(StudentsClassActivity studentsClassActivity, String studentId, String collectionType) {
		try {
			for (String classId : studentsClassActivity.getClassUid().split(Constants.COMMA)) {
				BoundStatement boundStatement = new BoundStatement(queries.deleteLessonUsage());
				boundStatement.bind(classId, (studentId == null ? studentsClassActivity.getUserUid():studentId), collectionType, studentsClassActivity.getCourseUid(), studentsClassActivity.getUnitUid(),
						studentsClassActivity.getLessonUid());
				ResultSetFuture resultSetFuture = getAnalyticsCassSession().executeAsync(boundStatement);
				resultSetFuture.get();
			}
		} catch (Exception e) {
			LOG.error("Exception while delete lesson usage", e);
			return false;
		}
		return true;
	}
	
	@Override
	public boolean deleteAssessmentOrCollectionUsage(StudentsClassActivity studentsClassActivity, String studentId) {
		try {
			for (String classId : studentsClassActivity.getClassUid().split(Constants.COMMA)) {
				BoundStatement boundStatement = new BoundStatement(queries.deleteCourseUsage());
				boundStatement.bind(classId, (studentId == null ? studentsClassActivity.getUserUid():studentId), studentsClassActivity.getCollectionType(), studentsClassActivity.getCourseUid(), studentsClassActivity.getUnitUid(),
						studentsClassActivity.getLessonUid(), studentsClassActivity.getCollectionUid());
				ResultSetFuture resultSetFuture = getAnalyticsCassSession().executeAsync(boundStatement);
				resultSetFuture.get();
			}
		} catch (Exception e) {
			LOG.error("Exception while delete assessment or collection usage ", e);
			return false;
		}
		return true;
	}
	
	@Override
	public boolean deleteClassActivityDataCube(String rowKey, String collectionType, String studentId, String leafNode) {
		try {
			
				BoundStatement boundStatement = new BoundStatement(queries.deleteDataCubeByRowkeyColumn());
				boundStatement.bind(rowKey,collectionType,studentId,leafNode);
				ResultSetFuture resultSetFuture = getAnalyticsCassSession().executeAsync(boundStatement);
				resultSetFuture.get();
		} catch (Exception e) {
			LOG.error("Exception while delete lesson usage", e);
			return false;
		}
		return true;
	}
	@Override
	public boolean deleteClassActivityDataCube(String rowKey) {
		try {
			
				BoundStatement boundStatement = new BoundStatement(queries.deleteDataCubeByRowkey());
				boundStatement.bind(rowKey);
				ResultSetFuture resultSetFuture = getAnalyticsCassSession().executeAsync(boundStatement);
				resultSetFuture.get();
		} catch (Exception e) {
			LOG.error("Exception while delete lesson usage", e);
			return false;
		}
		return true;
	}
	@Override
	public boolean addStatPublisherQueue(String metricsName, String gooruOid, String type, long eventTime) {
		try {
			BoundStatement boundStatement = new BoundStatement(queries.addStatPublisherQueue());
			boundStatement.bind(metricsName, gooruOid,type,eventTime);
			getEventCassSession().executeAsync(boundStatement);
		} catch (Exception e) {
			LOG.error("Error while add stat publisher queue..", e);
			return false;
		}
		return true;
	}
@Override
	public boolean insertUserSessionTaxonomyActivity(UserSessionTaxonomyActivity userSessionTaxonomyActivity) {
		try {
			BoundStatement boundStatement = new BoundStatement(queries.insertUserSessionTaxonomyActivity());
			boundStatement.bind(userSessionTaxonomyActivity.getSessionId(), userSessionTaxonomyActivity.getGooruOid(), userSessionTaxonomyActivity.getSubjectId(),
					userSessionTaxonomyActivity.getCourseId(), userSessionTaxonomyActivity.getDomainId(), userSessionTaxonomyActivity.getStandardsId(),
					userSessionTaxonomyActivity.getLearningTargetsId(), userSessionTaxonomyActivity.getQuestionType(), userSessionTaxonomyActivity.getResourceType(),
					userSessionTaxonomyActivity.getAnswerStatus(), userSessionTaxonomyActivity.getDisplayCode(), userSessionTaxonomyActivity.getReaction(), userSessionTaxonomyActivity.getScore(), userSessionTaxonomyActivity.getTimeSpent(),
					userSessionTaxonomyActivity.getViews());
			getAnalyticsCassSession().executeAsync(boundStatement);
		} catch (Exception e) {
			LOG.error("Error while add stat publisher queue..", e);
			return false;
		}
		return true;
	}
@Override
	public ResultSet mergeUserSessionTaxonomyActivity(UserSessionTaxonomyActivity userSessionTaxonomyActivity) {
		ResultSet result = null;
		try {
			BoundStatement boundStatement = new BoundStatement(queries.selectUserSessionTaxonomyActivity());
			boundStatement.bind(userSessionTaxonomyActivity.getSessionId(), userSessionTaxonomyActivity.getGooruOid(), userSessionTaxonomyActivity.getSubjectId(),
					userSessionTaxonomyActivity.getCourseId(), userSessionTaxonomyActivity.getDomainId(), userSessionTaxonomyActivity.getStandardsId(),
					userSessionTaxonomyActivity.getLearningTargetsId());
			ResultSetFuture resultSetFuture = getAnalyticsCassSession().executeAsync(boundStatement);
			result = resultSetFuture.get();
			if (result != null) {
				for (Row columns : result) {
					userSessionTaxonomyActivity.setTimeSpent((userSessionTaxonomyActivity.getTimeSpent() + columns.getLong(Constants._TIME_SPENT)));
					userSessionTaxonomyActivity.setViews((userSessionTaxonomyActivity.getViews() + columns.getLong(Constants.VIEWS)));
				}
			}
		} catch (Exception e) {
			LOG.error("Exception while read questions grade by session", e);
		}
		return result;
	}
}
