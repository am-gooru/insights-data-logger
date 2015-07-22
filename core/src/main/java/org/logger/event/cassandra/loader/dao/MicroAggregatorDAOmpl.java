package org.logger.event.cassandra.loader.dao;

/*******************************************************************************
 * CounterDetailsDAOCassandraImpl.java
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.lang.StringUtils;
import org.ednovo.data.model.ResourceCo;
import org.ednovo.data.model.TypeConverter;
import org.ednovo.data.model.UserCo;
import org.logger.event.cassandra.loader.CassandraConnectionProvider;
import org.logger.event.cassandra.loader.ColumnFamily;
import org.logger.event.cassandra.loader.Constants;
import org.logger.event.cassandra.loader.DataUtils;
import org.logger.event.cassandra.loader.EventColumns;
import org.logger.event.cassandra.loader.LoaderConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.retry.ConstantBackoff;

public class MicroAggregatorDAOmpl extends BaseDAOCassandraImpl implements MicroAggregatorDAO, Constants {

	private static final Logger logger = LoggerFactory.getLogger(MicroAggregatorDAOmpl.class);

	private CassandraConnectionProvider connectionProvider;

	private BaseCassandraRepoImpl baseCassandraDao;

	private RawDataUpdateDAOImpl rawUpdateDAO;

	public static Map<String, Object> cache;

	ExecutorService service = Executors.newFixedThreadPool(10);

	public MicroAggregatorDAOmpl(CassandraConnectionProvider connectionProvider) {
		super(connectionProvider);
		this.connectionProvider = connectionProvider;
		this.baseCassandraDao = new BaseCassandraRepoImpl(this.connectionProvider);
		cache = new LinkedHashMap<String, Object>();
		this.rawUpdateDAO = new RawDataUpdateDAOImpl(this.connectionProvider);
	}

	/**
	 * This is the method to generate session activity and class activity details.
	 * 
	 * @param eventMap
	 */
	public void eventProcessor(Map<String, Object> eventMap) {
		try {
			String eventName = eventMap.containsKey(EVENT_NAME) ? (String) eventMap.get(EVENT_NAME) : null;
			String gooruUUID = eventMap.containsKey(GOORUID) ? (String) eventMap.get(GOORUID) : null;
			String contentGooruId = eventMap.get(CONTENT_GOORU_OID) != null ? (String) eventMap.get(CONTENT_GOORU_OID) : null;
			String lessonGooruId = eventMap.get(LESSON_GOORU_OID) != null ? (String) eventMap.get(LESSON_GOORU_OID) : null;
			String unitGooruId = eventMap.get(UNIT_GOORU_OID) != null ? (String) eventMap.get(UNIT_GOORU_OID) : null;
			String courseGooruId = eventMap.get(COURSE_GOORU_OID) != null ? (String) eventMap.get(COURSE_GOORU_OID) : null;
			String classGooruId = eventMap.get(CLASS_GOORU_OID) != null ? (String) eventMap.get(CLASS_GOORU_OID) : null;
			String parentGooruId = eventMap.get(PARENT_GOORU_OID) != null ? (String) eventMap.get(PARENT_GOORU_OID) : null;
			String sessionId = eventMap.get(SESSION_ID) != null ? (String) eventMap.get(SESSION_ID) : null;
			String eventType = eventMap.get(TYPE) != null ? (String) eventMap.get(TYPE) : null;
			Boolean isStudent = eventMap.get(IS_STUDENT) != null ? (Boolean) eventMap.get(IS_STUDENT) : false;

			List<String> keysList = new ArrayList<String>();
			/**
			 * Mutation Batch for storing session activity details
			 */

			MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));
			/**
			 * Generate column list with session id
			 */
			this.storeSessions(m, eventMap, eventName, classGooruId, courseGooruId, unitGooruId, lessonGooruId, contentGooruId, parentGooruId, gooruUUID, eventType, sessionId, isStudent);

			/**
			 * Store session activity details
			 */
			ColumnListMutation<String> aggregatorColumns = m.withRow(baseCassandraDao.accessColumnFamily(ColumnFamily.SESSION_ACTIVITY.getColumnFamily()), sessionId);
			ColumnListMutation<String> counterColumns = m.withRow(baseCassandraDao.accessColumnFamily(ColumnFamily.SESSION_ACTIVITY_COUNTER.getColumnFamily()), sessionId);
			keysList.add(sessionId);
			this.generateSessionActivity(eventMap, aggregatorColumns, counterColumns, contentGooruId, parentGooruId, eventType);

			/**
			 * If user is playing collection from class , we need to generate All students and All session details.
			 */
			aggregateAllSessions(m, eventMap, keysList, eventName, classGooruId, courseGooruId, unitGooruId, lessonGooruId, contentGooruId, parentGooruId, gooruUUID, isStudent, eventType);

			m.execute();
			/**
			 * Storing the latest collection accessed time
			 */
			this.storeLastAccessedTime(contentGooruId, ((Number) eventMap.get(END_TIME)).longValue());
			/**
			 * Aggregations steps in close events
			 */
			if (STOP.equalsIgnoreCase(eventType) || PAUSE.equalsIgnoreCase(eventType)) {
				getDataFromCounterToAggregator(keysList, ColumnFamily.SESSION_ACTIVITY_COUNTER.getColumnFamily(), ColumnFamily.SESSION_ACTIVITY.getColumnFamily());
				generateClassActivity(eventMap, eventName, classGooruId, courseGooruId, unitGooruId, lessonGooruId, contentGooruId, gooruUUID, isStudent);
			}
		} catch (Exception e) {
			logger.error("Exception:", e);
		}
	}

	/**
	 * Aggregate data for All student and All session data. Key will be start with "AS"
	 * 
	 * @param m
	 * @param eventMap
	 * @param keysList
	 * @param eventName
	 * @param classGooruId
	 * @param courseGooruId
	 * @param unitGooruId
	 * @param lessonGooruId
	 * @param contentGooruId
	 * @param parentGooruId
	 * @param gooruUUID
	 * @param isStudent
	 * @param eventType
	 */
	private void aggregateAllSessions(MutationBatch m, Map<String, Object> eventMap, List<String> keysList, String eventName, String classGooruId, String courseGooruId, String unitGooruId,
			String lessonGooruId, String contentGooruId, String parentGooruId, String gooruUUID, boolean isStudent, String eventType) {
		try {

			if (classGooruId != null && courseGooruId != null) {
				String allSessionKey = null;
				if (LoaderConstants.CPV1.getName().equals(eventName)) {
					allSessionKey = this.generateColumnKey(AS, classGooruId, courseGooruId, unitGooruId, lessonGooruId, contentGooruId);
				} else {
					allSessionKey = this.generateColumnKey(AS, classGooruId, courseGooruId, unitGooruId, lessonGooruId, parentGooruId);
				}
				ColumnListMutation<String> allSessionAggColumns = m.withRow(baseCassandraDao.accessColumnFamily(ColumnFamily.SESSION_ACTIVITY.getColumnFamily()), allSessionKey);
				ColumnListMutation<String> allSessionCounterColumns = m.withRow(baseCassandraDao.accessColumnFamily(ColumnFamily.SESSION_ACTIVITY_COUNTER.getColumnFamily()), allSessionKey);
				keysList.add(allSessionKey);
				this.generateSessionActivity(eventMap, allSessionAggColumns, allSessionCounterColumns, contentGooruId, parentGooruId, eventType);
			}
			logger.info("All session All student aggregation completed for the session id : ", eventMap.get(SESSION_ID));
		} catch (Exception e) {
			logger.error("Exception:", e);
		}
	}

	/**
	 * Calculate and store different kind of class actvity aggregataion in class_activity CF
	 * 
	 * @param eventMap
	 * @param eventName
	 * @param classGooruId
	 * @param courseGooruId
	 * @param unitGooruId
	 * @param lessonGooruId
	 * @param contentGooruId
	 * @param gooruUUID
	 * @param isStudent
	 */
	private void generateClassActivity(Map<String, Object> eventMap, String eventName, String classGooruId, String courseGooruId, String unitGooruId, String lessonGooruId, String contentGooruId,
			String gooruUUID, boolean isStudent) {
		try {
			if (LoaderConstants.CPV1.getName().equals(eventName) && eventMap.containsKey(CLASS_GOORU_OID) && eventMap.get(CLASS_GOORU_OID) != null && isStudent) {
				String collectionType = eventMap.get(COLLECTION_TYPE).equals(COLLECTION) ? COLLECTION : ASSESSMENT;
				long scoreInPercentage = ((Number) eventMap.get(SCORE_IN_PERCENTAGE)).longValue();
				MutationBatch scoreMutation = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));
				List<String> scoreKeyList = generateClassActivityKeys(classGooruId, courseGooruId, unitGooruId, lessonGooruId, gooruUUID, collectionType);
				for (String key : scoreKeyList) {
					ColumnListMutation<String> scoreAggregator = scoreMutation.withRow(baseCassandraDao.accessColumnFamily(ColumnFamily.CLASS_ACTIVITY.getColumnFamily()), key);
					ColumnListMutation<String> scoreCounter = scoreMutation.withRow(baseCassandraDao.accessColumnFamily(ColumnFamily.CLASS_ACTIVITY_COUNTER.getColumnFamily()), key);
					scoreAggregator.putColumnIfNotNull(this.generateColumnKey(contentGooruId, _SCORE_IN_PERCENTAGE), scoreInPercentage);
					scoreAggregator.putColumnIfNotNull(this.generateColumnKey(contentGooruId, _LAST_ACCESSED), ((Number) eventMap.get(END_TIME)).longValue());
					for (Map.Entry<String, Object> entry : EventColumns.SCORE_AGGREGATE_COLUMNS.entrySet()) {
						columGenerator(eventMap, entry, scoreAggregator, scoreCounter, contentGooruId);
						columGenerator(eventMap, entry, scoreAggregator, scoreCounter, null);
					}
				}
				List<String> classActivityKeys = new ArrayList<String>();
				Map<String, String> keyAndColumnsMap = generateClassActivityAggregatedKeys(classGooruId, courseGooruId, unitGooruId, lessonGooruId, contentGooruId, gooruUUID, collectionType);
				for (Map.Entry<String, String> aggregateKeys : keyAndColumnsMap.entrySet()) {
					/**
					 * aggregateKeys.getKey() is rowKey and aggregateKeys.getValue() is columnName
					 */
					if (COLLECTION.equalsIgnoreCase(collectionType)) {
						ColumnListMutation<String> scoreCounter = scoreMutation.withRow(baseCassandraDao.accessColumnFamily(ColumnFamily.CLASS_ACTIVITY_COUNTER.getColumnFamily()),
								aggregateKeys.getKey());
						scoreCounter.incrementCounterColumn(aggregateKeys.getValue(), ((Number) eventMap.get(TOTALTIMEINMS)).longValue());
					} else if (ASSESSMENT.equalsIgnoreCase(collectionType)) {
						ColumnListMutation<String> scoreAggregator = scoreMutation.withRow(baseCassandraDao.accessColumnFamily(ColumnFamily.CLASS_ACTIVITY.getColumnFamily()), aggregateKeys.getKey());
						scoreAggregator.putColumnIfNotNull(aggregateKeys.getValue(), scoreInPercentage);
					}
					classActivityKeys.add(aggregateKeys.getKey());
				}
				scoreMutation.execute();
				this.getDataFromCounterToAggregator(scoreKeyList, ColumnFamily.CLASS_ACTIVITY_COUNTER.getColumnFamily(), ColumnFamily.CLASS_ACTIVITY.getColumnFamily());
				this.getDataFromCounterToAggregator(classActivityKeys, ColumnFamily.CLASS_ACTIVITY_COUNTER.getColumnFamily(), ColumnFamily.CLASS_ACTIVITY.getColumnFamily());
				this.triggerClassActivityAggregation(classGooruId, courseGooruId, unitGooruId, lessonGooruId, gooruUUID, collectionType);
			}
			logger.info("Class Activit computations completed for the session : {}", eventMap.get(SESSION_ID));
		} catch (Exception e) {
			logger.error("Exception", e);
		}
	}

	/**
	 * Score computation keys
	 * 
	 * @param classGooruId
	 * @param courseGooruId
	 * @param unitGooruId
	 * @param lessonGooruId
	 * @param gooruUUID
	 * @param collectionType
	 * @return
	 */
	private List<String> scoreComputeKeys(String classGooruId, String courseGooruId, String unitGooruId, String lessonGooruId, String gooruUUID) {
		List<String> scoreKeys = new ArrayList<String>();
		scoreKeys.add(generateColumnKey(classGooruId, courseGooruId, gooruUUID));
		scoreKeys.add(generateColumnKey(classGooruId, courseGooruId, unitGooruId, gooruUUID));
		scoreKeys.add(generateColumnKey(classGooruId, courseGooruId, unitGooruId, lessonGooruId, gooruUUID));
		return scoreKeys;
	}

	/**
	 * Calculate Average score in any level course/unit/lesson from class activity
	 * 
	 * @param classGooruId
	 * @param courseGooruId
	 * @param unitGooruId
	 * @param lessonGooruId
	 * @param gooruUUID
	 * @param collectionType
	 */
	private void triggerClassActivityAggregation(String classGooruId, String courseGooruId, String unitGooruId, String lessonGooruId, String gooruUUID, String collectionType) {
		try {
			MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));
			List<String> scoreKeys = scoreComputeKeys(classGooruId, courseGooruId, unitGooruId, lessonGooruId, gooruUUID);
			if (collectionType.equalsIgnoreCase(ASSESSMENT)) {
				for (String scoreKey : scoreKeys) {
					aggregateClassActivityScore(scoreKey, m);
				}
			} else {
				for (String scoreKey : scoreKeys) {
					String columnName = generateColumnKey(scoreKey, collectionType, TIME_SPENT);
					long collectionsViewed = baseCassandraDao.getCount(ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), columnName);
					m.withRow(baseCassandraDao.accessColumnFamily(ColumnFamily.CLASS_ACTIVITY.getColumnFamily()), scoreKey).putColumn(_COLLECTION_UNIQUE_VIEWS, collectionsViewed);
					m.withRow(baseCassandraDao.accessColumnFamily(ColumnFamily.CLASS_ACTIVITY.getColumnFamily()), generateColumnKey(scoreKey, collectionType)).putColumn(_UNIQUE_VIEWS,
							collectionsViewed);
				}
			}
			m.execute();
			logger.info("Score computation is completed");
		} catch (Exception e) {
			logger.error("Exception", e);
		}
	}

	private void aggregateClassActivityScore(String scoreKey, MutationBatch m) {
		String columnName = generateColumnKey(scoreKey, ASSESSMENT, _SCORE_IN_PERCENTAGE);
		long assessmentAttempted = baseCassandraDao.getCount(ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), columnName);
		Long scoreInPercentage = getScoreInPercentage(columnName);
		m.withRow(baseCassandraDao.accessColumnFamily(ColumnFamily.CLASS_ACTIVITY.getColumnFamily()), scoreKey).putColumn(_SCORE_IN_PERCENTAGE, scoreInPercentage);
		m.withRow(baseCassandraDao.accessColumnFamily(ColumnFamily.CLASS_ACTIVITY.getColumnFamily()), generateColumnKey(scoreKey, ASSESSMENT)).putColumn(_SCORE_IN_PERCENTAGE, scoreInPercentage);
		m.withRow(baseCassandraDao.accessColumnFamily(ColumnFamily.CLASS_ACTIVITY.getColumnFamily()), scoreKey).putColumn(_ASSESSEMENT_UNIQUE_VIEWS, assessmentAttempted);
		m.withRow(baseCassandraDao.accessColumnFamily(ColumnFamily.CLASS_ACTIVITY.getColumnFamily()), generateColumnKey(scoreKey, ASSESSMENT)).putColumn(_UNIQUE_VIEWS, assessmentAttempted);
	}

	/**
	 * Calculate Total score in any level course/unit/lesson from class activity
	 * 
	 * @param key
	 * @return
	 */
	private Long getScoreInPercentage(String key) {
		long score = 0L;
		long attemptedAssessmentCount = 0L;
		long scoreInPercentage = 0L;
		ColumnList<String> scoreList = baseCassandraDao.readWithKey(ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), key, 0);
		for (Column<String> scoreColumn : scoreList) {
			++attemptedAssessmentCount;
			score += scoreColumn.getLongValue();
		}
		if (attemptedAssessmentCount != 0L) {
			scoreInPercentage = (score / attemptedAssessmentCount);
		}
		return scoreInPercentage;
	}

	/**
	 * Calculate Assessment total score from individual question level from session activity
	 * 
	 * @param key
	 * @return
	 */
	private Long getAssessmentTotalScore(String key) {
		long score = 0L;
		ColumnList<String> scoreList = baseCassandraDao.readWithKey(ColumnFamily.SESSION_ACTIVITY.getColumnFamily(), key, 0);
		for (Column<String> scoreColumn : scoreList) {
			if (scoreColumn.getName().contains(_QUESTION_STATUS) && scoreColumn.getStringValue().equalsIgnoreCase(LoaderConstants.CORRECT.getName())) {
				/**
				 * Here based on question type back end can set any score here in future.Today default is 1 for all type of question
				 */
				++score;
			}
		}
		return score;
	}

	/**
	 * Generate different kind of keys in user level to store session_activity data
	 * 
	 * @param classGooruId
	 * @param courseGooruId
	 * @param unitGooruId
	 * @param lessonGooruId
	 * @param gooruUUID
	 * @param collectionType
	 * @return
	 */
	private List<String> generateClassActivityKeys(String classGooruId, String courseGooruId, String unitGooruId, String lessonGooruId, String gooruUUID, String collectionType) {
		List<String> scoreKeyList = new ArrayList<String>();
		scoreKeyList.add(generateColumnKey(classGooruId, courseGooruId, gooruUUID));
		scoreKeyList.add(generateColumnKey(classGooruId, courseGooruId, unitGooruId, gooruUUID));
		scoreKeyList.add(generateColumnKey(classGooruId, courseGooruId, unitGooruId, lessonGooruId, gooruUUID));
		scoreKeyList.add(generateColumnKey(classGooruId, courseGooruId, gooruUUID, collectionType));
		scoreKeyList.add(generateColumnKey(classGooruId, courseGooruId, unitGooruId, gooruUUID, collectionType));
		scoreKeyList.add(generateColumnKey(classGooruId, courseGooruId, unitGooruId, lessonGooruId, gooruUUID, collectionType));
		return scoreKeyList;
	}

	/**
	 * Generate keys to store store timespent for collection and recent score for assessment
	 * 
	 * @param classGooruId
	 * @param courseGooruId
	 * @param unitGooruId
	 * @param lessonGooruId
	 * @param gooruUUID
	 * @param collectionType
	 * @return
	 */
	private Map<String, String> generateClassActivityAggregatedKeys(String classGooruId, String courseGooruId, String unitGooruId, String lessonGooruId, String contentGooruId, String gooruUUID,
			String collectionType) {
		Map<String, String> keyAndColumnsMap = new HashMap<String, String>();
		String suffix = TIME_SPENT;
		if (ASSESSMENT.equalsIgnoreCase(collectionType)) {
			suffix = _SCORE_IN_PERCENTAGE;
		}
		keyAndColumnsMap.put(generateColumnKey(classGooruId, courseGooruId, gooruUUID, collectionType, suffix), generateColumnKey(unitGooruId, lessonGooruId, contentGooruId));
		keyAndColumnsMap.put(generateColumnKey(classGooruId, courseGooruId, unitGooruId, gooruUUID, collectionType, suffix), generateColumnKey(lessonGooruId, contentGooruId));
		keyAndColumnsMap.put(generateColumnKey(classGooruId, courseGooruId, unitGooruId, lessonGooruId, gooruUUID, collectionType, suffix), contentGooruId);
		return keyAndColumnsMap;
	}

	/**
	 * Prepare column list to store data in Cassandra as a Batch
	 * 
	 * @param eventMap
	 * @param entry
	 * @param aggregatorColumns
	 * @param counterColumns
	 * @param columnPrefix
	 */
	private void columGenerator(Map<String, Object> eventMap, Map.Entry<String, Object> entry, ColumnListMutation<String> aggregatorColumns, ColumnListMutation<String> counterColumns,
			String columnPrefix) {
		if (eventMap.containsKey(entry.getValue())) {
			if (eventMap.get(entry.getValue()) instanceof Number) {
				aggregatorColumns.putColumnIfNotNull(this.generateColumnKey(columnPrefix, entry.getKey()), ((Number) eventMap.get(entry.getValue())).longValue());
				counterColumns.incrementCounterColumn(this.generateColumnKey(columnPrefix, entry.getKey()), ((Number) eventMap.get(entry.getValue())).longValue());
			} else {
				aggregatorColumns.putColumnIfNotNull(this.generateColumnKey(columnPrefix, entry.getKey()), (String) eventMap.get(entry.getValue()));
			}
		}
	}

	/**
	 * Process player events and store raw in session_activity CF
	 * 
	 * @param eventMap
	 * @param aggregatorColumns
	 * @param counterColumns
	 * @param contentGooruId
	 * @param parentGooruId
	 * @param eventType
	 */
	private void generateSessionActivity(Map<String, Object> eventMap, ColumnListMutation<String> aggregatorColumns, ColumnListMutation<String> counterColumns, String contentGooruId,
			String parentGooruId, String eventType) {
		try {
			String sessionId = (String) eventMap.get(SESSION_ID);
			if (LoaderConstants.CPV1.getName().equals(eventMap.get(EVENT_NAME))) {
				for (Map.Entry<String, Object> entry : EventColumns.COLLECTION_PLAY_COLUMNS.entrySet()) {
					columGenerator(eventMap, entry, aggregatorColumns, counterColumns, contentGooruId);
				}
				if ((STOP.equalsIgnoreCase(eventType) || PAUSE.equalsIgnoreCase(eventType)) && !ASSESSMENT_URL.equals(eventMap.get(COLLECTION_TYPE))) {
					Long scoreInPercentage = 0L;
					Long score = 0L;
					if (eventMap.containsKey(TOTAL_QUESTIONS_COUNT)) {
						Long questionCount = ((Number) eventMap.get(TOTAL_QUESTIONS_COUNT)).longValue();
						aggregatorColumns.putColumnIfNotNull(this.generateColumnKey(contentGooruId, _QUESTION_COUNT), questionCount);
						logger.info("Question Count : {}", questionCount);
						if (questionCount > 0) {
							score = getAssessmentTotalScore(sessionId);
							scoreInPercentage = (100 * score / questionCount);
						}
						eventMap.put(SCORE_IN_PERCENTAGE, scoreInPercentage);
						eventMap.put(SCORE, score);
						logger.info("Score In percentage :{} in session : {}", scoreInPercentage, score);
					}
				}
				aggregatorColumns.putColumnIfNotNull(_GOORU_UID, (String) eventMap.get(GOORUID));
				if (eventMap.containsKey(SCORE)) {
					aggregatorColumns.putColumnIfNotNull(this.generateColumnKey(contentGooruId, SCORE), ((Number) eventMap.get(SCORE)).longValue());
				}
				if (eventMap.containsKey(SCORE_IN_PERCENTAGE)) {
					aggregatorColumns.putColumnIfNotNull(this.generateColumnKey(contentGooruId, _SCORE_IN_PERCENTAGE), ((Number) eventMap.get(SCORE_IN_PERCENTAGE)).longValue());
				}
			} else if (LoaderConstants.CRPV1.getName().equals(eventMap.get(EVENT_NAME))) {
				for (Map.Entry<String, Object> entry : EventColumns.COLLECTION_RESOURCE_PLAY_COLUMNS.entrySet()) {
					columGenerator(eventMap, entry, aggregatorColumns, counterColumns, contentGooruId);
				}
				if (OE.equals(eventMap.get(QUESTION_TYPE))) {
					aggregatorColumns.putColumnIfNotNull(this.generateColumnKey(contentGooruId, ACTIVE), "false");
				}
				if (QUESTION.equals(eventMap.get(RESOURCE_TYPE)) && (STOP.equals(eventMap.get(TYPE)) || PAUSE.equals(eventMap.get(TYPE)))) {
					String answerStatus = null;
					int[] attemptTrySequence = TypeConverter.stringToIntArray((String) eventMap.get(ATTMPT_TRY_SEQ));
					int[] attempStatus = TypeConverter.stringToIntArray((String) eventMap.get(ATTMPT_STATUS));

					int status = 0;
					status = ((Number) eventMap.get(ATTEMPT_COUNT)).intValue();
					if (status != 0) {
						status = status - 1;
					}
					if (attempStatus.length == 0) {
						answerStatus = LoaderConstants.SKIPPED.getName();
					} else if (attempStatus[status] == 0) {
						answerStatus = LoaderConstants.INCORRECT.getName();
					} else if (attempStatus[status] == 1) {
						answerStatus = LoaderConstants.CORRECT.getName();
					}
					logger.info("answerStatus : " + answerStatus);
					String option = DataUtils.makeCombinedAnswerSeq(attemptTrySequence.length == 0 ? 0 : attemptTrySequence[status]);
					counterColumns.incrementCounterColumn(this.generateColumnKey(contentGooruId, option), 1L);
					counterColumns.incrementCounterColumn(this.generateColumnKey(contentGooruId, answerStatus), 1L);
					aggregatorColumns.putColumnIfNotNull(this.generateColumnKey(contentGooruId, OPTIONS), option);
					aggregatorColumns.putColumnIfNotNull(this.generateColumnKey(contentGooruId, SCORE), ((Number) eventMap.get(SCORE)).longValue());
					if (!(answerStatus.equalsIgnoreCase(LoaderConstants.SKIPPED.getName()) && hasUserAlreadyAnswered(sessionId, contentGooruId))) {
						aggregatorColumns.putColumnIfNotNull(this.generateColumnKey(contentGooruId, _QUESTION_STATUS), answerStatus);
						aggregatorColumns.putColumnIfNotNull(this.generateColumnKey(contentGooruId, CHOICE), eventMap.containsKey(TEXT) ? (String)eventMap.get(TEXT) :null);
						aggregatorColumns.putColumnIfNotNull(this.generateColumnKey(contentGooruId, _ANSWER_OBECT), eventMap.containsKey(ANSWER_OBECT) ? (String)eventMap.get(ANSWER_OBECT) :null);
					}
				}
			} else if (LoaderConstants.CRAV1.getName().equals(eventMap.get(EVENT_NAME))) {

				long reaction = DataUtils.formatReactionString((String) eventMap.get(REACTION_TYPE));
				/**
				 * Resource Reaction
				 */
				aggregatorColumns.putColumnIfNotNull(this.generateColumnKey(contentGooruId, REACTION), reaction);
				/**
				 * Collection Reaction
				 */
				aggregatorColumns.putColumnIfNotNull(this.generateColumnKey(parentGooruId, TOTAL_REACTION), reaction);
				aggregatorColumns.putColumnIfNotNull(this.generateColumnKey(parentGooruId, REACTED_COUNT), 1L);

			} else if (LoaderConstants.RUFB.getName().equals(eventMap.get(EVENT_NAME))) {
				for (Map.Entry<String, Object> entry : EventColumns.USER_FEEDBACK_COLUMNS.entrySet()) {
					columGenerator(eventMap, entry, aggregatorColumns, counterColumns, contentGooruId);
				}
			}
			logger.info("Session Activity columns generated for the session : {}", eventMap.get(SESSION_ID));
		} catch (Exception e) {
			logger.error("Exception:", e);
		}
	}

	/**
	 * Store all the session ids is session table
	 * 
	 * @param m
	 * @param eventMap
	 * @param eventName
	 * @param classGooruId
	 * @param courseGooruId
	 * @param unitGooruId
	 * @param lessonGooruId
	 * @param contentGooruId
	 * @param parentGooruId
	 * @param gooruUUID
	 * @param eventType
	 * @param sessionId
	 * @param isStudent
	 */
	private void storeSessions(MutationBatch m, Map<String, Object> eventMap, String eventName, String classGooruId, String courseGooruId, String unitGooruId, String lessonGooruId,
			String contentGooruId, String parentGooruId, String gooruUUID, String eventType, String sessionId, Boolean isStudent) {
		try {
			String key = null;
			if (LoaderConstants.CPV1.getName().equals(eventMap.get(EVENT_NAME))) {
				if (classGooruId != null && isStudent) {
					key = generateColumnKey(classGooruId, courseGooruId, unitGooruId, lessonGooruId, contentGooruId, gooruUUID);
				} else {
					key = generateColumnKey(contentGooruId, gooruUUID);
				}
				Long eventTime = ((Number) eventMap.get(END_TIME)).longValue();
				m.withRow(baseCassandraDao.accessColumnFamily(ColumnFamily.SESSIONS.getColumnFamily()), generateColumnKey(key, INFO))
						.putColumnIfNotNull(generateColumnKey(sessionId, _SESSION_ID), sessionId).putColumnIfNotNull(generateColumnKey(sessionId, TYPE), eventType)
						.putColumnIfNotNull(generateColumnKey(sessionId, _EVENT_TIME), eventTime);
				m.withRow(baseCassandraDao.accessColumnFamily(ColumnFamily.SESSIONS.getColumnFamily()), generateColumnKey(RS, key)).putColumnIfNotNull(_SESSION_ID, sessionId);
				m.withRow(baseCassandraDao.accessColumnFamily(ColumnFamily.SESSIONS.getColumnFamily()), key).putColumnIfNotNull(sessionId, eventTime);

			} else if (LoaderConstants.CRPV1.getName().equals(eventMap.get(EVENT_NAME))) {
				if (classGooruId != null && isStudent) {
					key = generateColumnKey(classGooruId, courseGooruId, unitGooruId, lessonGooruId, parentGooruId, gooruUUID);
				} else {
					key = generateColumnKey(parentGooruId, gooruUUID);
				}
				m.withRow(baseCassandraDao.accessColumnFamily(ColumnFamily.SESSIONS.getColumnFamily()), generateColumnKey(key, INFO)).putColumnIfNotNull(
						generateColumnKey(sessionId, _LAST_ACCESSED_RESOURCE), contentGooruId);
			}
			logger.info("Session storage columns generated for session id :{}", sessionId);
		} catch (Exception e) {
			logger.error("Exception : ", e);
		}
	}

	/**
	 * Read All data from Counter CF to normal CF
	 * 
	 * @param keysList
	 * @param sourceColumnFamily
	 * @param targetColumFamily
	 */
	private void getDataFromCounterToAggregator(List<String> keysList, String sourceColumnFamily, String targetColumFamily) {
		try {
			MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));
			for (String key : keysList) {
				ColumnList<String> counterData = baseCassandraDao.readWithKey(sourceColumnFamily, key, 0);
				ColumnListMutation<String> aggregatorColumns = m.withRow(baseCassandraDao.accessColumnFamily(targetColumFamily), key);
				for (Column<String> column : counterData) {
					aggregatorColumns.putColumnIfNotNull(column.getName(), column.getLongValue());
				}
			}
			m.execute();
			logger.info("Regular CFs updated from Counter from the session");
		} catch (Exception e) {
			logger.error("Exception:", e);
		}
	}

	/**
	 * This method says what is the last accessed time of collection or assessment
	 * 
	 * @param gooruOid
	 * @param timestamp
	 */
	private void storeLastAccessedTime(String gooruOid, long timestamp) {
		try {
			MutationBatch resourceMutation = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
			baseCassandraDao.generateNonCounter(ColumnFamily.RESOURCE.getColumnFamily(), gooruOid, LAST_ACCESSED, timestamp, resourceMutation);
			resourceMutation.executeAsync();
		} catch (Exception e) {
			logger.error("Exception:", e);
		}
	}

	/**
	 * 
	 * @param eventMap
	 */
	public void updateRawData(Map<String, Object> eventMap) {
		logger.info("Into Raw Data Update");

		Map<String, Object> eventDataMap = new HashMap<String, Object>();
		List<Map<String, Object>> eventDataMapList = new ArrayList<Map<String, Object>>();
		if ((eventMap.containsKey(DATA))) {
			ObjectMapper mapper = new ObjectMapper();
			try {
				try {
					eventDataMap = mapper.readValue(eventMap.get(DATA).toString(), new TypeReference<HashMap<String, Object>>() {
					});
				} catch (Exception e) {
					eventDataMapList = mapper.readValue(eventMap.get(DATA).toString(), new TypeReference<ArrayList<Map<String, Object>>>() {
					});
				}
			} catch (Exception e1) {
				logger.error("Unable to parse data object inside payloadObject ", e1);
			}
		}

		if ((!eventDataMap.isEmpty() || !eventDataMapList.isEmpty()) && eventMap.containsKey(EVENT_NAME) && StringUtils.isNotBlank(eventMap.get(EVENT_NAME).toString())) {
			if (eventMap.get(EVENT_NAME).toString().equalsIgnoreCase(LoaderConstants.ITEM_DOT_CREATE.getName()) && eventMap.containsKey(ITEM_TYPE)) {
				if (!eventDataMap.isEmpty()) {
					if ((eventMap.get(ITEM_TYPE).toString().matches(ITEMTYPES_SCSFFC)) && eventMap.containsKey(MODE)) {
						/** Collection/Folder Create **/
						if (eventMap.get(MODE).toString().equalsIgnoreCase(CREATE)
								&& (eventDataMap.containsKey(RESOURCE_TYPE) && ((((Map<String, String>) eventDataMap.get(RESOURCE_TYPE)).get(NAME).toString().matches(RESOURCETYPES_SF))))) {
							this.createCollection(eventDataMap, eventMap);
						} else if (eventMap.get(MODE).toString().equalsIgnoreCase(MOVE)) {
							this.moveCollection(eventDataMap, eventMap);
						} else if (eventMap.get(MODE).toString().equalsIgnoreCase(COPY)) {
							this.copyCollection(eventDataMap, eventMap);
						}

					} else if ((eventMap.get(ITEM_TYPE).toString().equalsIgnoreCase(CLASSPAGE))
							&& (eventDataMap.containsKey(RESOURCE_TYPE) && ((Map<String, String>) eventDataMap.get(RESOURCE_TYPE)).get(NAME).toString().equalsIgnoreCase(CLASSPAGE))) {
						this.createClasspage(eventDataMap, eventMap);

					} else if (((eventMap.get(ITEM_TYPE).toString().equalsIgnoreCase(LoaderConstants.COLLECTION_DOT_RESOURCE.getName())))
							&& ((Map<String, Object>) eventDataMap.get(RESOURCE)).containsKey(RESOURCE_TYPE)
							&& (!(((Map<String, Map<String, Object>>) eventDataMap.get(RESOURCE)).get(RESOURCE_TYPE).get(NAME).toString().equalsIgnoreCase(SCOLLECTION)) && !((Map<String, Map<String, Object>>) eventDataMap
									.get(RESOURCE)).get(RESOURCE_TYPE).get(NAME).toString().equalsIgnoreCase(CLASSPAGE))) {
						this.createResource(eventDataMap, eventMap);
					}
				} else if ((eventMap.get(ITEM_TYPE).toString().equalsIgnoreCase(LoaderConstants.CLASSPAGE_DOT_COLLECTION.getName())) && eventDataMapList.size() > 0) {
					this.createClasspageAssignment(eventDataMapList, eventMap);
				}
			} else if (eventMap.get(EVENT_NAME).toString().equalsIgnoreCase(LoaderConstants.ITEM_DOT_EDIT.getName()) && (eventMap.containsKey(ITEM_TYPE)) && !eventDataMap.isEmpty()) {
				if (eventMap.get(ITEM_TYPE).toString().equalsIgnoreCase(LoaderConstants.CLASSPAGE_DOT_COLLECTION.getName())
						&& (eventDataMap.containsKey(RESOURCE) && ((Map<String, Map<String, Object>>) eventDataMap.get(RESOURCE)).containsKey(RESOURCE_TYPE) && (((Map<String, Map<String, Object>>) eventDataMap
								.get(RESOURCE)).get(RESOURCE_TYPE).get(NAME).toString().equalsIgnoreCase(LoaderConstants.SCOLLECTION.getName())))) {

					this.updateClasspageAssignment(eventDataMap, eventMap);

				} else if (((eventMap.get(ITEM_TYPE).toString().equalsIgnoreCase(LoaderConstants.SHELF_DOT_COLLECTION.getName())) || eventMap.get(ITEM_TYPE).toString().equalsIgnoreCase(CLASSPAGE))
						&& (eventMap.containsKey(DATA))) {

					this.updateShelfCollectionOrClasspage(eventDataMap, eventMap);

				} else if (eventMap.get(ITEM_TYPE).toString().equalsIgnoreCase(LoaderConstants.COLLECTION_DOT_RESOURCE.getName())
						&& ((Map<String, Object>) eventDataMap.get(RESOURCE)).containsKey(RESOURCE_TYPE)
						&& (!(((Map<String, Map<String, Object>>) eventDataMap.get(RESOURCE)).get(RESOURCE_TYPE).get(NAME).toString().equalsIgnoreCase(SCOLLECTION)) && !((Map<String, Map<String, Object>>) eventDataMap
								.get(RESOURCE)).get(RESOURCE_TYPE).get(NAME).toString().equalsIgnoreCase(CLASSPAGE))) {

					this.updateResource(eventDataMap, eventMap);

				}
			} else if (!eventDataMapList.isEmpty() && eventMap.get(EVENT_NAME).toString().equalsIgnoreCase(LoaderConstants.CLUAV1.getName())) {

				this.addClasspageUser(eventDataMapList, eventMap);

			} else if (!eventDataMap.isEmpty() && eventMap.get(EVENT_NAME).toString().equalsIgnoreCase(LoaderConstants.CLP_USER_REMOVE.getName())) {

				this.markDeletedClasspageUser(eventDataMap, eventMap);

			} else if (!eventDataMap.isEmpty() && eventMap.get(EVENT_NAME).toString().equalsIgnoreCase(LoaderConstants.ITEM_DOT_DELETE.getName())) {

				this.markItemDelete(eventMap);

			} else if (!eventDataMap.isEmpty() && eventMap.get(EVENT_NAME).toString().equalsIgnoreCase(LoaderConstants.REGISTER_DOT_USER.getName())) {

				this.updateRegisteredUser(eventDataMap, eventMap);

			} else if (!eventDataMap.isEmpty() && eventMap.get(EVENT_NAME).toString().equalsIgnoreCase(LoaderConstants.PROFILE_DOT_ACTION.getName())
					&& eventMap.get(ACTION_TYPE).toString().equalsIgnoreCase(EDIT)) {

				this.updateUserProfileData(eventDataMap, eventMap);

			}
		}
	}

	/**
	 * 
	 * @param eventDataMap
	 * @param eventMap
	 */
	private void createCollection(Map<String, Object> eventDataMap, Map<String, Object> eventMap) {
		ResourceCo collection = new ResourceCo();
		Map<String, Object> collectionMap = new HashMap<String, Object>();
		/**
		 * Update Resource CF
		 */

		updateResource(eventDataMap, eventMap, collection, collectionMap);
		/**
		 * Update insights collection CF for collection mapping
		 */
		rawUpdateDAO.updateCollectionTable(eventDataMap, collectionMap);

		/**
		 * Update Insights colectionItem CF for shelf-collection/folder-collection/shelf-folder mapping
		 **/
		Set<Entry<String, String>> entrySet = DataUtils.collectionItemKeys.entrySet();
		Map<String, Object> collectionItemMap = new HashMap<String, Object>();
		for (Entry<String, String> entry : entrySet) {
			if (entry.getKey().equalsIgnoreCase(ITEM_TYPE)) {
				collectionItemMap.put(entry.getValue(), (eventDataMap.containsKey(entry.getKey()) && eventDataMap.get(entry.getKey()) != null) ? eventDataMap.get(entry.getKey()).toString() : null);
			} else if (entry.getKey().equalsIgnoreCase(ITEM_ID) || entry.getKey().equalsIgnoreCase(COLLECTION_ITEM_ID)) {
				collectionItemMap.put(COLLECTION_ITEM_ID, (eventMap.containsKey(ITEM_ID) && eventMap.get(ITEM_ID) != null) ? eventMap.get(ITEM_ID).toString() : null);
			} else {
				collectionItemMap.put(entry.getValue(), (eventMap.containsKey(entry.getKey()) && eventMap.get(entry.getKey()) != null) ? eventMap.get(entry.getKey()).toString() : null);
			}
		}
		collectionItemMap.put(DELETED, Integer.valueOf(0));
		rawUpdateDAO.updateCollectionItemTable(eventMap, collectionItemMap);
	}

	/**
	 * 
	 * @param eventDataMap
	 * @param eventMap
	 */
	private void moveCollection(Map<String, Object> eventDataMap, Map<String, Object> eventMap) {
		/**
		 * Here insights colectionItem CF gets updated for folder-collection mapping
		 */

		Set<Entry<String, String>> entrySet = DataUtils.collectionItemKeys.entrySet();
		Map<String, Object> collectionItemMap = new HashMap<String, Object>();
		for (Entry<String, String> entry : entrySet) {
			if (entry.getKey().matches(COLLECTION_ITEM_FIELDS) || entry.getKey().equalsIgnoreCase(ASSOCIATION_DATE)) {
				collectionItemMap.put(entry.getValue(), (eventDataMap.containsKey(entry.getKey()) && eventDataMap.get(entry.getKey()) != null) ? eventDataMap.get(entry.getKey()).toString() : null);
			} else if (entry.getKey().equalsIgnoreCase(ITEM_SEQUENCE)) {
				collectionItemMap.put(entry.getValue(), (eventMap.containsKey(entry.getKey()) && eventMap.get(entry.getKey()) != null) ? Integer.valueOf(eventMap.get(entry.getKey()).toString())
						: null);
			} else {
				collectionItemMap.put(entry.getValue(), ((eventMap.containsKey(entry.getKey()) && eventMap.get(entry.getKey()) != null) ? eventMap.get(entry.getKey()).toString() : null));
			}
		}
		collectionItemMap.put(DELETED, Integer.valueOf(0));
		rawUpdateDAO.updateCollectionItemTable(eventMap, collectionItemMap);

		Map<String, Object> markSourceItemMap = new HashMap<String, Object>();
		markSourceItemMap.put(COLLECTION_ITEM_ID, ((eventMap.containsKey(SOURCE_ITEM_ID) && eventMap.get(SOURCE_ITEM_ID) != null) ? eventMap.get(SOURCE_ITEM_ID).toString() : null));
		markSourceItemMap.put(DELETED, Integer.valueOf(1));
		rawUpdateDAO.updateCollectionItemTable(eventMap, markSourceItemMap);

	}

	/**
	 * 
	 * @param eventDataMap
	 * @param eventMap
	 */
	private void copyCollection(Map<String, Object> eventDataMap, Map<String, Object> eventMap) {

		/** Collection Copy **/
		Map<String, Object> collectionMap = new HashMap<String, Object>();
		ResourceCo collection = new ResourceCo();

		/**
		 * Update Resource CF
		 */
		updateResource(eventDataMap, eventMap, collection, collectionMap);

		/**
		 * Here insights collection CF gets updated for collection mapping
		 */
		rawUpdateDAO.updateCollectionTable(eventDataMap, collectionMap);

		/**
		 * Here Insights colectionItem CF gets updated for shelf-collection mapping
		 */
		String collectionGooruOid = eventDataMap.containsKey(GOORU_OID) ? eventDataMap.get(GOORU_OID).toString() : null;
		if (collectionGooruOid != null) {
			List<Map<String, Object>> collectionItemList = (List<Map<String, Object>>) eventDataMap.get("collectionItems");
			for (Map<String, Object> collectionItem : collectionItemList) {
				Map<String, Object> collectionItemMap = new HashMap<String, Object>();
				Map<String, Object> collectionItemResourceMap = (Map<String, Object>) collectionItem.get("resource");
				Set<Entry<String, String>> entrySet = DataUtils.collectionItemKeys.entrySet();
				for (Entry<String, String> entry : entrySet) {
					if (entry.getKey().matches(COLLECTION_ITEM_FIELDS) || entry.getKey().equalsIgnoreCase(ASSOCIATION_DATE)) {
						collectionItemMap.put(entry.getValue(), (collectionItem.containsKey(entry.getKey()) && collectionItem.get(entry.getKey()) != null) ? collectionItem.get(entry.getKey())
								.toString() : null);
					} else if (entry.getKey().equalsIgnoreCase("questionType")) {
						collectionItemMap.put(entry.getValue(),
								((collectionItemResourceMap.containsKey(entry.getKey()) && collectionItemResourceMap.get(entry.getKey()) != null) ? collectionItemResourceMap.get(entry.getKey())
										.toString() : null));
					} else if (entry.getKey().equalsIgnoreCase(CONTENT_GOORU_OID)) {
						collectionItemMap.put(entry.getValue(), (collectionItemResourceMap.containsKey(GOORU_OID) ? collectionItemResourceMap.get(GOORU_OID).toString() : null));
					} else if (entry.getKey().equalsIgnoreCase(PARENT_GOORU_OID)) {
						collectionItemMap.put(entry.getValue(), (eventMap.containsKey(CONTENT_GOORU_OID) ? eventMap.get(CONTENT_GOORU_OID).toString() : null));
					} else {
						collectionItemMap.put(entry.getValue(), (eventMap.containsKey(entry.getKey()) && eventMap.get(entry.getKey()) != null) ? eventMap.get(entry.getKey()).toString() : null);
					}
				}
				collectionItemMap.put(DELETED, Integer.valueOf(0));
				rawUpdateDAO.updateCollectionItemTable(collectionItem, collectionItemMap);
			}
		}
	}

	/**
	 * 
	 * @param eventDataMap
	 * @param eventMap
	 */
	private void createClasspage(Map<String, Object> eventDataMap, Map<String, Object> eventMap) {

		/** classpage create **/

		ResourceCo collection = new ResourceCo();
		Map<String, Object> collectionMap = new HashMap<String, Object>();
		/**
		 * Update Resource CF
		 */
		updateResource(eventDataMap, eventMap, collection, collectionMap);

		Map<String, Object> classpageMap = new HashMap<String, Object>();
		classpageMap.put(CLASS_CODE, ((eventMap.containsKey(CLASS_CODE) && eventMap.get(CLASS_CODE) != null) ? eventMap.get(CLASS_CODE).toString() : null));
		classpageMap.put("groupUId", ((eventMap.containsKey("groupUId") && eventMap.get("groupUId") != null) ? eventMap.get("groupUId").toString() : null));
		classpageMap.put(
				CONTENT_ID,
				((eventMap.containsKey(CONTENT_ID) && eventMap.get(CONTENT_ID) != null && !StringUtils.isBlank(eventMap.get(CONTENT_ID).toString())) ? Long
						.valueOf(eventMap.get(CONTENT_ID).toString()) : null));
		classpageMap.put(ORGANIZATION_UID, ((eventMap.containsKey(ORGANIZATION_UID) && eventMap.get(ORGANIZATION_UID) != null) ? eventMap.get(ORGANIZATION_UID).toString() : null));
		classpageMap.put(USER_UID, ((eventMap.containsKey(GOORUID) && eventMap.get(GOORUID) != null) ? eventMap.get(GOORUID).toString() : null));
		classpageMap.put("isGroupOwner", 1);
		classpageMap.put(DELETED, Integer.valueOf(0));
		classpageMap.put("activeFlag", 1);
		classpageMap.put("userGroupType", SYSTEM);
		rawUpdateDAO.updateClasspage(eventDataMap, classpageMap);

		/** Update insights collection CF for collection mapping **/
		rawUpdateDAO.updateCollectionTable(eventDataMap, collectionMap);

		/** Update Insights colectionItem CF for shelf-collection mapping **/
		Set<Entry<String, String>> entrySet = DataUtils.collectionItemKeys.entrySet();
		Map<String, Object> collectionItemMap = new HashMap<String, Object>();
		for (Entry<String, String> entry : entrySet) {
			if (entry.getKey().equalsIgnoreCase(ITEM_TYPE)) {
				collectionItemMap.put(entry.getValue(), (eventDataMap.containsKey(entry.getKey()) && eventDataMap.get(entry.getKey()) != null) ? eventDataMap.get(entry.getKey()).toString() : null);
			} else if (entry.getKey().equalsIgnoreCase(COLLECTION_ITEM_ID)) {
				collectionItemMap.put(COLLECTION_ITEM_ID, (eventMap.containsKey(ITEM_ID) && eventMap.get(ITEM_ID) != null) ? eventMap.get(ITEM_ID).toString() : null);
			} else {
				collectionItemMap.put(entry.getValue(), (eventMap.containsKey(entry.getKey()) && eventMap.get(entry.getKey()) != null) ? eventMap.get(entry.getKey()).toString() : null);
			}
		}
		collectionItemMap.put(DELETED, Integer.valueOf(0));
		rawUpdateDAO.updateCollectionItemTable(eventMap, collectionItemMap);
	}

	/**
	 * 
	 * @param dataMapList
	 * @param eventMap
	 */
	private void addClasspageUser(List<Map<String, Object>> dataMapList, Map<String, Object> eventMap) {
		/** classpage user add **/
		for (Map<String, Object> dataMap : dataMapList) {
			Map<String, Object> classpageMap = new HashMap<String, Object>();
			classpageMap.put("classId", ((eventMap.containsKey(CONTENT_GOORU_OID) && eventMap.get(CONTENT_GOORU_OID) != null) ? eventMap.get(CONTENT_GOORU_OID).toString() : null));
			classpageMap.put(CLASS_CODE, ((eventMap.containsKey(CLASS_CODE) && eventMap.get(CLASS_CODE) != null) ? eventMap.get(CLASS_CODE).toString() : null));
			classpageMap.put("groupUId", ((eventMap.containsKey("groupUId") && eventMap.get("groupUId") != null) ? eventMap.get("groupUId").toString() : null));
			classpageMap.put(
					CONTENT_ID,
					((eventMap.containsKey(CONTENT_ID) && eventMap.get(CONTENT_ID) != null && !StringUtils.isBlank(eventMap.get(CONTENT_ID).toString())) ? Long.valueOf(eventMap.get(CONTENT_ID)
							.toString()) : null));
			classpageMap.put(ORGANIZATION_UID, ((eventMap.containsKey(ORGANIZATION_UID) && eventMap.get(ORGANIZATION_UID) != null) ? eventMap.get(ORGANIZATION_UID).toString() : null));
			classpageMap.put("isGroupOwner", Integer.valueOf(0));
			classpageMap.put(DELETED, Integer.valueOf(0));
			classpageMap.put("activeFlag", (dataMap.containsKey("status") && dataMap.get("status") != null && dataMap.get("status").toString().equalsIgnoreCase("active")) ? Integer.valueOf(1)
					: Integer.valueOf(0));
			classpageMap.put("username", ((dataMap.containsKey("username") && dataMap.get("username") != null) ? dataMap.get("username") : null));
			classpageMap.put(USER_UID, ((dataMap.containsKey("gooruUid") && dataMap.get("gooruUid") != null) ? dataMap.get("gooruUid") : null));
			baseCassandraDao.updateClasspageCF(ColumnFamily.CLASSPAGE.getColumnFamily(), classpageMap);
		}
	}

	/**
	 * 
	 * @param eventDataMap
	 * @param eventMap
	 */
	private void markDeletedClasspageUser(Map<String, Object> eventDataMap, Map<String, Object> eventMap) {
		Map<String, Object> classpageMap = new HashMap<String, Object>();
		String classGooruId = eventMap.containsKey(CONTENT_GOORU_OID) ? (String)eventMap.get(CONTENT_GOORU_OID) : null;
		String userUid = eventMap.containsKey("removedGooruUId")  ? (String)eventMap.get("removedGooruUId") : null;
		classpageMap.put("groupUId", ((eventMap.containsKey("groupUId") && eventMap.get("groupUId") != null) ? eventMap.get("groupUId").toString() : null));
		classpageMap.put(DELETED, Integer.valueOf(1));
		classpageMap.put("classId", classGooruId);
		classpageMap.put(USER_UID, userUid);
		baseCassandraDao.updateClasspageCF(ColumnFamily.CLASSPAGE.getColumnFamily(), classpageMap);
		baseCassandraDao.deleteColumn(ColumnFamily.USER_GROUP_ASSOCIATION.getColumnFamily(), classGooruId, userUid);
	}

	/**
	 * 
	 * @param dataMapList
	 * @param eventMap
	 */
	private void createClasspageAssignment(List<Map<String, Object>> dataMapList, Map<String, Object> eventMap) {
		/** classpage assignment create **/
		for (Map<String, Object> dataMap : dataMapList) {
			if (((eventMap.containsKey(DATA)) && ((Map<String, Object>) dataMap.get(RESOURCE)).containsKey(RESOURCE_TYPE) && (((Map<String, String>) ((Map<String, Object>) dataMap.get(RESOURCE))
					.get(RESOURCE_TYPE)).get(NAME).toString().equalsIgnoreCase(SCOLLECTION)))) {
				/** Update insights collection CF for collection mapping **/
				Map<String, Object> resourceMap = (Map<String, Object>) dataMap.get(RESOURCE);
				Map<String, Object> collectionMap = new HashMap<String, Object>();
				if (eventMap.containsKey(CONTENT_ID) && eventMap.get(CONTENT_ID) != null && !StringUtils.isBlank(eventMap.get(CONTENT_ID).toString())) {
					collectionMap.put("contentId", Long.valueOf(eventMap.get(CONTENT_ID).toString()));
				}
				collectionMap.put(GOORU_OID, eventMap.get(PARENT_GOORU_OID));
				rawUpdateDAO.updateCollectionTable(resourceMap, collectionMap);

				/**
				 * Update Insights colectionItem CF for shelf-collection mapping
				 **/
				Set<Entry<String, String>> entrySet = DataUtils.collectionItemKeys.entrySet();
				Map<String, Object> collectionItemMap = new HashMap<String, Object>();
				for (Entry<String, String> entry : entrySet) {
					if (entry.getKey().matches(COLLECTION_ITEM_FIELDS) || entry.getKey().equalsIgnoreCase(ASSOCIATION_DATE)) {
						collectionItemMap.put(entry.getValue(), (dataMap.containsKey(entry.getKey()) && dataMap.get(entry.getKey()) != null) ? dataMap.get(entry.getKey()).toString() : null);
					} else {
						collectionItemMap.put(entry.getValue(), (eventMap.containsKey(entry.getKey()) && eventMap.get(entry.getKey()) != null) ? eventMap.get(entry.getKey()).toString() : null);
					}
				}
				collectionItemMap.put(DELETED, Integer.valueOf(0));
				rawUpdateDAO.updateCollectionItemTable(dataMap, collectionItemMap);
			}
		}
	}

	/**
	 * 
	 * @param eventDataMap
	 * @param eventMap
	 */
	private void updateClasspageAssignment(Map<String, Object> eventDataMap, Map<String, Object> eventMap) {

		/** Update Insights colectionItem CF for shelf-collection mapping **/
		Map<String, Object> resourceMap = (Map<String, Object>) eventDataMap.get(RESOURCE);
		ResourceCo collection = new ResourceCo();
		/**
		 * Update Resource CF
		 */
		if (!resourceMap.isEmpty()) {
			updateResource(resourceMap, eventMap, collection, null);
		} else {
			logger.info("Resource data is empty for assignement edit");
		}

		Set<Entry<String, String>> entrySet = DataUtils.collectionItemKeys.entrySet();
		Map<String, Object> collectionItemMap = new HashMap<String, Object>();
		for (Entry<String, String> entry : entrySet) {
			if (entry.getKey().matches(COLLECTION_ITEM_FIELDS) || entry.getKey().equalsIgnoreCase(ASSOCIATION_DATE)) {
				collectionItemMap.put(entry.getValue(), (eventDataMap.containsKey(entry.getKey()) && eventDataMap.get(entry.getKey()) != null) ? eventDataMap.get(entry.getKey()).toString() : null);
			} else {
				collectionItemMap.put(entry.getValue(), (eventMap.containsKey(entry.getKey()) && eventMap.get(entry.getKey()) != null) ? eventMap.get(entry.getKey()).toString() : null);
			}
		}
		collectionItemMap.put(DELETED, Integer.valueOf(0));
		rawUpdateDAO.updateCollectionItemTable(eventDataMap, collectionItemMap);
	}

	/**
	 * Collection/Classpage Edit
	 * 
	 * @param eventDataMap
	 * @param eventMap
	 */
	private void updateShelfCollectionOrClasspage(Map<String, Object> eventDataMap, Map<String, Object> eventMap) {

		/**
		 * Update Resource CF
		 */
		ResourceCo collection = new ResourceCo();
		updateResource(eventDataMap, eventMap, collection, null);

	}

	/**
	 * Resource Create
	 * 
	 * @param eventDataMap
	 * @param eventMap
	 */
	private void createResource(Map<String, Object> eventDataMap, Map<String, Object> eventMap) {

		/**
		 * Update Resource CF for resource data
		 */
		ResourceCo resourceCo = new ResourceCo();
		rawUpdateDAO.processResource(eventDataMap, resourceCo);
		if (eventMap.containsKey(CONTENT_ID) && eventMap.get(CONTENT_ID) != null && !StringUtils.isBlank(eventMap.get(CONTENT_ID).toString())) {
			resourceCo.setContentId(Long.valueOf(eventMap.get(CONTENT_ID).toString()));
		}
		baseCassandraDao.updateResourceEntity(resourceCo);

		Map<String, Object> resourceMap = (Map<String, Object>) eventDataMap.get(RESOURCE);

		if (eventDataMap.containsKey(COLLECTION) && eventDataMap.get(COLLECTION) != null && ((Map<String, Map<String, String>>) eventDataMap.get(COLLECTION)).containsKey(RESOURCE_TYPE)
				&& (((Map<String, Map<String, String>>) eventDataMap.get(COLLECTION)).get(RESOURCE_TYPE).get(NAME).equalsIgnoreCase(SCOLLECTION))) {
			Map<String, Object> collectionMap = (Map<String, Object>) eventDataMap.get(COLLECTION);
			/**
			 * Update Resource CF for collection data
			 */
			if (!collectionMap.isEmpty()) {
				ResourceCo collection = new ResourceCo();
				rawUpdateDAO.processCollection(collectionMap, collection);
				collection.setContentId(Long.valueOf(eventMap.get(PARENT_CONTENT_ID).toString()));
				collection.setVersion(Integer.valueOf(collectionMap.get(VERSION).toString()));
				baseCassandraDao.updateResourceEntity(collection);
			} else {
				logger.info("Collection data is empty on resource create event");
			}
		}

		/** Update Insights colectionItem CF **/
		Set<Entry<String, String>> entrySet = DataUtils.collectionItemKeys.entrySet();
		Map<String, Object> collectionItemMap = new HashMap<String, Object>();
		for (Entry<String, String> entry : entrySet) {
			if (entry.getKey().matches(COLLECTION_ITEM_FIELDS) || entry.getKey().equalsIgnoreCase(ASSOCIATION_DATE)) {
				collectionItemMap.put(entry.getValue(), (eventDataMap.containsKey(entry.getKey()) && eventDataMap.get(entry.getKey()) != null) ? eventDataMap.get(entry.getKey()).toString() : null);
			} else {
				collectionItemMap.put(entry.getValue(), (eventMap.containsKey(entry.getKey()) && eventMap.get(entry.getKey()) != null) ? eventMap.get(entry.getKey()).toString() : null);
			}
		}
		collectionItemMap.put(DELETED, Integer.valueOf(0));
		rawUpdateDAO.updateCollectionItemTable(resourceMap, collectionItemMap);

		/** Update Insights Assessment Answer CF **/
		if (((Map<String, String>) resourceMap.get(RESOURCE_TYPE)).get(NAME).equalsIgnoreCase("assessment-question")) {
			Map<String, Object> assessmentAnswerMap = new HashMap<String, Object>();
			String collectionGooruOid = eventMap.get("parentGooruId").toString();
			String questionGooruOid = eventMap.get("contentGooruId").toString();
			Long collectionContentId = ((eventMap.containsKey(PARENT_CONTENT_ID) && eventMap.get(PARENT_CONTENT_ID) != null) ? Long.valueOf(eventMap.get(PARENT_CONTENT_ID).toString()) : null);
			Long questionId = ((eventMap.containsKey(CONTENT_ID) && eventMap.get(CONTENT_ID) != null && !StringUtils.isBlank(eventMap.get(CONTENT_ID).toString())) ? Long.valueOf(eventMap.get(
					CONTENT_ID).toString()) : null);
			assessmentAnswerMap.put("collectionGooruOid", collectionGooruOid);
			assessmentAnswerMap.put("questionGooruOid", questionGooruOid);
			assessmentAnswerMap.put("questionId", questionId);
			assessmentAnswerMap.put("collectionContentId", collectionContentId);
			rawUpdateDAO.updateAssessmentAnswer(resourceMap, assessmentAnswerMap);
		}
	}

	/**
	 * 
	 * @param eventDataMap
	 * @param eventMap
	 */
	private void updateResource(Map<String, Object> eventDataMap, Map<String, Object> eventMap) {

		/** Resource Edit **/

		ResourceCo resourceCo = new ResourceCo();
		Map<String, Object> resourceMap = (Map<String, Object>) eventDataMap.get(RESOURCE);
		if (eventMap.containsKey(CONTENT_ID) && eventMap.get(CONTENT_ID) != null && !StringUtils.isBlank(eventMap.get(CONTENT_ID).toString())) {
			resourceCo.setContentId(Long.valueOf(eventMap.get(CONTENT_ID).toString()));
		}
		baseCassandraDao.updateResourceEntity(rawUpdateDAO.processResource(eventDataMap, resourceCo));
		if (eventDataMap.containsKey(COLLECTION) && eventDataMap.get(COLLECTION) != null && ((Map<String, Map<String, String>>) eventDataMap.get(COLLECTION)).containsKey(RESOURCE_TYPE)
				&& (((Map<String, Map<String, String>>) eventDataMap.get(COLLECTION)).get(RESOURCE_TYPE).get(NAME).equalsIgnoreCase(SCOLLECTION))) {
			Map<String, Object> collectionMap = (Map<String, Object>) eventDataMap.get(COLLECTION);
			ResourceCo collection = new ResourceCo();
			rawUpdateDAO.processCollection(collectionMap, collection);
			if (eventMap.get(PARENT_CONTENT_ID) != null) {
				collection.setContentId(Long.valueOf(eventMap.get(PARENT_CONTENT_ID).toString()));
			}
			if (collectionMap.get(VERSION) != null) {
				collection.setVersion(Integer.valueOf(collectionMap.get(VERSION).toString()));
			}
			baseCassandraDao.updateResourceEntity(collection);
		}

		/** Update Insights colectionItem CF **/
		Set<Entry<String, String>> entrySet = DataUtils.collectionItemKeys.entrySet();
		Map<String, Object> collectionItemMap = new HashMap<String, Object>();
		for (Entry<String, String> entry : entrySet) {
			if (entry.getKey().matches(COLLECTION_ITEM_FIELDS) || entry.getKey().equalsIgnoreCase(ASSOCIATION_DATE) || entry.getKey().equalsIgnoreCase("typeName")) {
				collectionItemMap.put(entry.getValue(), (eventDataMap.containsKey(entry.getKey()) && eventDataMap.get(entry.getKey()) != null) ? eventDataMap.get(entry.getKey()).toString() : null);
			} else {
				collectionItemMap.put(entry.getValue(), (eventMap.containsKey(entry.getKey()) && eventMap.get(entry.getKey()) != null) ? eventMap.get(entry.getKey()).toString() : null);
			}
		}
		collectionItemMap.put(DELETED, Integer.valueOf(0));
		rawUpdateDAO.updateCollectionItemTable(resourceMap, collectionItemMap);

		if (((Map<String, String>) resourceMap.get(RESOURCE_TYPE)).get(NAME).equalsIgnoreCase("assessment-question")) {
			Map<String, Object> assessmentAnswerMap = new HashMap<String, Object>();
			String collectionGooruOid = eventMap.get(CONTENT_GOORU_OID).toString();
			String questionGooruOid = eventMap.get(PARENT_GOORU_OID).toString();
			Long collectionContentId = ((eventMap.containsKey(PARENT_CONTENT_ID) && eventMap.get(PARENT_CONTENT_ID) != null) ? Long.valueOf(eventMap.get(PARENT_CONTENT_ID).toString()) : null);
			Long questionId = ((eventMap.containsKey(CONTENT_ID) && eventMap.get(CONTENT_ID) != null && !StringUtils.isBlank(eventMap.get(CONTENT_ID).toString())) ? Long.valueOf(eventMap.get(
					CONTENT_ID).toString()) : null);
			assessmentAnswerMap.put("collectionGooruOid", collectionGooruOid);
			assessmentAnswerMap.put("questionGooruOid", questionGooruOid);
			assessmentAnswerMap.put("questionId", questionId);
			assessmentAnswerMap.put("collectionContentId", collectionContentId);
			rawUpdateDAO.updateAssessmentAnswer((Map<String, Object>) eventDataMap.get("questionInfo"), assessmentAnswerMap);
		}

	}

	/**
	 * 
	 * @param eventDataMap
	 * @param eventMap
	 */
	private void updateRegisteredUser(Map<String, Object> eventDataMap, Map<String, Object> eventMap) {

		UserCo userCo = new UserCo();
		String organizationUId = null;
		if (eventDataMap.containsKey(ORGANIZATION_UID) && eventDataMap.get(ORGANIZATION_UID) != null) {
			organizationUId = eventDataMap.get(ORGANIZATION_UID).toString();
		} else if (eventMap.containsKey(ORGANIZATION_UID) && eventMap.get(ORGANIZATION_UID) != null) {
			organizationUId = eventMap.get(ORGANIZATION_UID).toString();
			Map<String, String> organizationMap = new HashMap<String, String>();
			organizationMap.put("partyUid", organizationUId);
			userCo.setOrganization(organizationMap);
		}
		userCo.setAccountId(organizationUId);
		userCo.setOrganizationUid(organizationUId);
		baseCassandraDao.updateUserEntity(rawUpdateDAO.processUser(eventDataMap, userCo));

	}

	/**
	 * 
	 * @param eventDataMap
	 * @param eventMap
	 */
	private void updateUserProfileData(Map<String, Object> eventDataMap, Map<String, Object> eventMap) {

		UserCo userCo = new UserCo();
		String organizationUId = null;
		if (eventDataMap.containsKey(ORGANIZATION_UID) && eventDataMap.get(ORGANIZATION_UID) != null) {
			organizationUId = eventDataMap.get(ORGANIZATION_UID).toString();
		} else if (eventMap.containsKey(ORGANIZATION_UID) && eventMap.get(ORGANIZATION_UID) != null) {
			organizationUId = eventMap.get(ORGANIZATION_UID).toString();
			Map<String, String> organizationMap = new HashMap<String, String>();
			organizationMap.put("partyUid", organizationUId);
			userCo.setOrganization(organizationMap);
		}
		userCo.setOrganizationUid(organizationUId);
		userCo.setAccountId(organizationUId);
		userCo.setAboutMe((eventDataMap.containsKey("aboutMe") && eventDataMap.get("aboutMe") != null) ? eventDataMap.get("aboutMe").toString() : null);
		userCo.setGrade((eventDataMap.containsKey("grade") && eventDataMap.get("grade") != null) ? eventDataMap.get("grade").toString() : null);
		userCo.setNetwork((eventDataMap.containsKey("school") && eventDataMap.get("school") != null) ? eventDataMap.get("school").toString() : null);
		userCo.setNotes((eventDataMap.containsKey("notes") && eventDataMap.get("notes") != null) ? eventDataMap.get("notes").toString() : null);
		baseCassandraDao.updateUserEntity(rawUpdateDAO.processUser((Map<String, Object>) eventDataMap.get("user"), userCo));

	}

	/**
	 * 
	 * @param eventDataMap
	 * @param eventMap
	 */
	private void markItemDelete(Map<String, Object> eventMap) {
		if (eventMap.get(EVENT_NAME).equals(LoaderConstants.ITEM_DOT_DELETE.getName())) {
			Map<String, Object> collectionItemMap = new HashMap<String, Object>();
			collectionItemMap.put(COLLECTION_ITEM_ID, (eventMap.containsKey(ITEM_ID) ? eventMap.get(ITEM_ID).toString() : null));
			collectionItemMap.put(DELETED, Integer.valueOf(1));
			rawUpdateDAO.updateCollectionItemTable(eventMap, collectionItemMap);
		}
		baseCassandraDao.deleteColumn(ColumnFamily.COLLECTIONITEMASSOC.getColumnFamily(), (String)eventMap.get(PARENT_GOORU_OID), (String)eventMap.get(CONTENT_GOORU_OID));
		
	}

	private void updateResource(Map<String, Object> eventDataMap, Map<String, Object> eventMap, ResourceCo resourceCo, Map<String, Object> collectionMap) {
		if (eventMap.containsKey(CONTENT_ID) && eventMap.get(CONTENT_ID) != null && !StringUtils.isBlank(eventMap.get(CONTENT_ID).toString())) {
			resourceCo.setContentId(Long.valueOf(eventMap.get(CONTENT_ID).toString()));
			if (collectionMap != null)
				collectionMap.put(CONTENT_ID, Long.valueOf(eventMap.get(CONTENT_ID).toString()));
		}
		try {
			baseCassandraDao.updateResourceEntity(rawUpdateDAO.processCollection(eventDataMap, resourceCo));
		} catch (Exception ex) {
			logger.error("Unable to save resource entity for Id {} due to {}", eventDataMap.get(GOORU_OID).toString(), ex);
		}
	}

	/**
	 * Re-Computations operation - if course,unit,lesson or assessment deletes or move 
	 * @param eventMap
	 */
	public void processClassActivityOpertaions(Map<String, Object> eventMap) {
		try {
			String contentGooruId = eventMap.get(CONTENT_GOORU_OID) != null ? (String) eventMap.get(CONTENT_GOORU_OID) : null;
			String lessonGooruId = eventMap.get(LESSON_GOORU_OID) != null ? (String) eventMap.get(LESSON_GOORU_OID) : null;
			String unitGooruId = eventMap.get(UNIT_GOORU_OID) != null ? (String) eventMap.get(UNIT_GOORU_OID) : null;
			String courseGooruId = eventMap.get(COURSE_GOORU_OID) != null ? (String) eventMap.get(COURSE_GOORU_OID) : null;
			String collectionType = eventMap.get(TYPE) != null ? (String) eventMap.get(TYPE) : null;
			for (String classGooruId : (eventMap.get("classGooruIds") + "").replaceAll("\\[", "").replaceAll("\\]", "").replaceAll("\"", "").split(COMMA)) {
				/**
				 * Get Students list for a class
				 */
				ColumnList<String> studentList = baseCassandraDao.readWithKey(ColumnFamily.USER_GROUP_ASSOCIATION.getColumnFamily(), classGooruId, 0);
				generateDeleteTasks(classGooruId, courseGooruId, unitGooruId, lessonGooruId, contentGooruId, studentList.getColumnNames(), collectionType);
			}
		} catch (Exception e) {
			logger.error("Exception:", e);
		}

	}

	/**
	 * Creating a tasks to delete score and recompute. A single task will process one user data.
	 * @param classGooruId
	 * @param courseGooruId
	 * @param unitGooruId
	 * @param lessonGooruId
	 * @param contentGooruId
	 * @param studentsIds
	 * @param collectionType
	 */
	private void generateDeleteTasks(final String classGooruId, final String courseGooruId, final String unitGooruId, final String lessonGooruId, final String contentGooruId,
			final Collection<String> studentsIds, final String collectionType) {
		try {
			Set<Callable<String>> deleteTasks = new HashSet<Callable<String>>();
			for (final String studentUId : studentsIds) {
				deleteTasks.add(new Callable<String>() {
					public String call() throws Exception {
						return generateKeysAndDeleteColumns(classGooruId, courseGooruId, unitGooruId, lessonGooruId, contentGooruId, studentUId, collectionType);
					}
				});
			}

			List<Future<String>> taskStatues = service.invokeAll(deleteTasks);
			for (Future<String> taskStatus : taskStatues) {
				logger.info(taskStatus.get());
			}
		} catch (Exception e) {
			logger.error("Exception:", e);
		}
	}

	/**
	 * Based on collection type, travel through all the level and remove score columns and row keys where neccessary 
	 * @param classGooruId
	 * @param courseGooruId
	 * @param unitGooruId
	 * @param lessonGooruId
	 * @param contentGooruId
	 * @param gooruUUID
	 * @param collectionType
	 * @return
	 */
	private String generateKeysAndDeleteColumns(final String classGooruId, final String courseGooruId, final String unitGooruId, final String lessonGooruId, final String contentGooruId,
			final String gooruUUID, final String collectionType) {
		try {
			Set<String> reComputeKeys = null;
			MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));
			logger.info("processing started at : {} for user : {}",new Date(),gooruUUID);
			logger.info("Deletion happening at the level: {}",collectionType);
			if (collectionType.equalsIgnoreCase(COURSE)) {
				String parentKey = generateColumnKey(classGooruId, courseGooruId, gooruUUID, ASSESSMENT, _SCORE_IN_PERCENTAGE);
				logger.info("parentKey: {}",parentKey);
				ColumnList<String> attemptedAssessmentList = baseCassandraDao.readWithKey(ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), parentKey, 0);
				Set<String> unitMap = generateUnitRowKeys(classGooruId, courseGooruId, gooruUUID, attemptedAssessmentList.getColumnNames());
				deleteRowKeys(ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), unitMap);
				Set<String> lessonMap = generateLessonRowKeys(classGooruId, courseGooruId, gooruUUID, attemptedAssessmentList.getColumnNames());
				deleteRowKeys(ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), lessonMap);
				baseCassandraDao.deleteRowKey(ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), parentKey);
				reComputeKeys = generateRecomputationKeys(classGooruId, courseGooruId, gooruUUID, attemptedAssessmentList.getColumnNames());
			} else if (collectionType.equalsIgnoreCase(UNIT)) {
				String parentKey = generateColumnKey(classGooruId, courseGooruId,unitGooruId, gooruUUID, ASSESSMENT, _SCORE_IN_PERCENTAGE);
				logger.info("parentKey: {}",parentKey);
				ColumnList<String> attemptedAssessmentList = baseCassandraDao.readWithKey(ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), parentKey, 0);
				Set<String> unitMap = generateLessonRowKeys(classGooruId, courseGooruId, unitGooruId, gooruUUID, attemptedAssessmentList.getColumnNames());
				deleteRowKeys(ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), unitMap);
				for (String columnKeySuffix : attemptedAssessmentList.getColumnNames()) {
					baseCassandraDao.deleteColumn(ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), generateColumnKey(classGooruId, courseGooruId, gooruUUID, ASSESSMENT, _SCORE_IN_PERCENTAGE),
							generateColumnKey(unitGooruId, columnKeySuffix));
				}
				baseCassandraDao.deleteRowKey(ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), parentKey);
				reComputeKeys = generateRecomputationKeys(classGooruId, courseGooruId, unitGooruId, gooruUUID, attemptedAssessmentList.getColumnNames());
			} else if (collectionType.equalsIgnoreCase(LESSON)) {
				String parentKey = generateColumnKey(classGooruId, courseGooruId, unitGooruId, lessonGooruId, gooruUUID, ASSESSMENT, _SCORE_IN_PERCENTAGE);
				logger.info("parentKey: {}",parentKey);
				ColumnList<String> attemptedAssessmentList = baseCassandraDao.readWithKey(ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), parentKey, 0);
				for (String columnKeySuffix : attemptedAssessmentList.getColumnNames()) {
					baseCassandraDao.deleteColumn(ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), generateColumnKey(classGooruId, courseGooruId, gooruUUID, ASSESSMENT, _SCORE_IN_PERCENTAGE),
							generateColumnKey(unitGooruId, lessonGooruId, columnKeySuffix));
					baseCassandraDao.deleteColumn(ColumnFamily.CLASS_ACTIVITY.getColumnFamily(),
							generateColumnKey(classGooruId, courseGooruId, unitGooruId, gooruUUID, ASSESSMENT, _SCORE_IN_PERCENTAGE), generateColumnKey(lessonGooruId, columnKeySuffix));

				}
				baseCassandraDao.deleteRowKey(ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), parentKey);
				reComputeKeys = generateRecomputationKeys(classGooruId, courseGooruId, unitGooruId, lessonGooruId, gooruUUID);
			} else if (collectionType.matches(ASSESSMENT_TYPES)) {
				Map<String, String> keysAndColumns = generateKeysAndColumnIfAssessmentDelete(classGooruId, courseGooruId, unitGooruId, lessonGooruId, contentGooruId, gooruUUID);
				logger.info("parentKey: {}",generateColumnKey(classGooruId, courseGooruId,unitGooruId, lessonGooruId,gooruUUID, ASSESSMENT, _SCORE_IN_PERCENTAGE));
				for (Map.Entry<String, String> keyColumnPair : keysAndColumns.entrySet()) {
					if (baseCassandraDao.getCount(ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), keyColumnPair.getKey()) > 1) {
						baseCassandraDao.deleteColumn(ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), keyColumnPair.getKey(), keyColumnPair.getValue());
					} else {
						baseCassandraDao.deleteRowKey(ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), keyColumnPair.getKey());
					}
				}
				reComputeKeys = generateRecomputationKeys(classGooruId, courseGooruId, unitGooruId, lessonGooruId, gooruUUID);
			}
			logger.info("Deletion process is completed at : {} for user : {}",new Date(),gooruUUID);
			if (reComputeKeys != null) {
				for (String key : reComputeKeys) {
					aggregateClassActivityScore(key, m);
					logger.info("Re-Computed score for : {}",key);
				}

				m.execute();
			}
			logger.info("Re computation is completed at : {} for user : {}",new Date(),gooruUUID);
		} catch (Exception e) {
			logger.error("Exception:" + e);
		}
		return generateColumnKey(RECOMPUTE_SUCCESS_MESSAGE,gooruUUID);
	}

	/**
	 * Process set keys and delete a row.
	 * @param cfName
	 * @param keySet
	 */
	private void deleteRowKeys(String cfName, Set<String> keySet) {
		for (String key : keySet) {
			baseCassandraDao.deleteRowKey(ColumnFamily.CLASS_ACTIVITY.getColumnFamily(), key);
		}
	}

	/**
	 * Generate Keys and Columns is assessment delete in lesson level
	 * @param classGooruId
	 * @param courseGooruId
	 * @param unitGooruId
	 * @param lessonGooruId
	 * @param contentGooruId
	 * @param gooruUUID
	 * @return
	 */
	private Map<String, String> generateKeysAndColumnIfAssessmentDelete(String classGooruId, String courseGooruId, String unitGooruId, String lessonGooruId, String contentGooruId, String gooruUUID) {
		Map<String, String> keysAndCoulmns = new HashMap<String, String>();
		keysAndCoulmns.put(generateColumnKey(classGooruId, courseGooruId, unitGooruId, lessonGooruId, gooruUUID, ASSESSMENT, _SCORE_IN_PERCENTAGE), contentGooruId);
		keysAndCoulmns.put(generateColumnKey(classGooruId, courseGooruId, unitGooruId, gooruUUID, ASSESSMENT, _SCORE_IN_PERCENTAGE), generateColumnKey(lessonGooruId, contentGooruId));
		keysAndCoulmns.put(generateColumnKey(classGooruId, courseGooruId, gooruUUID, ASSESSMENT, _SCORE_IN_PERCENTAGE), generateColumnKey(unitGooruId, lessonGooruId, contentGooruId));
		return keysAndCoulmns;
	}
	/**
	 * Generate unit level keys is delete happens in course
	 * @param classGooruId
	 * @param courseGooruId
	 * @param gooruUUID
	 * @param columnNames
	 * @return
	 */
	private Set<String> generateUnitRowKeys(String classGooruId, String courseGooruId, String gooruUUID, Collection<String> columnNames) {
		Set<String> unitKeys = new HashSet<String>();
		for (String columnName : columnNames) {
			String[] unit = columnName.split(SEPERATOR);
			unitKeys.add(generateColumnKey(classGooruId, courseGooruId, unit[0], gooruUUID, ASSESSMENT, _SCORE_IN_PERCENTAGE));
		}
		return unitKeys;
	}

	/**
	 * Generate lesson level keys is delete happens in course
	 * @param classGooruId
	 * @param courseGooruId
	 * @param gooruUUID
	 * @param columnNames
	 * @return
	 */
	private Set<String> generateLessonRowKeys(String classGooruId, String courseGooruId, String gooruUUID, Collection<String> columnNames) {
		Set<String> lessonKeys = new HashSet<String>();
		for (String columnName : columnNames) {
			String[] lesson = columnName.split(SEPERATOR);
			lessonKeys.add(generateColumnKey(classGooruId, courseGooruId, lesson[0], lesson[1], gooruUUID, ASSESSMENT, _SCORE_IN_PERCENTAGE));
		}
		return lessonKeys;
	}

	/**
	 * Generate lesson level keys is delete happens in unit
	 * @param classGooruId
	 * @param courseGooruId
	 * @param unitGooruId
	 * @param gooruUUID
	 * @param columnNames
	 * @return
	 */
	private Set<String> generateLessonRowKeys(String classGooruId, String courseGooruId, String unitGooruId, String gooruUUID, Collection<String> columnNames) {
		Set<String> lessonKeys = new HashSet<String>();
		for (String columnName : columnNames) {
			String[] lesson = columnName.split(SEPERATOR);
			lessonKeys.add(generateColumnKey(classGooruId, courseGooruId, unitGooruId, lesson[0], gooruUUID, ASSESSMENT, _SCORE_IN_PERCENTAGE));
		}
		return lessonKeys;
	}

	/**
	 * Generate Keys to update re-computed score
	 * @param classGooruId
	 * @param courseGooruId
	 * @param gooruUUID
	 * @param columnNames
	 * @return
	 */
	private Set<String> generateRecomputationKeys(String classGooruId, String courseGooruId, String gooruUUID, Collection<String> columnNames) {
		Set<String> lessonKeys = new HashSet<String>();
		for (String columnName : columnNames) {
			String[] lesson = columnName.split(SEPERATOR);
			lessonKeys.add(generateColumnKey(classGooruId, courseGooruId, lesson[0], lesson[1], gooruUUID));
			lessonKeys.add(generateColumnKey(classGooruId, courseGooruId, lesson[0], gooruUUID));
			lessonKeys.add(generateColumnKey(classGooruId, courseGooruId, gooruUUID));

		}
		return lessonKeys;
	}

	/**
	 * Generate Keys to update re-computed score
	 * @param classGooruId
	 * @param courseGooruId
	 * @param unitGooruId
	 * @param gooruUUID
	 * @param columnNames
	 * @return
	 */
	private Set<String> generateRecomputationKeys(String classGooruId, String courseGooruId, String unitGooruId, String gooruUUID, Collection<String> columnNames) {
		Set<String> lessonKeys = new HashSet<String>();
		for (String columnName : columnNames) {
			String[] lesson = columnName.split(SEPERATOR);
			lessonKeys.add(generateColumnKey(classGooruId, courseGooruId, unitGooruId, lesson[0], gooruUUID));
			lessonKeys.add(generateColumnKey(classGooruId, courseGooruId, unitGooruId, gooruUUID));
			lessonKeys.add(generateColumnKey(classGooruId, courseGooruId, gooruUUID));
		}
		return lessonKeys;
	}

	/**
	 * Generate Keys to update re-computed score
	 * @param classGooruId
	 * @param courseGooruId
	 * @param unitGooruId
	 * @param lessonGooruId
	 * @param gooruUUID
	 * @return
	 */
	private Set<String> generateRecomputationKeys(String classGooruId, String courseGooruId, String unitGooruId, String lessonGooruId, String gooruUUID) {
		Set<String> lessonKeys = new HashSet<String>();
		lessonKeys.add(generateColumnKey(classGooruId, courseGooruId, unitGooruId, lessonGooruId, gooruUUID));
		lessonKeys.add(generateColumnKey(classGooruId, courseGooruId, unitGooruId, gooruUUID));
		lessonKeys.add(generateColumnKey(classGooruId, courseGooruId, gooruUUID));
		return lessonKeys;
	}

	/**
	 * Append string with ~ seperator
	 * @param columns
	 * @return
	 */
	private String generateColumnKey(String... columns) {
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
		ColumnList<String> counterColumns = baseCassandraDao.readWithKey(ColumnFamily.SESSION_ACTIVITY.getColumnFamily(), key, 0);
		boolean status = false;
		String attemptStatus = counterColumns.getColumnByName(columnPrefix + SEPERATOR + _QUESTION_STATUS) != null ? counterColumns.getStringValue(columnPrefix + SEPERATOR + _QUESTION_STATUS, null)
				: null;
		if (attemptStatus != null && attemptStatus.matches(ANSWERING_STATUS)) {
			status = true;
		}
		return status;
	}
}
