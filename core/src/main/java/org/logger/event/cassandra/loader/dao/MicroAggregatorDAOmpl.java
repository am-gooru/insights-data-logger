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

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.ednovo.data.model.JSONDeserializer;
import org.ednovo.data.model.ResourceCo;
import org.ednovo.data.model.TypeConverter;
import org.ednovo.data.model.UserCo;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.logger.event.cassandra.loader.CassandraConnectionProvider;
import org.logger.event.cassandra.loader.ColumnFamily;
import org.logger.event.cassandra.loader.Constants;
import org.logger.event.cassandra.loader.DataUtils;
import org.logger.event.cassandra.loader.LoaderConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;

public class MicroAggregatorDAOmpl extends BaseDAOCassandraImpl implements MicroAggregatorDAO, Constants {

	private static final Logger logger = LoggerFactory.getLogger(MicroAggregatorDAOmpl.class);

	private CassandraConnectionProvider connectionProvider;

	private SimpleDateFormat secondsDateFormatter;

	private long questionCountInQuiz = 0L;

	private BaseCassandraRepoImpl baseCassandraDao;

	private RawDataUpdateDAOImpl rawUpdateDAO;

	public static Map<String, Object> cache;

	public MicroAggregatorDAOmpl(CassandraConnectionProvider connectionProvider) {
		super(connectionProvider);
		this.connectionProvider = connectionProvider;
		this.baseCassandraDao = new BaseCassandraRepoImpl(this.connectionProvider);
		this.secondsDateFormatter = new SimpleDateFormat("yyyyMMddkkmmss");
		cache = new LinkedHashMap<String, Object>();
		this.rawUpdateDAO = new RawDataUpdateDAOImpl(this.connectionProvider);
	}

	/**
	 * This is process all the real time reports event(collection.play,resource.play,collection.play,etc..)
	 * 
	 * @param eventMap
	 * @param aggregatorJson
	 * 
	 */
	public void realTimeMetrics(Map<String, Object> eventMap, String aggregatorJson) {
		if (cache.size() > 100000) {
			cache.clear();
		}
		
		List<String> classPages = null;
		classPages = this.getClassPages(eventMap);

		String eventName = eventMap.containsKey(EVENT_NAME) ? eventMap.get(EVENT_NAME).toString() : EMPTY_STRING;
		String gooruUUID = eventMap.containsKey(GOORUID) ? eventMap.get(GOORUID).toString() : EMPTY_STRING;

		String key = eventMap.get(CONTENT_GOORU_OID) != null ? eventMap.get(CONTENT_GOORU_OID).toString() : null;
		List<String> keysList = new ArrayList<String>();

		MutationBatch resourceMutation = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);

		MutationBatch microAggMutation = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);

		boolean isStudent = false;
		/*
		 * 
		 * Update last accessed time/user
		 */

		baseCassandraDao.generateNonCounter(ColumnFamily.RESOURCE.getColumnFamily(), key, LAST_ACCESSED, Long.parseLong(EMPTY_STRING + eventMap.get(END_TIME)),
				resourceMutation);
		baseCassandraDao
				.generateNonCounter(ColumnFamily.RESOURCE.getColumnFamily(), key, LAST_ACCESSED_USER, gooruUUID, resourceMutation);

		try {
			resourceMutation.execute();
		} catch (Exception e) {
			logger.error("Exception while saving last accessed time.",e);
		}
		/* Maintain session - Start */

		if (eventName.equalsIgnoreCase(LoaderConstants.CPV1.getName()) && eventMap.containsKey(TYPE) && eventMap.get(TYPE).toString().equalsIgnoreCase(START)) {
			Date eventDateTime = new Date(Long.parseLong(EMPTY_STRING + eventMap.get(START_TIME)));
			String eventRowKey = secondsDateFormatter.format(eventDateTime).toString();

			if (classPages != null && classPages.size() > 0) {
				for (String classPage : classPages) {
				baseCassandraDao.generateNonCounter(ColumnFamily.MICROAGGREGATION.getColumnFamily(), classPage + SEPERATOR + key + SEPERATOR
						+ gooruUUID, eventMap.get(SESSION_ID).toString(), eventRowKey, microAggMutation);
				}
			}
			baseCassandraDao.generateNonCounter(ColumnFamily.MICROAGGREGATION.getColumnFamily(), key + SEPERATOR + gooruUUID, eventMap.get(SESSION_ID)
					.toString(), eventRowKey, microAggMutation);
		}

		/* Maintain session - END */

		if (eventName.equalsIgnoreCase(LoaderConstants.CPV1.getName())) {
			questionCountInQuiz = this.getQuestionCount(eventMap);
			if (classPages != null && classPages.size() > 0) {
				for (String classPage : classPages) {
					boolean isOwner = baseCassandraDao.getClassPageOwnerInfo(ColumnFamily.CLASSPAGE.getColumnFamily(), gooruUUID, classPage, 0);

					logger.info("isOwner : {}", isOwner);

					if (cache.containsKey(gooruUUID + SEPERATOR + classPage)) {
						isStudent = (Boolean) cache.get(gooruUUID + SEPERATOR + classPage);
					} else {

						isStudent = baseCassandraDao.isUserPartOfClass(ColumnFamily.CLASSPAGE.getColumnFamily(), gooruUUID, classPage, 0);

						int retryCount = 1;
						while (retryCount < 5 && !isStudent) {
							try {
								Thread.sleep(500);
							} catch (Exception e) {
								logger.error("Exception while waiting for classpage join.",e);
							}
							isStudent = baseCassandraDao.isUserPartOfClass(ColumnFamily.CLASSPAGE.getColumnFamily(), gooruUUID, classPage, 0);
							logger.info("Retrying to check if a student : {}", retryCount);
							retryCount++;
						}
						cache.put(gooruUUID + SEPERATOR + classPage, isStudent);
					}
					logger.info("isStudent : {}", isStudent);

					eventMap.put(CLASSPAGEGOORUOID, classPage);
					if (!isOwner && isStudent) {
						keysList.add(ALL_SESSION + classPage + SEPERATOR + key);
						keysList.add(ALL_SESSION + classPage + SEPERATOR + key + SEPERATOR + eventMap.get(GOORUID));
					}

					keysList.add(eventMap.get(SESSION_ID) + SEPERATOR + classPage + SEPERATOR + key + SEPERATOR + gooruUUID);
					logger.info("Recent Key 1: {} ", eventMap.get(SESSION_ID) + SEPERATOR + classPage + SEPERATOR + key + SEPERATOR + gooruUUID);
					baseCassandraDao.generateNonCounter(ColumnFamily.MICROAGGREGATION.getColumnFamily(), RECENT_SESSION + classPage + SEPERATOR + key, gooruUUID,
							eventMap.get(SESSION_ID).toString(), microAggMutation);

				}
			}
			keysList.add(ALL_SESSION + eventMap.get(CONTENT_GOORU_OID).toString());
			keysList.add(ALL_SESSION + eventMap.get(CONTENT_GOORU_OID).toString() + SEPERATOR + eventMap.get(GOORUID));
			keysList.add(eventMap.get(SESSION_ID) + SEPERATOR + eventMap.get(CONTENT_GOORU_OID).toString() + SEPERATOR + eventMap.get(GOORUID));
			baseCassandraDao.generateNonCounter(ColumnFamily.MICROAGGREGATION.getColumnFamily(), RECENT_SESSION + key, gooruUUID,
					eventMap.get(SESSION_ID).toString(), microAggMutation);
		}

		if (eventName.equalsIgnoreCase(LoaderConstants.CRPV1.getName()) || eventName.equalsIgnoreCase(LoaderConstants.CRAV1.getName())) {

			if (classPages != null && classPages.size() > 0) {
				for (String classPage : classPages) {
					boolean isOwner = baseCassandraDao.getClassPageOwnerInfo(ColumnFamily.CLASSPAGE.getColumnFamily(), gooruUUID, classPage, 0);

					if (cache.containsKey(gooruUUID + SEPERATOR + classPage)) {
						isStudent = (Boolean) cache.get(eventMap.get(GOORUID) + SEPERATOR + classPage);
					} else {

						isStudent = baseCassandraDao.isUserPartOfClass(ColumnFamily.CLASSPAGE.getColumnFamily(), gooruUUID, classPage, 0);

						int retryCount = 1;
						while (retryCount < 5 && !isStudent) {
							try {
								Thread.sleep(500);
							} catch (Exception e) {
								logger.error("Exception while waiting for classpage join.",e);
							}
							;
							isStudent = baseCassandraDao.isUserPartOfClass(ColumnFamily.CLASSPAGE.getColumnFamily(), gooruUUID, classPage, 0);
							logger.info("Retrying to check if a student : {}", retryCount);
							retryCount++;
						}
						cache.put(gooruUUID + SEPERATOR + classPage, isStudent);
					}
					logger.info("isStudent : {}", isStudent);
					eventMap.put(CLASSPAGEGOORUOID, classPage);
					if (!isOwner && isStudent) {
						keysList.add(ALL_SESSION + classPage + SEPERATOR + eventMap.get(PARENT_GOORU_OID));
						keysList.add(ALL_SESSION + classPage + SEPERATOR + eventMap.get(PARENT_GOORU_OID) + SEPERATOR + gooruUUID);
					}
					keysList.add(eventMap.get(SESSION_ID) + SEPERATOR + classPage + SEPERATOR + eventMap.get(PARENT_GOORU_OID) + SEPERATOR + gooruUUID);
					logger.info("Recent Key 3: {} ", eventMap.get(SESSION_ID) + SEPERATOR + classPage + SEPERATOR + eventMap.get(PARENT_GOORU_OID) + SEPERATOR + gooruUUID);
					baseCassandraDao.generateNonCounter(ColumnFamily.MICROAGGREGATION.getColumnFamily(), RECENT_SESSION + classPage + SEPERATOR + eventMap.get(PARENT_GOORU_OID), gooruUUID
							, eventMap.get(SESSION_ID).toString(), microAggMutation);

				}
			}
			keysList.add(ALL_SESSION + eventMap.get(PARENT_GOORU_OID));
			keysList.add(ALL_SESSION + eventMap.get(PARENT_GOORU_OID) + SEPERATOR + gooruUUID);
			keysList.add(eventMap.get(SESSION_ID) + SEPERATOR + eventMap.get(PARENT_GOORU_OID) + SEPERATOR + gooruUUID);
			baseCassandraDao.generateNonCounter(ColumnFamily.MICROAGGREGATION.getColumnFamily(), RECENT_SESSION + eventMap.get(PARENT_GOORU_OID), gooruUUID,
					eventMap.get(SESSION_ID).toString(), microAggMutation);

		}

		if (eventName.equalsIgnoreCase(LoaderConstants.RUFB.getName())) {
			if (classPages != null && classPages.size() > 0) {
				for (String classPage : classPages) {
					keysList.add(eventMap.get(SESSION_ID) + SEPERATOR + classPage + SEPERATOR + eventMap.get(PARENT_GOORU_OID) + SEPERATOR + gooruUUID);
					keysList.add(ALL_SESSION + classPage + SEPERATOR + eventMap.get(PARENT_GOORU_OID) + SEPERATOR + gooruUUID);
				}
			}
		}
		/**
		 * Saving data in micro_aggregation columnfamily.
		 */
		try {
			microAggMutation.execute();
		} catch (Exception e) {
			logger.error("Exception while saving micro_aggregation columnfamily.",e);
		}

		if (keysList != null && keysList.size() > 0) {
			JSONObject j = null;
			try {
				j = new JSONObject(aggregatorJson);
			} catch (Exception e) {
				logger.error("Exception while aggregator json conversion.",e);
			}
			this.startCounters(eventMap, j, keysList, key);
			this.postAggregatorUpdate(eventMap, j, keysList, key);
			this.startCounterAggregator(eventMap, j, keysList, key);
		}
	}

	/**
	 * 
	 * @param eventMap
	 * @param aggregatorJson
	 * @param keysList
	 * @param key
	 * @throws JSONException
	 */
	public void postAggregatorUpdate(Map<String, Object> eventMap, JSONObject aggregatorJson, List<String> keysList, String key) {
		MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
		Map<String, Object> m1 = JSONDeserializer.deserialize(aggregatorJson.toString(), new TypeReference<Map<String, Object>>() {
		});
		Set<Map.Entry<String, Object>> entrySet = m1.entrySet();

		for (Entry entry : entrySet) {
			Set<Map.Entry<String, Object>> entrySets = m1.entrySet();
			Map<String, Object> e = (Map<String, Object>) m1.get(entry.getKey());
			for (String localKey : keysList) {
				if (e.get(AGGTYPE) != null && e.get(AGGTYPE).toString().equalsIgnoreCase(COUNTER)) {
					if (!(entry.getKey() != null && entry.getKey().toString().equalsIgnoreCase(CHOICE))
							&& !(entry.getKey().toString().equalsIgnoreCase(LoaderConstants.TOTALVIEWS.getName()) && eventMap.get(TYPE).toString().equalsIgnoreCase(STOP))
							&& !eventMap.get(EVENT_NAME).toString().equalsIgnoreCase(LoaderConstants.CRAV1.getName())) {
						long value = this.getCounterLongValue(localKey, key + SEPERATOR + entry.getKey().toString());
						baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), localKey, key + SEPERATOR + entry.getKey().toString(), value, m);

					}

					if (entry.getKey() != null && entry.getKey().toString().equalsIgnoreCase(CHOICE) && eventMap.get(RESOURCE_TYPE).toString().equalsIgnoreCase(QUESTION)
							&& eventMap.get(TYPE).toString().equalsIgnoreCase(STOP)) {
						int[] attemptTrySequence = TypeConverter.stringToIntArray(EMPTY_STRING + eventMap.get(ATTMPT_TRY_SEQ));
						int[] attempStatus = TypeConverter.stringToIntArray(EMPTY_STRING + eventMap.get(ATTMPT_STATUS));
						String answerStatus = null;
						int status = 0;
						status = (Integer) eventMap.get("attemptCount");
						if (status != 0) {
							status = status - 1;
						}
						if (attempStatus[0] == 1) {
							answerStatus = LoaderConstants.CORRECT.getName();
						} else if (attempStatus[0] == 0) {
							answerStatus = LoaderConstants.INCORRECT.getName();
						}
						String option = DataUtils.makeCombinedAnswerSeq(attemptTrySequence.length == 0 ? 0 : attemptTrySequence[0]);
						if (option != null && option.equalsIgnoreCase(LoaderConstants.SKIPPED.getName())) {
							answerStatus = option;
						}
						String openEndedText = eventMap.get(TEXT).toString();
						if (eventMap.get(QUESTION_TYPE).toString().equalsIgnoreCase(OE) && openEndedText != null && !openEndedText.isEmpty()) {
							option = "A";
						}
						boolean answered = this.isUserAlreadyAnswered(localKey, key);

						if (answered) {
							if (!option.equalsIgnoreCase(LoaderConstants.SKIPPED.getName())) {
								long value = this.getCounterLongValue(localKey, key + SEPERATOR + option);
								baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), localKey, key + SEPERATOR + option, value, m);
							}
						} else {
							long value = this.getCounterLongValue(localKey, key + SEPERATOR + option);
							baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), localKey, key + SEPERATOR + option, value, m);
						}
						if (!eventMap.get(QUESTION_TYPE).toString().equalsIgnoreCase(OE) && !answerStatus.equalsIgnoreCase(LoaderConstants.SKIPPED.getName())) {
							long values = this.getCounterLongValue(localKey, key + SEPERATOR + answerStatus);
							baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), localKey, key + SEPERATOR + answerStatus, values, m);
						}
					}

				}
				this.realTimeAggregator(localKey, eventMap);
			}

		}
		try {
			m.execute();
		} catch (Exception e) {
			logger.error("Exception:Unable to save post aggregated data.", e);
		}
	}

	/**
	 * 
	 * @param eventMap
	 * @param aggregatorJson
	 * @param keysList
	 * @param key
	 * @throws JSONException
	 */
	public void startCounterAggregator(Map<String, Object> eventMap, JSONObject aggregatorJson, List<String> keysList, String key) {

		MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
		Map<String, Object> m1 = JSONDeserializer.deserialize(aggregatorJson.toString(), new TypeReference<Map<String, Object>>() {
		});
		Set<Map.Entry<String, Object>> entrySet = m1.entrySet();

		for (Entry entry : entrySet) {
			Set<Map.Entry<String, Object>> entrySets = m1.entrySet();
			Map<String, Object> e = (Map<String, Object>) m1.get(entry.getKey());
			for (String localKey : keysList) {
				if (e.get(AGGTYPE) != null && e.get(AGGTYPE).toString().equalsIgnoreCase(AGG)) {
					if (e.get(AGGMODE) != null && e.get(AGGMODE).toString().equalsIgnoreCase(AVG)) {
						this.calculateAvg(localKey, eventMap.get(CONTENT_GOORU_OID) + SEPERATOR + e.get(DIVISOR).toString(), eventMap.get(CONTENT_GOORU_OID) + SEPERATOR + e.get(DIVIDEND).toString(),
								eventMap.get(CONTENT_GOORU_OID) + SEPERATOR + entry.getKey().toString());
					}

					if (e.get(AGGMODE) != null && e.get(AGGMODE).toString().equalsIgnoreCase(SUMOFAVG)) {
						long averageC = this.iterateAndFindAvg(localKey);
						baseCassandraDao.saveLongValue(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), localKey, eventMap.get(PARENT_GOORU_OID) + SEPERATOR + entry.getKey().toString(), averageC);
						long averageR = this.iterateAndFindAvg(localKey + SEPERATOR + eventMap.get(CONTENT_GOORU_OID));
						baseCassandraDao.saveLongValue(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), localKey, eventMap.get(CONTENT_GOORU_OID) + SEPERATOR + entry.getKey().toString(), averageR);
					}
					if (e.get(AGGMODE) != null && e.get(AGGMODE).toString().equalsIgnoreCase(SUM)) {
						baseCassandraDao.saveLongValue(ColumnFamily.MICROAGGREGATION.getColumnFamily(), localKey + SEPERATOR + key + SEPERATOR + entry.getKey().toString(), eventMap.get(GOORUID)
								.toString(), 1L);

						long sumOf = baseCassandraDao.getCount(ColumnFamily.MICROAGGREGATION.getColumnFamily(), localKey + SEPERATOR + key + SEPERATOR + entry.getKey().toString());
						baseCassandraDao.saveLongValue(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), localKey, key + SEPERATOR + entry.getKey().toString(), sumOf);
					}

				}
				if (eventMap.containsKey(TYPE) && eventMap.get(TYPE).toString().equalsIgnoreCase(STOP)) {
					String collectionStatus = COMPLETED;
					try {
						Thread.sleep(200);
					} catch (InterruptedException e1) {
						logger.error("Exception:Thread interrupted.", e);
					}
					baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), localKey, eventMap.get(CONTENT_GOORU_OID) + SEPERATOR + "completion_progress",
							collectionStatus, m);
				}
			}
		}
		try {
			m.execute();
		} catch (Exception e) {
			logger.error("Exception:Unable to save counter aggregated data.",e);
		}
	}

	/**
	 * 
	 * @param eventMap
	 * @param aggregatorJson
	 * @param keysList
	 * @param key
	 * @throws JSONException
	 */
	public void startCounters(Map<String, Object> eventMap, JSONObject aggregatorJson, List<String> keysList, String key) {
		MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
		Map<String, Object> m1 = JSONDeserializer.deserialize(aggregatorJson.toString(), new TypeReference<Map<String, Object>>() {
		});
		Set<Map.Entry<String, Object>> entrySet = m1.entrySet();

		for (Entry entry : entrySet) {
			Set<Map.Entry<String, Object>> entrySets = m1.entrySet();
			Map<String, Object> e = (Map<String, Object>) m1.get(entry.getKey());
			for (String localKey : keysList) {
				if (e.get(AGGTYPE) != null && e.get(AGGTYPE).toString().equalsIgnoreCase(COUNTER)) {
					if (!(entry.getKey() != null && entry.getKey().toString().equalsIgnoreCase(CHOICE))
							&& !(entry.getKey().toString().equalsIgnoreCase(LoaderConstants.TOTALVIEWS.getName()) && eventMap.get(TYPE).toString().equalsIgnoreCase(STOP))
							&& !eventMap.get(EVENT_NAME).toString().equalsIgnoreCase(LoaderConstants.CRAV1.getName())) {
						baseCassandraDao.generateCounter(ColumnFamily.REALTIMECOUNTER.getColumnFamily(), localKey, key + SEPERATOR + entry.getKey(),
								e.get(AGGMODE).toString().equalsIgnoreCase(AUTO) ? 1L : Long.parseLong(eventMap.get(e.get(AGGMODE)).toString()), m);
					}
				
					if (entry.getKey() != null && entry.getKey().toString().equalsIgnoreCase(CHOICE) && eventMap.get(RESOURCE_TYPE).toString().equalsIgnoreCase(QUESTION)
							&& eventMap.get(TYPE).toString().equalsIgnoreCase(STOP)) {

						int[] attemptTrySequence = TypeConverter.stringToIntArray(EMPTY_STRING + eventMap.get(ATTMPT_TRY_SEQ));
						int[] attempStatus = TypeConverter.stringToIntArray(EMPTY_STRING + eventMap.get(ATTMPT_STATUS));
						String answerStatus = null;
						int status = 0;

						status = (Integer) eventMap.get("attemptCount");
						if (status != 0) {
							status = status - 1;
						}

						if (attempStatus[0] == 1) {
							answerStatus = LoaderConstants.CORRECT.getName();
						} else if (attempStatus[0] == 0) {
							answerStatus = LoaderConstants.INCORRECT.getName();
						}

						String option = DataUtils.makeCombinedAnswerSeq(attemptTrySequence.length == 0 ? 0 : attemptTrySequence[0]);
						if (option != null && option.equalsIgnoreCase(LoaderConstants.SKIPPED.getName())) {
							answerStatus = option;
						}
						String openEndedText = eventMap.get(TEXT).toString();
						if (eventMap.get(QUESTION_TYPE).toString().equalsIgnoreCase(OE) && openEndedText != null && !openEndedText.isEmpty()) {
							option = "A";
						}
						boolean answered = this.isUserAlreadyAnswered(localKey, key);

						if (answered) {
							if (!option.equalsIgnoreCase(LoaderConstants.SKIPPED.getName())) {
								baseCassandraDao.generateCounter(ColumnFamily.REALTIMECOUNTER.getColumnFamily(), localKey, key + SEPERATOR + option,
										e.get(AGGMODE).toString().equalsIgnoreCase(AUTO) ? 1L : Long.parseLong(eventMap.get(e.get(AGGMODE)).toString()), m);
								updatePostAggregator(localKey, key + SEPERATOR + option);
							}
						} else {
							baseCassandraDao.generateCounter(ColumnFamily.REALTIMECOUNTER.getColumnFamily(), localKey, key + SEPERATOR + option, e.get(AGGMODE).toString().equalsIgnoreCase(AUTO) ? 1L
									: Long.parseLong(eventMap.get(e.get(AGGMODE)).toString()), m);
							updatePostAggregator(localKey, key + SEPERATOR + option);
						}

						if (eventMap.get(QUESTION_TYPE).toString().equalsIgnoreCase(OE) && answerStatus.equalsIgnoreCase(LoaderConstants.SKIPPED.getName())) {
							baseCassandraDao.generateCounter(ColumnFamily.REALTIMECOUNTER.getColumnFamily(), localKey, key + SEPERATOR + answerStatus, 1L, m);
							updatePostAggregator(localKey, key + SEPERATOR + answerStatus);
						} else if (answerStatus != null && !answerStatus.equalsIgnoreCase(LoaderConstants.SKIPPED.getName())) {
							baseCassandraDao.generateCounter(ColumnFamily.REALTIMECOUNTER.getColumnFamily(), localKey, key + SEPERATOR + answerStatus, 1L, m);
							updatePostAggregator(localKey, key + SEPERATOR + answerStatus);
						}
					}
					if (eventMap.get(EVENT_NAME).toString().equalsIgnoreCase(LoaderConstants.CRAV1.getName()) && e.get(AGGMODE) != null) {
						baseCassandraDao.saveLongValue(ColumnFamily.MICROAGGREGATION.getColumnFamily(), localKey, key + SEPERATOR + eventMap.get(GOORUID) + SEPERATOR + entry.getKey().toString(), e
								.get(AGGMODE).toString().equalsIgnoreCase(AUTO) ? 1L : DataUtils.formatReactionString(eventMap.get(e.get(AGGMODE)).toString()));
						baseCassandraDao.saveLongValue(ColumnFamily.MICROAGGREGATION.getColumnFamily(), localKey + SEPERATOR + key, eventMap.get(GOORUID) + SEPERATOR + entry.getKey().toString(), e
								.get(AGGMODE).toString().equalsIgnoreCase(AUTO) ? 1L : DataUtils.formatReactionString(eventMap.get(e.get(AGGMODE)).toString()));
						baseCassandraDao.saveLongValue(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), localKey, key + SEPERATOR + entry.getKey().toString(), e.get(AGGMODE).toString()
								.equalsIgnoreCase(AUTO) ? 1L : DataUtils.formatReactionString(eventMap.get(e.get(AGGMODE)).toString()));
					}
				}
			}
		}
		try {
			m.execute();
		} catch (Exception e) {
			logger.error("Exception:Unable to save real time class/collection reports data.", e);
		}
	}

	/**
	 * 
	 * @param key
	 * @param columnName
	 */
	private void updatePostAggregator(String key, String columnName) {
		Column<String> values = baseCassandraDao.readWithKeyColumn(ColumnFamily.REALTIMECOUNTER.getColumnFamily(), key, columnName, 0);
		long value = values != null ? values.getLongValue() : 0L;
		baseCassandraDao.saveLongValue(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), key, columnName, value);
	}

	/**
	 * @param key
	 *            ,metric
	 * @return long value return view count for resources
	 * @throws ConnectionException
	 *             if host is unavailable
	 */
	public long getCounterLongValue(String key, String metric) {
		ColumnList<String> result = null;
		Long count = 0L;

		result = baseCassandraDao.readWithKey(ColumnFamily.REALTIMECOUNTER.getColumnFamily(), key, 0);

		if (result != null && !result.isEmpty() && result.getColumnByName(metric) != null) {
			count = result.getColumnByName(metric).getLongValue();
		}

		return (count);
	}

	/**
	 * 
	 * @param keyValue
	 * @param eventMap
	 * @throws JSONException
	 */
	public void realTimeAggregator(String keyValue, Map<String, Object> eventMap) {

		MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
		String resourceType = eventMap.get(RESOURCE_TYPE) != null ? eventMap.get(RESOURCE_TYPE).toString() : null;
		if (eventMap.get(EVENT_NAME).toString().equalsIgnoreCase(LoaderConstants.CPV1.getName())) {
			long scoreInPercentage = 0L;
			long score = 0L;
			String collectionStatus = INPROGRESS;
			if (eventMap.get(TYPE).toString().equalsIgnoreCase(STOP)) {
				score = eventMap.get(SCORE) != null ? Long.parseLong(eventMap.get(SCORE).toString()) : 0L;
				if (questionCountInQuiz != 0L) {
					scoreInPercentage = ((score * 100 / questionCountInQuiz));
				}
			}

			baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), keyValue, COLLECTION + SEPERATOR + _GOORU_OID, eventMap.get(CONTENT_GOORU_OID).toString(), m);
			baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), keyValue, eventMap.get(CONTENT_GOORU_OID) + SEPERATOR + "completion_progress", collectionStatus, m);
			baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), keyValue, eventMap.get(CONTENT_GOORU_OID) + SEPERATOR + _QUESTION_COUNT, questionCountInQuiz, m);
			baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), keyValue, eventMap.get(CONTENT_GOORU_OID) + SEPERATOR + _SCORE_IN_PERCENTAGE, scoreInPercentage, m);
			baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), keyValue, eventMap.get(CONTENT_GOORU_OID) + SEPERATOR + SCORE, score, m);
		}

		// For user feed back
		if (eventMap.get(EVENT_NAME).toString().equalsIgnoreCase(LoaderConstants.RUFB.getName())) {
			if (eventMap.get(SESSION_ID).toString().equalsIgnoreCase("AS")) {
				String sessionKey = null;
				String sessionId = null;
				String newKey = null;
				if ((eventMap.get(CLASSPAGEGOORUOID) != null) && (!eventMap.get(CLASSPAGEGOORUOID).toString().isEmpty())) {
					sessionKey = "RS" + SEPERATOR + eventMap.get(CLASSPAGEGOORUOID) + SEPERATOR + eventMap.get(PARENT_GOORU_OID);
				} else if ((eventMap.get("classId") != null) && (!eventMap.get("classId").toString().isEmpty())) {
					sessionKey = "RS" + SEPERATOR + eventMap.get("classId") + SEPERATOR + eventMap.get(PARENT_GOORU_OID);
				} else {
					sessionKey = "RS" + SEPERATOR + eventMap.get(PARENT_GOORU_OID);
				}
				logger.info("sessionKey:" + sessionKey);
				Column<String> session = baseCassandraDao.readWithKeyColumn(ColumnFamily.MICROAGGREGATION.getColumnFamily(), sessionKey, eventMap.get(GOORUID).toString(), 0);
				sessionId = session != null ? session.getStringValue() : null;

				if ((sessionId != null) && (!sessionId.isEmpty())) {
					newKey = keyValue.replaceFirst(ALL_SESSION, sessionId + SEPERATOR);
					logger.info("newKey:" + newKey);
					baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), newKey, eventMap.get(CONTENT_GOORU_OID) + SEPERATOR + _FEEDBACK,
							eventMap.containsKey(TEXT) ? eventMap.get(TEXT).toString() : null, m);
					baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), newKey, eventMap.get(CONTENT_GOORU_OID) + SEPERATOR + _FEED_BACK_PROVIDER,
							eventMap.containsKey(PROVIDER) ? eventMap.get(PROVIDER).toString() : null, m);
					baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), newKey, eventMap.get(CONTENT_GOORU_OID) + SEPERATOR + _FEED_BACK_TIMESTAMP,
							Long.valueOf(EMPTY_STRING + eventMap.get(START_TIME)), m);
					baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), newKey, eventMap.get(CONTENT_GOORU_OID) + SEPERATOR + ACTIVE,
							eventMap.containsKey(ACTIVE) ? eventMap.get(ACTIVE).toString() : "false", m);

				}
			}

			baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), keyValue, eventMap.get(CONTENT_GOORU_OID) + SEPERATOR + _FEEDBACK,
					eventMap.containsKey(TEXT) ? eventMap.get(TEXT).toString() : null, m);
			baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), keyValue, eventMap.get(CONTENT_GOORU_OID) + SEPERATOR + _FEED_BACK_PROVIDER,
					eventMap.containsKey(PROVIDER) ? eventMap.get(PROVIDER).toString() : null, m);
			baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), keyValue, eventMap.get(CONTENT_GOORU_OID) + SEPERATOR + _FEED_BACK_TIMESTAMP,
					Long.valueOf(EMPTY_STRING + eventMap.get(START_TIME)), m);
			baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), keyValue, eventMap.get(CONTENT_GOORU_OID) + SEPERATOR + ACTIVE,
					eventMap.containsKey(ACTIVE) ? eventMap.get(ACTIVE).toString() : "false", m);
		}
		baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), keyValue, eventMap.get(CONTENT_GOORU_OID) + SEPERATOR + _GOORU_OID, eventMap.get(CONTENT_GOORU_OID)
				.toString(), m);
		baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), keyValue, _GOORU_UID, eventMap.get(GOORUID).toString(), m);
		if (eventMap.containsKey(CLASSPAGEGOORUOID) && eventMap.get(CLASSPAGEGOORUOID) != null) {
			baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), keyValue, _CLASSPAGEID, eventMap.get(CLASSPAGEGOORUOID).toString(), m);
		}

		if (eventMap.get(EVENT_NAME).toString().equalsIgnoreCase(LoaderConstants.CRPV1.getName())) {

			Column<String> totalTimeSpentValues = baseCassandraDao.readWithKeyColumn(ColumnFamily.REALTIMECOUNTER.getColumnFamily(), keyValue, eventMap.get(PARENT_GOORU_OID) + SEPERATOR
					+ LoaderConstants.TS.getName(), 0);
			long totalTimeSpent = totalTimeSpentValues != null ? totalTimeSpentValues.getLongValue() : 0L;

			Column<String> viewsValues = baseCassandraDao.readWithKeyColumn(ColumnFamily.REALTIMECOUNTER.getColumnFamily(), keyValue, eventMap.get(PARENT_GOORU_OID) + SEPERATOR
					+ LoaderConstants.TOTALVIEWS.getName(), 0);
			long views = viewsValues != null ? viewsValues.getLongValue() : 0L;

			if (views == 0L && totalTimeSpent > 0L) {
				baseCassandraDao.increamentCounter(ColumnFamily.REALTIMECOUNTER.getColumnFamily(), keyValue, eventMap.get(PARENT_GOORU_OID) + SEPERATOR + LoaderConstants.TOTALVIEWS.getName(), 1L);
				views = 1;
			}

			if (views != 0L) {
				baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), keyValue, eventMap.get(PARENT_GOORU_OID) + SEPERATOR + LoaderConstants.TS.getName(),
						totalTimeSpent, m);
				baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), keyValue, eventMap.get(PARENT_GOORU_OID) + SEPERATOR + LoaderConstants.AVGTS.getName(),
						(totalTimeSpent / views), m);
			}
		}
		if (resourceType != null && resourceType.equalsIgnoreCase(QUESTION)) {
			if (eventMap.get(TYPE).toString().equalsIgnoreCase(STOP)) {
				int[] attemptTrySequence = TypeConverter.stringToIntArray(EMPTY_STRING + eventMap.get(ATTMPT_TRY_SEQ));
				int[] attempStatus = TypeConverter.stringToIntArray(EMPTY_STRING + eventMap.get(ATTMPT_STATUS));
				// String answerStatus = null;
				int status = 0;
				status = (Integer) eventMap.get("attemptCount");
				if (status != 0) {
					status = status - 1;
				}
				int attemptStatus = attempStatus[0];
				String option = DataUtils.makeCombinedAnswerSeq(attemptTrySequence.length == 0 ? 0 : attemptTrySequence[0]);
				/*
				 * if (option != null && option.equalsIgnoreCase(LoaderConstants.SKIPPED.getName())) { answerStatus = option; }
				 */
				if (eventMap.get(QUESTION_TYPE).toString().equalsIgnoreCase(OE)) {
					String openEndedtextValue = eventMap.get(TEXT).toString();
					if (openEndedtextValue != null && !openEndedtextValue.isEmpty()) {
						option = "A";
					}
				} else {
					option = DataUtils.makeCombinedAnswerSeq(attemptTrySequence.length == 0 ? 0 : attemptTrySequence[0]);
				}
				boolean answered = this.isUserAlreadyAnswered(keyValue, eventMap.get(CONTENT_GOORU_OID).toString());
				if (answered) {
					if (!option.equalsIgnoreCase(LoaderConstants.SKIPPED.getName())) {
						m = this.addObjectForAggregator(eventMap, keyValue, m, option, attemptStatus);
					}
				} else {
					m = this.addObjectForAggregator(eventMap, keyValue, m, option, attemptStatus);
				}
				baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), keyValue, eventMap.get(CONTENT_GOORU_OID) + SEPERATOR + TYPE, eventMap.get(QUESTION_TYPE)
						.toString(), m);

			}
		}
		try {
			m.execute();
		} catch (Exception e) {
			logger.error("Exception:Unable to save real class/collection aggregated data.",e);
		}

	}

	/**
	 * 
	 * @param eventMap
	 * @param keyValue
	 * @param m
	 * @param options
	 * @param attemptStatus
	 * @return
	 * @throws JSONException
	 */
	public MutationBatch addObjectForAggregator(Map<String, Object> eventMap, String keyValue, MutationBatch m, String options, int attemptStatus) {

		String textValue = null;
		String answerObject = null;
		long scoreL = 0L;

		textValue = eventMap.get(TEXT).toString();
		if (eventMap.containsKey(ANSWER_OBECT)) {
			answerObject = eventMap.get(ANSWER_OBECT).toString();
		}
		String answers = eventMap.get(ANS).toString();
		JSONObject answersJson = null;
		try {
			answersJson = new JSONObject(answers);
		} catch (JSONException e) {
			logger.error("Exception while conversion answer object as JSON",e);
		}
		JSONArray names = answersJson.names();
		String firstChoosenAns = null;

		if (names != null && names.length() != 0) {
			try {
				firstChoosenAns = names.getString(0);
			} catch (JSONException e) {
				logger.error("Exception while conversion answer choice as JSON",e);
			}
		}

		if (eventMap.get(SCORE) != null) {
			scoreL = Long.parseLong(eventMap.get(SCORE).toString());
		}

		baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), keyValue, eventMap.get(CONTENT_GOORU_OID) + SEPERATOR + CHOICE, textValue, m);
		baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), keyValue, eventMap.get(CONTENT_GOORU_OID) + SEPERATOR + ACTIVE, "false", m);
		baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), keyValue, eventMap.get(CONTENT_GOORU_OID) + SEPERATOR + SCORE, scoreL, m);
		baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), keyValue, eventMap.get(CONTENT_GOORU_OID) + SEPERATOR + OPTIONS, options, m);
		baseCassandraDao
				.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), keyValue, eventMap.get(CONTENT_GOORU_OID) + SEPERATOR + _QUESTION_STATUS, Long.valueOf(attemptStatus), m);
		baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), keyValue, eventMap.get(CONTENT_GOORU_OID) + SEPERATOR + _ANSWER_OBECT, answerObject, m);
		baseCassandraDao.generateNonCounter(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), keyValue, eventMap.get(CONTENT_GOORU_OID) + SEPERATOR + CHOICE, firstChoosenAns, m);

		return m;
	}

	/**
	 * 
	 * @param parentIds
	 * @return
	 */
	private List<String> getClassPagesFromItems(List<String> parentIds) {
		List<String> classPageGooruOids = new ArrayList<String>();
		for (String classPageGooruOid : parentIds) {
			String type = null;
			ColumnList<String> resource = baseCassandraDao.readWithKey(ColumnFamily.DIMRESOURCE.getColumnFamily(), "GLP~" + classPageGooruOid, 0);
			if (resource != null) {
				type = resource.getColumnByName("type_name") != null ? resource.getColumnByName("type_name").getStringValue() : null;
			}

			if (type != null && type.equalsIgnoreCase(LoaderConstants.CLASSPAGE.getName())) {
				classPageGooruOids.add(classPageGooruOid);
			}
		}
		return classPageGooruOids;
	}

	/**
	 * 
	 * @param eventMap
	 * @return
	 */
	public long getQuestionCount(Map<String, Object> eventMap) {
		String contentGooruOId = eventMap.get(CONTENT_GOORU_OID).toString();
		ColumnList<String> questionLists = null;
		long totalQuestion = 0L;
		long oeQuestion = 0L;
		long updatedQuestionCount = 0L;

		questionLists = baseCassandraDao.readWithKey(ColumnFamily.QUESTIONCOUNT.getColumnFamily(), contentGooruOId, 0);

		if ((questionLists != null) && (!questionLists.isEmpty())) {
			totalQuestion = questionLists.getColumnByName("questionCount").getLongValue();
			oeQuestion = questionLists.getColumnByName("oeCount").getLongValue();
			updatedQuestionCount = totalQuestion - oeQuestion;
		}
		return updatedQuestionCount;
	}

	/**
	 * 
	 * @param eventMap
	 * @return
	 */
	public List<String> getClassPages(Map<String, Object> eventMap) {
		List<String> classPages = null;
		if (eventMap.get(EVENT_NAME).toString().equalsIgnoreCase(LoaderConstants.CPV1.getName())) {
			if (cache.containsKey(eventMap.get(EVENT_ID) + SEPERATOR + eventMap.get(CONTENT_GOORU_OID))) {
				classPages = (List<String>) cache.get(eventMap.get(EVENT_ID) + SEPERATOR + eventMap.get(CONTENT_GOORU_OID));
			} else {
				if(eventMap.containsKey(PARENT_GOORU_OID) && eventMap.get(PARENT_GOORU_OID) != null && StringUtils.isNotBlank(eventMap.get(PARENT_GOORU_OID).toString())){
					classPages = new ArrayList<String>();
					classPages.add(eventMap.get(PARENT_GOORU_OID).toString());
					cache.put(eventMap.get(EVENT_ID) + SEPERATOR + eventMap.get(CONTENT_GOORU_OID), classPages);
				}
			}
		} else if (eventMap.get(EVENT_NAME).toString().equalsIgnoreCase(LoaderConstants.CRPV1.getName()) && eventMap.containsKey(PARENT_GOORU_OID)
				&& StringUtils.isNotBlank(eventMap.get(PARENT_GOORU_OID).toString())) {
			if (cache.containsKey(eventMap.get(PARENT_EVENT_ID) + SEPERATOR + eventMap.get(PARENT_GOORU_OID))) {
				classPages = (List<String>) cache.get(eventMap.get(PARENT_EVENT_ID) + SEPERATOR + eventMap.get(PARENT_GOORU_OID));
			} else {
				ColumnList<String> collectionPlayEvent = baseCassandraDao.readWithKey(ColumnFamily.EVENTDETAIL.getColumnFamily(), eventMap.get(PARENT_EVENT_ID).toString(), 0);
				if (collectionPlayEvent != null && collectionPlayEvent.size() > 0) {
					if (collectionPlayEvent.getStringValue(_EVENT_NAME, null) != null && (collectionPlayEvent.getStringValue(_EVENT_NAME, null)).equalsIgnoreCase(LoaderConstants.CPV1.getName())) {
						if (StringUtils.isNotBlank(collectionPlayEvent.getStringValue(_PARENT_GOORU_OID, null))) {
							classPages = new ArrayList<String>();
							classPages.add(collectionPlayEvent.getStringValue(_PARENT_GOORU_OID, null));
							cache.put(eventMap.get(PARENT_EVENT_ID) + SEPERATOR + eventMap.get(PARENT_GOORU_OID), classPages);
						}
					}
				}else {
					List<String> parents = baseCassandraDao.getParentId(ColumnFamily.COLLECTIONITEM.getColumnFamily(), eventMap.get(PARENT_GOORU_OID).toString(), 0);
					if (!parents.isEmpty()) {
						classPages = this.getClassPagesFromItems(parents);
					}
				}
			}
		} else if ((eventMap.get(EVENT_NAME).toString().equalsIgnoreCase(LoaderConstants.CRAV1.getName()))) {
			ColumnList<String> R = baseCassandraDao.readWithKey(ColumnFamily.EVENTDETAIL.getColumnFamily(), eventMap.get(PARENT_EVENT_ID).toString(), 0);
			if (R != null && R.size() > 0) {
				String parentEventId = R.getStringValue(_PARENT_EVENT_ID, null);
				if (parentEventId != null && cache.containsKey(parentEventId + SEPERATOR + R.getStringValue(_PARENT_GOORU_OID, null))) {
					classPages = (List<String>) cache.get(parentEventId + SEPERATOR + R.getStringValue(_PARENT_GOORU_OID, null));
				} else {
					ColumnList<String> C = baseCassandraDao.readWithKey(ColumnFamily.EVENTDETAIL.getColumnFamily(), parentEventId, 0);
					if (C.getStringValue(_EVENT_NAME, null) != null && (C.getStringValue(_EVENT_NAME, null)).equalsIgnoreCase(LoaderConstants.CLPV1.getName())) {
						classPages = new ArrayList<String>();
						classPages.add(C.getStringValue(_CONTENT_GOORU_OID, null));
					} else if (C.getStringValue(_EVENT_NAME, null) != null && (C.getStringValue(_EVENT_NAME, null)).equalsIgnoreCase(LoaderConstants.CPV1.getName())) {
						if (StringUtils.isNotBlank(C.getStringValue(_PARENT_GOORU_OID, null))) {
							classPages = new ArrayList<String>();
							classPages.add(C.getStringValue(_PARENT_GOORU_OID, null));

						}
					}
				}
			}
		} else if (eventMap.get(EVENT_NAME).toString().equalsIgnoreCase(LoaderConstants.RUFB.getName())) {
			if (eventMap.containsKey("classId") && StringUtils.isNotBlank(eventMap.get("classId").toString())) {
				classPages = new ArrayList<String>();
				classPages.add(eventMap.get("classId").toString());
			}
		}
		return classPages;
	}

	/**
	 * 
	 * @param key
	 * @return
	 */
	private long iterateAndFindAvg(String key) {
		ColumnList<String> columns = null;
		long values = 0L;
		long count = 0L;
		long avgValues = 0L;

		columns = baseCassandraDao.readWithKey(ColumnFamily.MICROAGGREGATION.getColumnFamily(), key, 0);
		count = columns.size();
		if (columns != null && columns.size() > 0) {
			for (int i = 0; i < columns.size(); i++) {
				values += columns.getColumnByIndex(i).getLongValue();
			}
			avgValues = values / count;
		}

		return avgValues;
	}

	/**
	 * 
	 * @param localKey
	 * @param divisor
	 * @param dividend
	 * @param columnToUpdate
	 */
	private void calculateAvg(String localKey, String divisor, String dividend, String columnToUpdate) {
		long d = this.getCounterLongValue(localKey, divisor);
		if (d != 0L) {
			long average = (this.getCounterLongValue(localKey, dividend) / d);
			baseCassandraDao.saveLongValue(ColumnFamily.REALTIMEAGGREGATOR.getColumnFamily(), localKey, columnToUpdate, average);
		}
	}

	/**
	 * 
	 * @param key
	 * @param columnPrefix
	 * @return
	 */
	public boolean isUserAlreadyAnswered(String key, String columnPrefix) {
		ColumnList<String> counterColumns = baseCassandraDao.readWithKey(ColumnFamily.REALTIMECOUNTER.getColumnFamily(), key, 0);
		boolean status = false;
		long attemptCount = counterColumns.getColumnByName(columnPrefix + SEPERATOR + ATTEMPTS) != null ? counterColumns.getLongValue(columnPrefix + SEPERATOR + ATTEMPTS, null) : 0L;

		if (attemptCount > 0L) {
			status = true;
		}

		return status;

	}

	/**
	 * 
	 * @param eventMap
	 */
	public void migrationAndUpdate(Map<String, Object> eventMap) {
		List<String> classPages = this.getClassPages(eventMap);
		List<String> keysList = new ArrayList<String>();

		if (eventMap.get(MODE) != null && eventMap.get(MODE).toString().equalsIgnoreCase(STUDY)) {

			if (classPages != null && classPages.size() > 0) {
				for (String classPage : classPages) {
					boolean isOwner = baseCassandraDao.getClassPageOwnerInfo(ColumnFamily.CLASSPAGE.getColumnFamily(), eventMap.get(GOORUID).toString(), classPage, 0);
					boolean isStudent = baseCassandraDao.isUserPartOfClass(ColumnFamily.CLASSPAGE.getColumnFamily(), eventMap.get(GOORUID).toString(), classPage, 0);
					if (!isOwner && isStudent) {
						keysList.add(ALL_SESSION + classPage + SEPERATOR + eventMap.get(PARENT_GOORU_OID));
						keysList.add(ALL_SESSION + classPage + SEPERATOR + eventMap.get(PARENT_GOORU_OID) + SEPERATOR + eventMap.get(GOORUID));
					}
					keysList.add(eventMap.get(SESSION_ID) + SEPERATOR + classPage + SEPERATOR + eventMap.get(PARENT_GOORU_OID) + SEPERATOR + eventMap.get(GOORUID));
					// keysList.add(FIRSTSESSION+classPage+SEPERATOR+eventMap.get(PARENTGOORUOID)+SEPERATOR+eventMap.get(GOORUID));
				}
			}

		}

		this.completeMigration(eventMap, keysList);
	}

	/**
	 * 
	 * @param eventMap
	 * @param keysList
	 */
	public void completeMigration(Map<String, Object> eventMap, List<String> keysList) {
		MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
		if (keysList != null && keysList.size() > 0) {
			for (String keyValue : keysList) {
				ColumnList<String> counterColumns = baseCassandraDao.readWithKey(ColumnFamily.REALTIMECOUNTER.getColumnFamily(), keyValue, 0);
				long views = counterColumns.getColumnByName(eventMap.get(CONTENT_GOORU_OID) + SEPERATOR + LoaderConstants.VIEWS.getName()) != null ? counterColumns.getLongValue(
						eventMap.get(CONTENT_GOORU_OID) + SEPERATOR + LoaderConstants.VIEWS.getName(), 0L) : 0L;
				long timeSpent = counterColumns.getColumnByName(eventMap.get(CONTENT_GOORU_OID) + SEPERATOR + LoaderConstants.TS.getName()) != null ? counterColumns.getLongValue(
						eventMap.get(CONTENT_GOORU_OID) + SEPERATOR + LoaderConstants.TS.getName(), 0L) : 0L;
				logger.info("views : {} : timeSpent : {} ", views, timeSpent);

				if (views == 0L && timeSpent > 0L) {
					baseCassandraDao.generateCounter(ColumnFamily.REALTIMECOUNTER.getColumnFamily(), keyValue, eventMap.get(CONTENT_GOORU_OID) + SEPERATOR + LoaderConstants.VIEWS.getName(), 1L, m);
				}
			}
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

				this.markItemDelete(eventDataMap, eventMap);

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
		classpageMap.put("groupUId", ((eventMap.containsKey("groupUId") && eventMap.get("groupUId") != null) ? eventMap.get("groupUId").toString() : null));
		classpageMap.put(DELETED, Integer.valueOf(1));
		classpageMap.put("classId", ((eventMap.containsKey(CONTENT_GOORU_OID) && eventMap.get(CONTENT_GOORU_OID) != null) ? eventMap.get(CONTENT_GOORU_OID).toString() : null));
		classpageMap.put(USER_UID, ((eventMap.containsKey("removedGooruUId") && eventMap.get("removedGooruUId") != null) ? eventMap.get("removedGooruUId") : null));
		baseCassandraDao.updateClasspageCF(ColumnFamily.CLASSPAGE.getColumnFamily(), classpageMap);
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
			if(eventMap.get(PARENT_CONTENT_ID) != null){
				collection.setContentId(Long.valueOf(eventMap.get(PARENT_CONTENT_ID).toString()));
			}
			if(collectionMap.get(VERSION) != null){
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
	private void markItemDelete(Map<String, Object> eventDataMap, Map<String, Object> eventMap) {
		Map<String, Object> collectionItemMap = new HashMap<String, Object>();
		collectionItemMap.put(COLLECTION_ITEM_ID, (eventMap.containsKey(ITEM_ID) ? eventMap.get(ITEM_ID).toString() : null));
		collectionItemMap.put(DELETED, Integer.valueOf(1));
		rawUpdateDAO.updateCollectionItemTable(eventMap, collectionItemMap);
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
			logger.error("Unable to save resource entity for Id {} due to {}", ex);
		}
	}
}
