package org.logger.event.cassandra.loader.dao;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.ednovo.data.model.Event;
import org.ednovo.data.model.JSONDeserializer;
import org.ednovo.data.model.TypeConverter;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.json.JSONException;
import org.json.JSONObject;
import org.logger.event.cassandra.loader.CassandraConnectionProvider;
import org.logger.event.cassandra.loader.ColumnFamily;
import org.logger.event.cassandra.loader.Constants;
import org.logger.event.cassandra.loader.DataLoggerCaches;
import org.logger.event.cassandra.loader.ESIndexices;
import org.logger.event.cassandra.loader.IndexType;
import org.logger.event.cassandra.loader.LoaderConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.AccessDeniedException;

import com.google.gson.Gson;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;

public class ELSIndexerImpl extends BaseDAOCassandraImpl implements ELSIndexer, Constants {

	private static final Logger logger = LoggerFactory.getLogger(ELSIndexerImpl.class);

	private CassandraConnectionProvider connectionProvider;

	private BaseCassandraRepoImpl baseDao;

	private DataLoggerCaches loggerCache;
	
	SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd kk:mm:ss+0000");

	SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd kk:mm:ss");

	SimpleDateFormat formatter3 = new SimpleDateFormat("yyyy/MM/dd kk:mm:ss.000");

	public ELSIndexerImpl(CassandraConnectionProvider connectionProvider) {
		super(connectionProvider);
		this.connectionProvider = connectionProvider;
		this.baseDao = new BaseCassandraRepoImpl(this.connectionProvider);
		this.setLoggerCache(new DataLoggerCaches());
		this.getLoggerCache().init();
	}

	/**
	 * 
	 * @param fields
	 */
	public void indexEvents(String fields) {
		JSONObject jsonField = null;
		try {
			jsonField = new JSONObject(fields);
		} catch (JSONException e1) {
			try {
				jsonField = new JSONObject(fields.substring(14).trim());
			} catch (JSONException e2) {
				logger.error("Exception:Unable to convert JSON in the method indexEvents." + e2);
			}
		}
		if (jsonField.has(VERSION)) {
			Event events = new Gson().fromJson(fields, Event.class);
			Map<String, Object> eventMap = new HashMap<String, Object>();
			try {
				eventMap = JSONDeserializer.deserializeEventv2(events);
			} catch (JSONException e) {
				logger.error("Exception:Unable to convert JSON in the method indexEvents." + e);
			}

			eventMap.put(EVENT_NAME, events.getEventName());
			eventMap.put(EVENT_ID, events.getEventId());
			eventMap.put(EVENT_TIME, String.valueOf(events.getStartTime()));
			if (eventMap.get(CONTENT_GOORU_OID) != null) {
				eventMap = this.getTaxonomyInfo(eventMap, String.valueOf(eventMap.get(CONTENT_GOORU_OID)));
				eventMap = this.getContentInfo(eventMap, String.valueOf(eventMap.get(CONTENT_GOORU_OID)));
			}
			if (eventMap.get(GOORUID) != null) {
				eventMap = this.getUserInfo(eventMap, String.valueOf(eventMap.get(GOORUID)));
			}
			if (String.valueOf(eventMap.get(CONTENT_GOORU_OID)) != null) {
				ColumnList<String> questionList = baseDao.readWithKey(ColumnFamily.QUESTIONCOUNT.getColumnFamily(), String.valueOf(eventMap.get(CONTENT_GOORU_OID)), 0);
				if (questionList != null && questionList.size() > 0) {
					eventMap.put(QUESTION_COUNT, questionList.getColumnByName(QUESTION_COUNT) != null ? questionList.getColumnByName(QUESTION_COUNT).getLongValue() : 0L);
					eventMap.put(RESOURCE_COUNT, questionList.getColumnByName(RESOURCE_COUNT) != null ? questionList.getColumnByName(RESOURCE_COUNT).getLongValue() : 0L);
					eventMap.put(OE_COUNT, questionList.getColumnByName(OE_COUNT) != null ? questionList.getColumnByName(OE_COUNT).getLongValue() : 0L);
					eventMap.put(MC_COUNT, questionList.getColumnByName(MC_COUNT) != null ? questionList.getColumnByName(MC_COUNT).getLongValue() : 0L);

					eventMap.put(FIB_COUNT, questionList.getColumnByName(FIB_COUNT) != null ? questionList.getColumnByName(FIB_COUNT).getLongValue() : 0L);
					eventMap.put(MA_COUNT, questionList.getColumnByName(MA_COUNT) != null ? questionList.getColumnByName(MA_COUNT).getLongValue() : 0L);
					eventMap.put(TF_COUNT, questionList.getColumnByName(TF_COUNT) != null ? questionList.getColumnByName(TF_COUNT).getLongValue() : 0L);

					eventMap.put(ITEM_COUNT, questionList.getColumnByName(ITEM_COUNT) != null ? questionList.getColumnByName(ITEM_COUNT).getLongValue() : 0L);
				}
			}
			this.saveInESIndex(eventMap, ESIndexices.EVENTLOGGERINFO.getIndex() + "_" + getLoggerCache().getCache().get(INDEXING_VERSION), IndexType.EVENTDETAIL.getIndexType(), String.valueOf(eventMap.get("eventId")));
			if (eventMap.get(EVENT_NAME).toString().matches(INDEX_EVENTS) && eventMap.containsKey(CONTENT_GOORU_OID)) {
				try {
					indexResource(eventMap.get(CONTENT_GOORU_OID).toString());
					if (eventMap.containsKey(SOURCE_GOORU_OID)) {
						indexResource(eventMap.get(SOURCE_GOORU_OID).toString());
					}
					if (!eventMap.get(GOORUID).toString().equalsIgnoreCase("ANONYMOUS")) {
						getUserAndIndex(eventMap.get(GOORUID).toString());
					}
				} catch (Exception e) {
					logger.error("Exception:Unable to index events in the method indexEvents." + e);
				}
			}
		} else {
			try {
				Iterator<?> keys = jsonField.keys();
				Map<String, Object> eventMap = new HashMap<String, Object>();
				while (keys.hasNext()) {
					String key = (String) keys.next();

					eventMap.put(key, String.valueOf(jsonField.get(key)));

					/*
					 * if(key.equalsIgnoreCase("contentGooruId") || key.equalsIgnoreCase("gooruOId") || key.equalsIgnoreCase("gooruOid")){ eventMap.put("gooruOid", String.valueOf(jsonField.get(key)));
					 * }
					 */

					if (key.equalsIgnoreCase(EVENT_NAME) && (String.valueOf(jsonField.get(key)).equalsIgnoreCase("create-reaction"))) {
						eventMap.put(EVENT_NAME, "reaction.create");
					}

					if (key.equalsIgnoreCase(EVENT_NAME)
							&& (String.valueOf(jsonField.get(key)).equalsIgnoreCase("collection-play") || String.valueOf(jsonField.get(key)).equalsIgnoreCase("collection-play-dots")
									|| String.valueOf(jsonField.get(key)).equalsIgnoreCase("collections-played") || String.valueOf(jsonField.get(key)).equalsIgnoreCase("quiz-play"))) {

						eventMap.put(EVENT_NAME, "collection.play");
					}

					if (key.equalsIgnoreCase(EVENT_NAME)
							&& (String.valueOf(jsonField.get(key)).equalsIgnoreCase("signIn-google-login") || String.valueOf(jsonField.get(key)).equalsIgnoreCase("signIn-google-home") || String
									.valueOf(jsonField.get(key)).equalsIgnoreCase("anonymous-login"))) {
						eventMap.put(EVENT_NAME, "user.login");
					}

					if (key.equalsIgnoreCase(EVENT_NAME)
							&& (String.valueOf(jsonField.get(key)).equalsIgnoreCase("signUp-home") || String.valueOf(jsonField.get(key)).equalsIgnoreCase("signUp-login"))) {
						eventMap.put(EVENT_NAME, "user.register");
					}

					if (key.equalsIgnoreCase(EVENT_NAME)
							&& (String.valueOf(jsonField.get(key)).equalsIgnoreCase("collection-resource-play") || String.valueOf(jsonField.get(key)).equalsIgnoreCase("collection-resource-player")
									|| String.valueOf(jsonField.get(key)).equalsIgnoreCase("collection-resource-play-dots")
									|| String.valueOf(jsonField.get(key)).equalsIgnoreCase("collection-question-resource-play-dots")
									|| String.valueOf(jsonField.get(key)).equalsIgnoreCase("collection-resource-oe-play-dots") || String.valueOf(jsonField.get(key)).equalsIgnoreCase(
									"collection-resource-question-play-dots"))) {
						eventMap.put(EVENT_NAME, "collection.resource.play");
					}

					if (key.equalsIgnoreCase(EVENT_NAME)
							&& (String.valueOf(jsonField.get(key)).equalsIgnoreCase("resource-player") || String.valueOf(jsonField.get(key)).equalsIgnoreCase("resource-play-dots")
									|| String.valueOf(jsonField.get(key)).equalsIgnoreCase("resourceplayerstart") || String.valueOf(jsonField.get(key)).equalsIgnoreCase("resourceplayerplay")
									|| String.valueOf(jsonField.get(key)).equalsIgnoreCase("resources-played") || String.valueOf(jsonField.get(key)).equalsIgnoreCase("question-oe-play-dots") || String
									.valueOf(jsonField.get(key)).equalsIgnoreCase("question-play-dots"))) {
						eventMap.put(EVENT_NAME, "resource.play");
					}

					if (key.equalsIgnoreCase("gooruUId") || key.equalsIgnoreCase("gooruUid")) {
						eventMap.put(GOORUID, String.valueOf(jsonField.get(key)));
					}

				}
				if (eventMap.containsKey(CONTENT_GOORU_OID) && eventMap.get(CONTENT_GOORU_OID) != null) {
					eventMap = this.getTaxonomyInfo(eventMap, String.valueOf(eventMap.get(CONTENT_GOORU_OID)));
					eventMap = this.getContentInfo(eventMap, String.valueOf(eventMap.get(CONTENT_GOORU_OID)));
				}
				if (eventMap.get(GOORUID) != null) {
					eventMap = this.getUserInfo(eventMap, String.valueOf(eventMap.get(GOORUID)));
				}

				if (eventMap.get(EVENT_NAME).equals(LoaderConstants.CPV1.getName()) && eventMap.containsKey(CONTENT_GOORU_OID) && eventMap.get(CONTENT_GOORU_OID) != null) {
					ColumnList<String> questionList = baseDao.readWithKey(ColumnFamily.QUESTIONCOUNT.getColumnFamily(), String.valueOf(eventMap.get(CONTENT_GOORU_OID)), 0);
					if (questionList != null && questionList.size() > 0) {
						eventMap.put(QUESTION_COUNT, questionList.getColumnByName(QUESTION_COUNT) != null ? questionList.getColumnByName(QUESTION_COUNT).getLongValue() : 0L);
						eventMap.put(RESOURCE_COUNT, questionList.getColumnByName(RESOURCE_COUNT) != null ? questionList.getColumnByName(RESOURCE_COUNT).getLongValue() : 0L);
						eventMap.put(OE_COUNT, questionList.getColumnByName(OE_COUNT) != null ? questionList.getColumnByName(OE_COUNT).getLongValue() : 0L);
						eventMap.put(MC_COUNT, questionList.getColumnByName(MC_COUNT) != null ? questionList.getColumnByName(MC_COUNT).getLongValue() : 0L);

						eventMap.put(FIB_COUNT, questionList.getColumnByName(FIB_COUNT) != null ? questionList.getColumnByName(FIB_COUNT).getLongValue() : 0L);
						eventMap.put(MA_COUNT, questionList.getColumnByName(MA_COUNT) != null ? questionList.getColumnByName(MA_COUNT).getLongValue() : 0L);
						eventMap.put(TF_COUNT, questionList.getColumnByName(TF_COUNT) != null ? questionList.getColumnByName(TF_COUNT).getLongValue() : 0L);

						eventMap.put(ITEM_COUNT, questionList.getColumnByName(ITEM_COUNT) != null ? questionList.getColumnByName(ITEM_COUNT).getLongValue() : 0L);
					}
				}
				this.saveInESIndex(eventMap, ESIndexices.EVENTLOGGERINFO.getIndex() + "_" + getLoggerCache().getCache().get(INDEXING_VERSION), IndexType.EVENTDETAIL.getIndexType(), String.valueOf(eventMap.get("eventId")));
				if (eventMap.get(EVENT_NAME).toString().matches(INDEX_EVENTS) && eventMap.containsKey(CONTENT_GOORU_OID)) {
					indexResource(eventMap.get(CONTENT_GOORU_OID).toString());
					if (eventMap.containsKey(SOURCE_GOORU_OID)) {
						indexResource(eventMap.get(SOURCE_GOORU_OID).toString());
					}
					if (!eventMap.get(GOORUID).toString().equalsIgnoreCase("ANONYMOUS")) {
						getUserAndIndex(eventMap.get(GOORUID).toString());
					}
				}
			} catch (Exception e3) {
				throw new RuntimeException();
			}
		}

	}

	/**
	 * Indexing resources
	 * 
	 * @param ids
	 * @throws Exception 
	 */
	public void indexResource(String ids) throws Exception {
		Collection<String> idList = new ArrayList<String>();
		for (String id : ids.split(",")) {
			idList.add("GLP~" + id);
		}
		logger.debug("Indexing resources : {}", idList);
		Rows<String, String> resource = baseDao.readWithKeyList(ColumnFamily.DIMRESOURCE.getColumnFamily(), idList, 0);
	
			if (resource != null && resource.size() > 0) {
				this.getResourceAndIndex(resource);
			} else {
				throw new AccessDeniedException("Invalid Id!!");
			}
	}

	/**
	 * Get user details
	 * 
	 * @param eventMap
	 * @param gooruUId
	 * @return
	 */
	public Map<String, Object> getUserInfo(Map<String, Object> eventMap, String gooruUId) {
		Collection<String> user = new ArrayList<String>();
		user.add(gooruUId);
		ColumnList<String> eventDetailsNew = baseDao.readWithKey(ColumnFamily.EXTRACTEDUSER.getColumnFamily(), gooruUId, 0);
		if (eventDetailsNew != null && eventDetailsNew.size() > 0) {
			for (int i = 0; i < eventDetailsNew.size(); i++) {
				String columnName = eventDetailsNew.getColumnByIndex(i).getName();
				String value = eventDetailsNew.getColumnByIndex(i).getStringValue();
				if (value != null) {
					eventMap.put(columnName, value);
				}
			}
		}
		return eventMap;
	}

	/**
	 * Get content basic metadata
	 * 
	 * @param eventMap
	 * @param gooruOId
	 * @return
	 */
	public Map<String, Object> getContentInfo(Map<String, Object> eventMap, String gooruOId) {

		Set<String> contentItems = baseDao.getAllLevelParents(ColumnFamily.COLLECTIONITEM.getColumnFamily(), gooruOId, 0);
		if (contentItems != null && !contentItems.isEmpty()) {
			eventMap.put("contentItems", contentItems);
		}
		ColumnList<String> resource = baseDao.readWithKey(ColumnFamily.DIMRESOURCE.getColumnFamily(), "GLP~" + gooruOId, 0);
		if (resource != null) {
			eventMap.put("title", resource.getStringValue("title", null));
			eventMap.put("description", resource.getStringValue("description", null));
			eventMap.put("sharing", resource.getStringValue("sharing", null));
			eventMap.put("category", resource.getStringValue("category", null));
			eventMap.put("typeName", resource.getStringValue("type_name", null));
			eventMap.put("license", resource.getStringValue("license_name", null));
			eventMap.put("contentOrganizationId", resource.getStringValue("organization_uid", null));

			if (resource.getColumnByName("instructional_id") != null) {
				eventMap.put("instructionalId", resource.getColumnByName("instructional_id").getLongValue());
			}
			if (resource.getColumnByName("resource_format_id") != null) {
				eventMap.put("resourceFormatId", resource.getColumnByName("resource_format_id").getLongValue());
			}

			if (resource.getColumnByName("type_name") != null) {
				if (getLoggerCache().getResourceTypesCache().containsKey(resource.getColumnByName("type_name").getStringValue())) {
					eventMap.put("resourceTypeId", getLoggerCache().getResourceTypesCache().get(resource.getColumnByName("type_name").getStringValue()));
				}
			}
			if (resource.getColumnByName("category") != null) {
				if (getLoggerCache().getCategoryCache().containsKey(resource.getColumnByName("category").getStringValue())) {
					eventMap.put("resourceCategoryId", getLoggerCache().getCategoryCache().get(resource.getColumnByName("category").getStringValue()));
				}
			}
			ColumnList<String> questionCount = baseDao.readWithKey(ColumnFamily.QUESTIONCOUNT.getColumnFamily(), gooruOId, 0);
			if (questionCount != null && !questionCount.isEmpty()) {
				long questionCounts = questionCount.getLongValue(QUESTION_COUNT, 0L);
				eventMap.put(QUESTION_COUNT, questionCounts);
				if (questionCounts > 0L) {
					if (getLoggerCache().getResourceTypesCache().containsKey(resource.getColumnByName("type_name").getStringValue())) {
						eventMap.put("resourceTypeId", getLoggerCache().getResourceTypesCache().get(resource.getColumnByName("type_name").getStringValue()));
					}
				}
			} else {
				eventMap.put(QUESTION_COUNT, 0L);
			}
		}

		return eventMap;
	}

	/**
	 * Get taxonomy details
	 * 
	 * @param eventMap
	 * @param gooruOid
	 * @return
	 */
	public Map<String, Object> getTaxonomyInfo(Map<String, Object> eventMap, String gooruOid) {
		Collection<String> user = new ArrayList<String>();
		user.add(gooruOid);
		Map<String, String> whereColumn = new HashMap<String, String>();
		whereColumn.put("gooru_oid", gooruOid);
		Rows<String, String> eventDetailsNew = baseDao.readIndexedColumnList(ColumnFamily.DIMCONTENTCLASSIFICATION.getColumnFamily(), whereColumn, 0);
		Set<Long> subjectCode = new HashSet<Long>();
		Set<Long> courseCode = new HashSet<Long>();
		Set<Long> unitCode = new HashSet<Long>();
		Set<Long> topicCode = new HashSet<Long>();
		Set<Long> lessonCode = new HashSet<Long>();
		Set<Long> conceptCode = new HashSet<Long>();
		Set<Long> taxArray = new HashSet<Long>();

		for (Row<String, String> row : eventDetailsNew) {
			ColumnList<String> userInfo = row.getColumns();
			long root = userInfo.getColumnByName("root_node_id") != null ? userInfo.getColumnByName("root_node_id").getLongValue() : 0L;
			if (root == 20000L) {
				long value = userInfo.getColumnByName("code_id") != null ? userInfo.getColumnByName("code_id").getLongValue() : 0L;
				long depth = userInfo.getColumnByName("depth") != null ? userInfo.getColumnByName("depth").getLongValue() : 0L;
				if (value != 0L && depth == 1L) {
					subjectCode.add(value);
				} else if (depth == 2L) {
					ColumnList<String> columns = baseDao.readWithKey(ColumnFamily.EXTRACTEDCODE.getColumnFamily(), String.valueOf(value), 0);
					long subject = columns.getColumnByName("subject_code_id") != null ? columns.getColumnByName("subject_code_id").getLongValue() : 0L;
					if (subject != 0L)
						subjectCode.add(subject);
					if (value != 0L)
						courseCode.add(value);
				}

				else if (depth == 3L) {
					ColumnList<String> columns = baseDao.readWithKey(ColumnFamily.EXTRACTEDCODE.getColumnFamily(), String.valueOf(value), 0);
					long subject = columns.getColumnByName("subject_code_id") != null ? columns.getColumnByName("subject_code_id").getLongValue() : 0L;
					long course = columns.getColumnByName("course_code_id") != null ? columns.getColumnByName("course_code_id").getLongValue() : 0L;
					if (subject != 0L)
						subjectCode.add(subject);
					if (course != 0L)
						courseCode.add(course);
					if (value != 0L)
						unitCode.add(value);
				} else if (depth == 4L) {
					ColumnList<String> columns = baseDao.readWithKey(ColumnFamily.EXTRACTEDCODE.getColumnFamily(), String.valueOf(value), 0);
					long subject = columns.getColumnByName("subject_code_id") != null ? columns.getColumnByName("subject_code_id").getLongValue() : 0L;
					long course = columns.getColumnByName("course_code_id") != null ? columns.getColumnByName("course_code_id").getLongValue() : 0L;
					long unit = columns.getColumnByName("unit_code_id") != null ? columns.getColumnByName("unit_code_id").getLongValue() : 0L;
					if (subject != 0L)
						subjectCode.add(subject);
					if (course != 0L)
						courseCode.add(course);
					if (unit != 0L)
						unitCode.add(unit);
					if (value != 0L)
						topicCode.add(value);
				} else if (depth == 5L) {
					ColumnList<String> columns = baseDao.readWithKey(ColumnFamily.EXTRACTEDCODE.getColumnFamily(), String.valueOf(value), 0);
					long subject = columns.getColumnByName("subject_code_id") != null ? columns.getColumnByName("subject_code_id").getLongValue() : 0L;
					long course = columns.getColumnByName("course_code_id") != null ? columns.getColumnByName("course_code_id").getLongValue() : 0L;
					long unit = columns.getColumnByName("unit_code_id") != null ? columns.getColumnByName("unit_code_id").getLongValue() : 0L;
					long topic = columns.getColumnByName("topic_code_id") != null ? columns.getColumnByName("topic_code_id").getLongValue() : 0L;
					if (subject != 0L)
						subjectCode.add(subject);
					if (course != 0L)
						courseCode.add(course);
					if (unit != 0L)
						unitCode.add(unit);
					if (topic != 0L)
						topicCode.add(topic);
					if (value != 0L)
						lessonCode.add(value);
				} else if (depth == 6L) {
					ColumnList<String> columns = baseDao.readWithKey(ColumnFamily.EXTRACTEDCODE.getColumnFamily(), String.valueOf(value), 0);
					long subject = columns.getColumnByName("subject_code_id") != null ? columns.getColumnByName("subject_code_id").getLongValue() : 0L;
					long course = columns.getColumnByName("course_code_id") != null ? columns.getColumnByName("course_code_id").getLongValue() : 0L;
					long unit = columns.getColumnByName("unit_code_id") != null ? columns.getColumnByName("unit_code_id").getLongValue() : 0L;
					long topic = columns.getColumnByName("topic_code_id") != null ? columns.getColumnByName("topic_code_id").getLongValue() : 0L;
					long lesson = columns.getColumnByName("lesson_code_id") != null ? columns.getColumnByName("lesson_code_id").getLongValue() : 0L;
					if (subject != 0L)
						subjectCode.add(subject);
					if (course != 0L)
						courseCode.add(course);
					if (unit != 0L && unit != 0)
						unitCode.add(unit);
					if (topic != 0L)
						topicCode.add(topic);
					if (lesson != 0L)
						lessonCode.add(lesson);
					if (value != 0L)
						conceptCode.add(value);
				} else if (value != 0L) {
					taxArray.add(value);

				}
			} else {
				long value = userInfo.getColumnByName("code_id") != null ? userInfo.getColumnByName("code_id").getLongValue() : 0L;
				if (value != 0L) {
					taxArray.add(value);
				}
			}
		}
		if (subjectCode != null && !subjectCode.isEmpty())
			eventMap.put("subject", subjectCode);
		if (courseCode != null && !courseCode.isEmpty())
			eventMap.put("course", courseCode);
		if (unitCode != null && !unitCode.isEmpty())
			eventMap.put("unit", unitCode);
		if (topicCode != null && !topicCode.isEmpty())
			eventMap.put("topic", topicCode);
		if (lessonCode != null && !lessonCode.isEmpty())
			eventMap.put("lesson", lessonCode);
		if (conceptCode != null && !conceptCode.isEmpty())
			eventMap.put("concept", conceptCode);
		if (taxArray != null && !taxArray.isEmpty())
			eventMap.put("standards", taxArray);

		return eventMap;
	}

	/**
	 * 
	 * @param resource
	 * @throws ParseException
	 */
	public void getResourceAndIndex(Rows<String, String> resource) throws ParseException {

		Map<String, Object> resourceMap = new LinkedHashMap<String, Object>();

		for (int a = 0; a < resource.size(); a++) {

			ColumnList<String> columns = resource.getRowByIndex(a).getColumns();

			if (columns == null) {
				return;
			}
			if (columns.getColumnByName("gooru_oid") != null) {
				Set<String> contentItems = baseDao.getAllLevelParents(ColumnFamily.COLLECTIONITEM.getColumnFamily(), columns.getColumnByName("gooru_oid").getStringValue(), 0);
				if (contentItems != null && !contentItems.isEmpty()) {
					resourceMap.put("contentItems", contentItems);
				}

			}
			if (columns.getColumnByName("title") != null) {
				resourceMap.put("title", columns.getColumnByName("title").getStringValue());
			}
			if (columns.getColumnByName("description") != null) {
				resourceMap.put("description", columns.getColumnByName("description").getStringValue());
			}
			if (columns.getColumnByName("gooru_oid") != null) {
				resourceMap.put("gooruOid", columns.getColumnByName("gooru_oid").getStringValue());
			}
			if (columns.getColumnByName("last_modified") != null) {
				try {
					resourceMap.put("lastModified", formatter.parse(columns.getColumnByName("last_modified").getStringValue()));
				} catch (Exception e) {
					try {
						resourceMap.put("lastModified", formatter2.parse(columns.getColumnByName("last_modified").getStringValue()));
					} catch (Exception e2) {
						resourceMap.put("lastModified", formatter3.parse(columns.getColumnByName("last_modified").getStringValue()));
					}
				}
			}
			if (columns.getColumnByName("created_on") != null) {
				try {
					resourceMap.put(
							"createdOn",
							columns.getColumnByName("created_on") != null ? formatter.parse(columns.getColumnByName("created_on").getStringValue()) : formatter.parse(columns.getColumnByName(
									"last_modified").getStringValue()));
				} catch (Exception e) {
					try {
						resourceMap.put(
								"createdOn",
								columns.getColumnByName("created_on") != null ? formatter2.parse(columns.getColumnByName("created_on").getStringValue()) : formatter2.parse(columns.getColumnByName(
										"last_modified").getStringValue()));
					} catch (Exception e2) {
						resourceMap.put(
								"createdOn",
								columns.getColumnByName("created_on") != null ? formatter3.parse(columns.getColumnByName("created_on").getStringValue()) : formatter3.parse(columns.getColumnByName(
										"last_modified").getStringValue()));
					}
				}
			}
			if (columns.getColumnByName("creator_uid") != null) {
				resourceMap.put("creatorUid", columns.getColumnByName("creator_uid").getStringValue());
			}
			if (columns.getColumnByName("user_uid") != null) {
				resourceMap.put("userUid", columns.getColumnByName("user_uid").getStringValue());
			}
			if (columns.getColumnByName("record_source") != null) {
				resourceMap.put("recordSource", columns.getColumnByName("record_source").getStringValue());
			}
			if (columns.getColumnByName("sharing") != null) {
				resourceMap.put("sharing", columns.getColumnByName("sharing").getStringValue());
			}
			/*
			 * if(columns.getColumnByName("views_count") != null){ resourceMap.put("viewsCount", columns.getColumnByName("views_count").getLongValue()); }
			 */
			if (columns.getColumnByName("organization_uid") != null) {
				resourceMap.put("contentOrganizationId", columns.getColumnByName("organization_uid").getStringValue());
			}
			if (columns.getColumnByName("thumbnail") != null) {
				resourceMap.put("thumbnail", columns.getColumnByName("thumbnail").getStringValue());
			}
			if (columns.getColumnByName("instructional_id") != null) {
				resourceMap.put("instructionalId", columns.getColumnByName("instructional_id").getLongValue());
			}
			if (columns.getColumnByName("resource_format_id") != null) {
				resourceMap.put("resourceFormatId", columns.getColumnByName("resource_format_id").getLongValue());
			}
			if (columns.getColumnByName("grade") != null) {
				Set<String> gradeArray = new HashSet<String>();
				for (String gradeId : columns.getColumnByName("grade").getStringValue().split(",")) {
					gradeArray.add(gradeId);
				}
				if (gradeArray != null && gradeArray.isEmpty()) {
					resourceMap.put("grade", gradeArray);
				}
			}
			if (columns.getColumnByName("license_name") != null) {
				if (getLoggerCache().getLicenseCache().containsKey(columns.getColumnByName("license_name").getStringValue())) {
					resourceMap.put("licenseId", getLoggerCache().getLicenseCache().get(columns.getColumnByName("license_name").getStringValue()));
				}
			}
			if (columns.getColumnByName("type_name") != null) {
				if (getLoggerCache().getResourceTypesCache().containsKey(columns.getColumnByName("type_name").getStringValue())) {
					resourceMap.put("resourceTypeId", getLoggerCache().getResourceTypesCache().get(columns.getColumnByName("type_name").getStringValue()));
				}
			}
			if (columns.getColumnByName("category") != null) {
				if (getLoggerCache().getCategoryCache().containsKey(columns.getColumnByName("category").getStringValue())) {
					resourceMap.put("resourceCategoryId", getLoggerCache().getCategoryCache().get(columns.getColumnByName("category").getStringValue()));
				}
			}
			if (columns.getColumnByName("category") != null) {
				resourceMap.put("category", columns.getColumnByName("category").getStringValue());
			}
			if (columns.getColumnByName("type_name") != null) {
				resourceMap.put("typeName", columns.getColumnByName("type_name").getStringValue());
			}
			if (columns.getColumnByName("resource_format") != null) {
				resourceMap.put("resourceFormat", columns.getColumnByName("resource_format").getStringValue());
			}
			if (columns.getColumnByName("instructional") != null) {
				resourceMap.put("instructional", columns.getColumnByName("instructional").getStringValue());
			}
			if (columns.getColumnByName("gooru_oid") != null) {
				ColumnList<String> questionList = baseDao.readWithKey(ColumnFamily.QUESTIONCOUNT.getColumnFamily(), columns.getColumnByName("gooru_oid").getStringValue(), 0);

				this.getLiveCounterData("all~" + columns.getColumnByName("gooru_oid").getStringValue(), resourceMap);

				if (questionList != null && questionList.size() > 0) {
					resourceMap.put(QUESTION_COUNT, questionList.getColumnByName(QUESTION_COUNT) != null ? questionList.getColumnByName(QUESTION_COUNT).getLongValue() : 0L);
					resourceMap.put(RESOURCE_COUNT, questionList.getColumnByName(RESOURCE_COUNT) != null ? questionList.getColumnByName(RESOURCE_COUNT).getLongValue() : 0L);
					resourceMap.put(OE_COUNT, questionList.getColumnByName(OE_COUNT) != null ? questionList.getColumnByName(OE_COUNT).getLongValue() : 0L);
					resourceMap.put(MC_COUNT, questionList.getColumnByName(MC_COUNT) != null ? questionList.getColumnByName(MC_COUNT).getLongValue() : 0L);

					resourceMap.put(FIB_COUNT, questionList.getColumnByName(FIB_COUNT) != null ? questionList.getColumnByName(FIB_COUNT).getLongValue() : 0L);
					resourceMap.put(MA_COUNT, questionList.getColumnByName(MA_COUNT) != null ? questionList.getColumnByName(MA_COUNT).getLongValue() : 0L);
					resourceMap.put(TF_COUNT, questionList.getColumnByName(TF_COUNT) != null ? questionList.getColumnByName(TF_COUNT).getLongValue() : 0L);

					resourceMap.put(ITEM_COUNT, questionList.getColumnByName(ITEM_COUNT) != null ? questionList.getColumnByName(ITEM_COUNT).getLongValue() : 0L);
				}
			}
			if (columns.getColumnByName("user_uid") != null) {
				resourceMap = this.getUserInfo(resourceMap, columns.getColumnByName("user_uid").getStringValue());
			}
			if (columns.getColumnByName("gooru_oid") != null) {
				resourceMap = this.getTaxonomyInfo(resourceMap, columns.getColumnByName("gooru_oid").getStringValue());
				this.saveInESIndex(resourceMap, ESIndexices.CONTENTCATALOGINFO.getIndex() + "_" + getLoggerCache().getCache().get(INDEXING_VERSION), IndexType.DIMRESOURCE.getIndexType(), columns
						.getColumnByName("gooru_oid").getStringValue());
			}
		}
	}

	/**
	 * 
	 * @param key
	 * @param resourceMap
	 * @return
	 */
	private Map<String, Object> getLiveCounterData(String key, Map<String, Object> resourceMap) {

		ColumnList<String> vluesList = baseDao.readWithKey(ColumnFamily.LIVEDASHBOARD.getColumnFamily(), key, 0);

		if (vluesList != null && vluesList.size() > 0) {

			long views = vluesList.getColumnByName("count~views") != null ? vluesList.getColumnByName("count~views").getLongValue() : 0L;
			long totalTimespent = vluesList.getColumnByName("time_spent~total") != null ? vluesList.getColumnByName("time_spent~total").getLongValue() : 0L;

			resourceMap.put("viewsCount", views);
			resourceMap.put("totalTimespent", totalTimespent);
			resourceMap.put("avgTimespent", views != 0L ? (totalTimespent / views) : 0L);

			long ratings = vluesList.getColumnByName("count~ratings") != null ? vluesList.getColumnByName("count~ratings").getLongValue() : 0L;
			long sumOfRatings = vluesList.getColumnByName("sum~rate") != null ? vluesList.getColumnByName("sum~rate").getLongValue() : 0L;
			resourceMap.put("ratingsCount", ratings);
			resourceMap.put("sumOfRatings", sumOfRatings);
			resourceMap.put("avgRating", ratings != 0L ? (sumOfRatings / ratings) : 0L);

			long reactions = vluesList.getColumnByName("count~reactions") != null ? vluesList.getColumnByName("count~reactions").getLongValue() : 0L;
			long sumOfreactionType = vluesList.getColumnByName("sum~reactionType") != null ? vluesList.getColumnByName("sum~reactionType").getLongValue() : 0L;
			resourceMap.put("reactionsCount", reactions);
			resourceMap.put("sumOfreactionType", sumOfreactionType);
			resourceMap.put("avgReaction", reactions != 0L ? (sumOfreactionType / reactions) : 0L);

			resourceMap.put("countOfRating5", vluesList.getColumnByName("count~5") != null ? vluesList.getColumnByName("count~5").getLongValue() : 0L);
			resourceMap.put("countOfRating4", vluesList.getColumnByName("count~4") != null ? vluesList.getColumnByName("count~4").getLongValue() : 0L);
			resourceMap.put("countOfRating3", vluesList.getColumnByName("count~3") != null ? vluesList.getColumnByName("count~3").getLongValue() : 0L);
			resourceMap.put("countOfRating2", vluesList.getColumnByName("count~2") != null ? vluesList.getColumnByName("count~2").getLongValue() : 0L);
			resourceMap.put("countOfRating1", vluesList.getColumnByName("count~1") != null ? vluesList.getColumnByName("count~1").getLongValue() : 0L);

			resourceMap.put("countOfICanExplain", vluesList.getColumnByName("count~i-can-explain") != null ? vluesList.getColumnByName("count~i-can-explain").getLongValue() : 0L);
			resourceMap.put("countOfINeedHelp", vluesList.getColumnByName("count~i-need-help") != null ? vluesList.getColumnByName("count~i-need-help").getLongValue() : 0L);
			resourceMap.put("countOfIDoNotUnderstand", vluesList.getColumnByName("count~i-donot-understand") != null ? vluesList.getColumnByName("count~i-donot-understand").getLongValue() : 0L);
			resourceMap.put("countOfMeh", vluesList.getColumnByName("count~meh") != null ? vluesList.getColumnByName("count~meh").getLongValue() : 0L);
			resourceMap.put("countOfICanUnderstand", vluesList.getColumnByName("count~i-can-understand") != null ? vluesList.getColumnByName("count~i-can-understand").getLongValue() : 0L);
			resourceMap.put("copyCount", vluesList.getColumnByName("count~copy") != null ? vluesList.getColumnByName("count~copy").getLongValue() : 0L);
			resourceMap.put("sharingCount", vluesList.getColumnByName("count~share") != null ? vluesList.getColumnByName("count~share").getLongValue() : 0L);
			resourceMap.put("commentCount", vluesList.getColumnByName("count~comment") != null ? vluesList.getColumnByName("count~comment").getLongValue() : 0L);
			resourceMap.put("reviewCount", vluesList.getColumnByName("count~review") != null ? vluesList.getColumnByName("count~review").getLongValue() : 0L);
		}
		return resourceMap;
	}

	/**
	 * 
	 * @param eventMap
	 * @param indexName
	 * @param indexType
	 * @param id
	 */
	public void saveInESIndex(Map<String, Object> eventMap, String indexName, String indexType, String id) {
		XContentBuilder contentBuilder = null;
		try {

			contentBuilder = jsonBuilder().startObject();
			for (Map.Entry<String, Object> entry : eventMap.entrySet()) {
				String rowKey = null;
				if (getLoggerCache().getBeFieldName().containsKey(entry.getKey())) {
					rowKey = getLoggerCache().getBeFieldName().get(entry.getKey());
				}
				if (rowKey != null && entry.getValue() != null && !entry.getValue().equals("null") && entry.getValue() != "") {
					contentBuilder.field(rowKey,
							TypeConverter.stringToAny(String.valueOf(entry.getValue()), getLoggerCache().getFieldDataTypes().containsKey(entry.getKey()) ? getLoggerCache().getFieldDataTypes().get(entry.getKey()) : "String"));
				}
			}
			indexingES(indexName, indexType, id, contentBuilder, 0);
		} catch (Exception e) {
			logger.error("Exception:Unable to index in the method saveInESIndex." + e);
		}

	}

	/**
	 * 
	 * @param indexName
	 * @param indexType
	 * @param id
	 * @param eventMap
	 */
	public void indexCustomFieldIndex(String indexName, String indexType, String id, Map<String, Object> eventMap) {
		UpdateRequestBuilder updateRequestBuilder = getESClient().prepareUpdate(indexName, indexType, id);
		StringBuilder ctxSource = new StringBuilder();

		for (Map.Entry<String, Object> entry : eventMap.entrySet()) {
			String rowKey = null;
			if (getLoggerCache().getBeFieldName().containsKey(entry.getKey())) {
				rowKey = getLoggerCache().getBeFieldName().get(entry.getKey());
			}
			if (rowKey != null && entry.getValue() != null && !entry.getValue().equals("null") && entry.getValue() != "") {
				updateRequestBuilder.addScriptParam(rowKey, entry.getValue());
				ctxSource.append(ctxSource.length() == 0 ? "" : ";");
				ctxSource.append("ctx._source." + rowKey + "=" + rowKey);
			}
		}
		updateRequestBuilder.setScript(ctxSource.toString()).execute().actionGet();

	}

	/**
	 * 
	 * @param indexName
	 * @param indexType
	 * @param id
	 * @param contentBuilder
	 * @param retryCount
	 */
	public void indexingES(String indexName, String indexType, String id, XContentBuilder contentBuilder, int retryCount) {
		try {
			contentBuilder.field("index_updated_time", new Date());
			getESClient().prepareIndex(indexName, indexType, id).setSource(contentBuilder).execute().actionGet();
		} catch (Exception e) {
			if (retryCount < 6) {
				try {
					Thread.sleep(2000);
				} catch (InterruptedException e1) {
					logger.error("Exception:Thread interrupted in the method indexingES." + e);
				}
				logger.info("Retrying count: {}  ", retryCount);
				retryCount++;
				indexingES(indexName, indexType, id, contentBuilder, retryCount);
			} else {
				logger.error("Exception:Unable to index in the method indexingES." + e);
			}
		}

	}

	/**
	 * 
	 * @param userId
	 * @throws Exception
	 */
	public void getUserAndIndex(String userId) throws Exception {
		ColumnList<String> userInfos = baseDao.readWithKey(ColumnFamily.DIMUSER.getColumnFamily(), userId, 0);

		if (userInfos != null & userInfos.size() > 0) {
			logger.debug("INdexing user : " + userId);
			XContentBuilder contentBuilder = jsonBuilder().startObject();
			if (userInfos.getColumnByName("gooru_uid") != null) {
				contentBuilder.field("user_uid", userInfos.getColumnByName("gooru_uid").getStringValue());
				Map<String, Object> userMap = new HashMap<String, Object>();
				this.getLiveCounterData("all~" + userInfos.getColumnByName("gooru_uid").getStringValue(), userMap);
				for (Map.Entry<String, Object> entry : userMap.entrySet()) {
					String rowKey = null;
					if (getLoggerCache().getBeFieldName().containsKey(entry.getKey())) {
						rowKey = getLoggerCache().getBeFieldName().get(entry.getKey());
						contentBuilder.field(rowKey, entry.getValue());
					}
				}
			}
			if (userInfos.getColumnByName("confirm_status") != null) {
				contentBuilder.field("confirm_status", userInfos.getColumnByName("confirm_status").getLongValue());
			}
			if (userInfos.getColumnByName("registered_on") != null) {
				contentBuilder.field("registered_on", TypeConverter.stringToAny(userInfos.getColumnByName("registered_on").getStringValue(), "Date"));
			}
			if (userInfos.getColumnByName("added_by_system") != null) {
				contentBuilder.field("added_by_system", userInfos.getColumnByName("added_by_system").getLongValue());
			}
			if (userInfos.getColumnByName("account_created_type") != null) {
				contentBuilder.field("account_created_type", userInfos.getColumnByName("account_created_type").getStringValue());
			}
			if (userInfos.getColumnByName("reference_uid") != null) {
				contentBuilder.field("reference_uid", userInfos.getColumnByName("reference_uid").getStringValue());
			}
			if (userInfos.getColumnByName("email_sso") != null) {
				contentBuilder.field("email_sso", userInfos.getColumnByName("email_sso").getStringValue());
			}
			if (userInfos.getColumnByName("deactivated_on") != null) {
				contentBuilder.field("deactivated_on", TypeConverter.stringToAny(userInfos.getColumnByName("deactivated_on").getStringValue(), "Date"));
			}
			if (userInfos.getColumnByName("active") != null) {
				contentBuilder.field("active", userInfos.getColumnByName("active").getIntegerValue());
			}
			if (userInfos.getColumnByName("last_login") != null) {
				contentBuilder.field("last_login", TypeConverter.stringToAny(userInfos.getColumnByName("last_login").getStringValue(), "Date"));
			}
			if (userInfos.getColumnByName("user_role") != null) {
				Set<String> roleSet = new HashSet<String>();
				for (String role : userInfos.getColumnByName("user_role").getStringValue().split(",")) {
					roleSet.add(role);
				}
				contentBuilder.field("roles", roleSet);
			}

			if (userInfos.getColumnByName("identity_id") != null) {
				contentBuilder.field("identity_id", userInfos.getColumnByName("identity_id").getIntegerValue());
			}
			if (userInfos.getColumnByName("mail_status") != null) {
				contentBuilder.field("mail_status", userInfos.getColumnByName("mail_status").getLongValue());
			}
			if (userInfos.getColumnByName("idp_id") != null) {
				contentBuilder.field("idp_id", userInfos.getColumnByName("idp_id").getIntegerValue());
			}
			if (userInfos.getColumnByName("state") != null) {
				contentBuilder.field("state", userInfos.getColumnByName("state").getStringValue());
			}
			if (userInfos.getColumnByName("login_type") != null) {
				contentBuilder.field("login_type", userInfos.getColumnByName("login_type").getStringValue());
			}
			if (userInfos.getColumnByName("user_group_uid") != null) {
				contentBuilder.field("user_group_uid", userInfos.getColumnByName("user_group_uid").getStringValue());
			}
			if (userInfos.getColumnByName("primary_organization_uid") != null) {
				contentBuilder.field("primary_organization_uid", userInfos.getColumnByName("primary_organization_uid").getStringValue());
			}
			if (userInfos.getColumnByName("license_version") != null) {
				contentBuilder.field("license_version", userInfos.getColumnByName("license_version").getStringValue());
			}
			if (userInfos.getColumnByName("parent_id") != null) {
				contentBuilder.field("parent_id", userInfos.getColumnByName("parent_id").getLongValue());
			}
			if (userInfos.getColumnByName("lastname") != null) {
				contentBuilder.field("lastname", userInfos.getColumnByName("lastname").getStringValue());
			}
			if (userInfos.getColumnByName("account_type_id") != null) {
				contentBuilder.field("account_type_id", userInfos.getColumnByName("account_type_id").getLongValue());
			}
			if (userInfos.getColumnByName("is_deleted") != null) {
				contentBuilder.field("is_deleted", userInfos.getColumnByName("is_deleted").getIntegerValue());
			}
			if (userInfos.getColumnByName("external_id") != null) {
				contentBuilder.field("external_id", userInfos.getColumnByName("external_id").getStringValue());
			}
			if (userInfos.getColumnByName("organization_uid") != null) {
				contentBuilder.field("user_organization_uid", userInfos.getColumnByName("organization_uid").getStringValue());
			}
			if (userInfos.getColumnByName("import_code") != null) {
				contentBuilder.field("import_code", userInfos.getColumnByName("import_code").getStringValue());
			}
			if (userInfos.getColumnByName("parent_uid") != null) {
				contentBuilder.field("parent_uid", userInfos.getColumnByName("parent_uid").getStringValue());
			}
			if (userInfos.getColumnByName("security_group_uid") != null) {
				contentBuilder.field("security_group_uid", userInfos.getColumnByName("security_group_uid").getStringValue());
			}
			if (userInfos.getColumnByName("username") != null) {
				contentBuilder.field("username", userInfos.getColumnByName("username").getStringValue());
			}
			if (userInfos.getColumnByName("role_id") != null) {
				contentBuilder.field("role_id", userInfos.getColumnByName("role_id").getLongValue());
			}
			if (userInfos.getColumnByName("firstname") != null) {
				contentBuilder.field("firstname", userInfos.getColumnByName("firstname").getStringValue());
			}
			if (userInfos.getColumnByName("register_token") != null) {
				contentBuilder.field("register_token", userInfos.getColumnByName("register_token").getStringValue());
			}
			if (userInfos.getColumnByName("view_flag") != null) {
				contentBuilder.field("view_flag", userInfos.getColumnByName("view_flag").getLongValue());
			}
			if (userInfos.getColumnByName("account_uid") != null) {
				contentBuilder.field("account_uid", userInfos.getColumnByName("account_uid").getStringValue());
			}

			Collection<String> user = new ArrayList<String>();
			user.add(userId);
			Rows<String, String> eventDetailsNew = baseDao.readWithKeyList(ColumnFamily.EXTRACTEDUSER.getColumnFamily(), user, 0);
			for (Row<String, String> row : eventDetailsNew) {
				ColumnList<String> userInfo = row.getColumns();
				for (int i = 0; i < userInfo.size(); i++) {
					String columnName = userInfo.getColumnByIndex(i).getName();
					String value = userInfo.getColumnByIndex(i).getStringValue();
					if (value != null) {
						contentBuilder.field(columnName, value);
					}
				}
			}
			contentBuilder.field("index_updated_time", new Date());
			connectionProvider.getESClient().prepareIndex(ESIndexices.USERCATALOG.getIndex() + "_" + getLoggerCache().getCache().get(INDEXING_VERSION), IndexType.DIMUSER.getIndexType(), userId).setSource(contentBuilder)
					.execute().actionGet();
		} else {
			throw new AccessDeniedException("Invalid Id : " + userId);
		}

	}

	/**
	 * 
	 * @param key
	 * @throws Exception
	 */
	public void indexTaxonomy(String key) throws Exception {

		for (String id : key.split(",")) {
			ColumnList<String> sourceValues = baseDao.readWithKey(ColumnFamily.TAXONOMYCODE.getColumnFamily(), id, 0);
			if (sourceValues != null && sourceValues.size() > 0) {
				XContentBuilder contentBuilder = jsonBuilder().startObject();
				for (int i = 0; i < sourceValues.size(); i++) {
					if (getLoggerCache().getTaxonomyCodeType().get(sourceValues.getColumnByIndex(i).getName()).equalsIgnoreCase("String")) {
						contentBuilder.field(sourceValues.getColumnByIndex(i).getName(), sourceValues.getColumnByIndex(i).getStringValue());
					}
					if (getLoggerCache().getTaxonomyCodeType().get(sourceValues.getColumnByIndex(i).getName()).equalsIgnoreCase("Long")) {
						contentBuilder.field(sourceValues.getColumnByIndex(i).getName(), sourceValues.getColumnByIndex(i).getLongValue());
					}
					if (getLoggerCache().getTaxonomyCodeType().get(sourceValues.getColumnByIndex(i).getName()).equalsIgnoreCase("Integer")) {
						contentBuilder.field(sourceValues.getColumnByIndex(i).getName(), sourceValues.getColumnByIndex(i).getIntegerValue());
					}
					if (getLoggerCache().getTaxonomyCodeType().get(sourceValues.getColumnByIndex(i).getName()).equalsIgnoreCase("Double")) {
						contentBuilder.field(sourceValues.getColumnByIndex(i).getName(), sourceValues.getColumnByIndex(i).getDoubleValue());
					}
					if (getLoggerCache().getTaxonomyCodeType().get(sourceValues.getColumnByIndex(i).getName()).equalsIgnoreCase("Date")) {
						contentBuilder.field(sourceValues.getColumnByIndex(i).getName(), TypeConverter.stringToAny(sourceValues.getColumnByIndex(i).getStringValue(), "Date"));
					}
				}
				contentBuilder.field("index_updated_time", new Date());
				connectionProvider.getESClient().prepareIndex(ESIndexices.TAXONOMYCATALOG.getIndex() + "_" + getLoggerCache().getCache().get(INDEXING_VERSION), IndexType.TAXONOMYCODE.getIndexType(), id)
						.setSource(contentBuilder).execute().actionGet();
			}
		}
	}

	public DataLoggerCaches getLoggerCache() {
		return loggerCache;
	}

	public void setLoggerCache(DataLoggerCaches loggerCache) {
		this.loggerCache = loggerCache;
	}

}
