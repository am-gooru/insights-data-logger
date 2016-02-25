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

import org.apache.commons.lang.StringUtils;
import org.ednovo.data.geo.location.GeoLocation;
import org.ednovo.data.model.EventBuilder;
import org.ednovo.data.model.JSONDeserializer;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.json.JSONException;
import org.json.JSONObject;
import org.logger.event.cassandra.loader.ColumnFamilySet;
import org.logger.event.cassandra.loader.Constants;
import org.logger.event.cassandra.loader.DataLoggerCaches;
import org.logger.event.cassandra.loader.DataUtils;
import org.logger.event.cassandra.loader.ESIndexices;
import org.logger.event.cassandra.loader.IndexType;
import org.logger.event.cassandra.loader.LoaderConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.AccessDeniedException;

import com.google.gson.Gson;
import com.maxmind.geoip2.model.CityResponse;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;

public class ELSIndexerImpl extends BaseDAOCassandraImpl {

	private static final Logger LOG = LoggerFactory.getLogger(ELSIndexerImpl.class);

	private BaseCassandraRepo baseDao;

	private GeoLocation geoLocation;

	SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd kk:mm:ss+0000");

	SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd kk:mm:ss");

	SimpleDateFormat formatter3 = new SimpleDateFormat("yyyy/MM/dd kk:mm:ss.000");

	public String repoPath;

	public static Map<String, Object> licenseCache;

	public static Map<String, Object> resourceTypesCache;

	public static Map<String, Object> categoryCache;

	public static Map<String, Object> instructionalCache;

	public static Map<String, String> cache;

	public ELSIndexerImpl() {
		this.geoLocation = GeoLocation.getInstance();
		baseDao = BaseCassandraRepo.instance();
		licenseCache = new LinkedHashMap<String, Object>();
		baseDao.readAllRows(ColumnFamilySet.LICENSE.getColumnFamily(), new CallBackRows() {
			
			@Override
			public void getRows(Rows<String, String> rows) {
				for (Row<String, String> row : rows) {
					licenseCache.put(row.getKey(), row.getColumns().getLongValue("id", null));
				}				
			}
		});

		resourceTypesCache = new LinkedHashMap<String, Object>();
		baseDao.readAllRows(ColumnFamilySet.RESOURCETYPES.getColumnFamily(), new CallBackRows() {
			
			@Override
			public void getRows(Rows<String, String> rows) {
				for (Row<String, String> row : rows) {
					resourceTypesCache.put(row.getKey(), row.getColumns().getLongValue("id", null));
				}				
			}
		});
		
		
		categoryCache = new LinkedHashMap<String, Object>();
		baseDao.readAllRows(ColumnFamilySet.CATEGORY.getColumnFamily(),new CallBackRows() {
			
			@Override
			public void getRows(Rows<String, String> rows) {
				for (Row<String, String> row : rows) {
					categoryCache.put(row.getKey(), row.getColumns().getLongValue("id", null));
				}				
			}
		});

		
		instructionalCache = new LinkedHashMap<String, Object>();
		baseDao.readAllRows(ColumnFamilySet.INSTRUCTIONAL.getColumnFamily(),new CallBackRows() {
			
			@Override
			public void getRows(Rows<String, String> rows) {
				for (Row<String, String> row : rows) {
					instructionalCache.put(row.getKey(), row.getColumns().getLongValue("id", null));
				}				
			}
		});

		
		cache = new LinkedHashMap<String, String>();
		cache.put(Constants.INDEXINGVERSION, baseDao.readWithKeyColumn(ColumnFamilySet.CONFIGSETTINGS.getColumnFamily(), Constants.INDEXINGVERSION, Constants.DEFAULTCOLUMN).getStringValue());
		repoPath = baseDao.readWithKeyColumn(ColumnFamilySet.CONFIGSETTINGS.getColumnFamily(), "repo.path", Constants.DEFAULTCOLUMN).getStringValue();
	}

	/**
	 * 
	 * @param fields
	 */
	public void indexEvents(String fields) {
		if (fields != null && !fields.isEmpty()) {
			JSONObject jsonField = null;
			try {
				jsonField = new JSONObject(fields);
			} catch (JSONException e1) {
				try {
					jsonField = new JSONObject(fields.substring(14).trim());
				} catch (JSONException e2) {
					LOG.error("Exception:Unable to convert JSON in the method indexEvents.", e2);
				}
			}
			if (jsonField.has(Constants.VERSION)) {
				EventBuilder events = new Gson().fromJson(fields, EventBuilder.class);
				Map<String, Object> eventMap = new HashMap<String, Object>();

				eventMap = JSONDeserializer.deserializeEvent(events);

				eventMap.put(Constants.EVENT_NAME, events.getEventName());
				eventMap.put(Constants.EVENT_ID, events.getEventId());
				eventMap.put(Constants.EVENT_TIME, events.getStartTime());
				// eventMap.put(RESULT_COUNT, events.getHitCount());
				if (eventMap.containsKey(Constants.CONTENT_GOORU_OID) && StringUtils.isNotBlank(eventMap.get(Constants.CONTENT_GOORU_OID).toString())) {
					eventMap = this.getTaxonomyInfo(eventMap, ((String) eventMap.get(Constants.CONTENT_GOORU_OID)));
					eventMap = this.getContentInfo(eventMap, ((String) eventMap.get(Constants.CONTENT_GOORU_OID)));

					ColumnList<String> questionList = baseDao.readWithKey(ColumnFamilySet.QUESTIONCOUNT.getColumnFamily(), ((String) eventMap.get(Constants.CONTENT_GOORU_OID)));
					if (questionList != null && questionList.size() > 0) {
						eventMap.put(Constants.QUESTION_COUNT, questionList.getColumnByName(Constants.QUESTION_COUNT) != null ? questionList.getColumnByName(Constants.QUESTION_COUNT).getLongValue()
								: 0L);
						eventMap.put(Constants.RESOURCE_COUNT, questionList.getColumnByName(Constants.RESOURCE_COUNT) != null ? questionList.getColumnByName(Constants.RESOURCE_COUNT).getLongValue()
								: 0L);
						eventMap.put(Constants.OE_COUNT, questionList.getColumnByName(Constants.OE_COUNT) != null ? questionList.getColumnByName(Constants.OE_COUNT).getLongValue() : 0L);
						eventMap.put(Constants.MC_COUNT, questionList.getColumnByName(Constants.MC_COUNT) != null ? questionList.getColumnByName(Constants.MC_COUNT).getLongValue() : 0L);

						eventMap.put(Constants.FIB_COUNT, questionList.getColumnByName(Constants.FIB_COUNT) != null ? questionList.getColumnByName(Constants.FIB_COUNT).getLongValue() : 0L);
						eventMap.put(Constants.MA_COUNT, questionList.getColumnByName(Constants.MA_COUNT) != null ? questionList.getColumnByName(Constants.MA_COUNT).getLongValue() : 0L);
						eventMap.put(Constants.TF_COUNT, questionList.getColumnByName(Constants.TF_COUNT) != null ? questionList.getColumnByName(Constants.TF_COUNT).getLongValue() : 0L);

						eventMap.put(Constants.ITEM_COUNT, questionList.getColumnByName(Constants.ITEM_COUNT) != null ? questionList.getColumnByName(Constants.ITEM_COUNT).getLongValue() : 0L);
					}

				}
				if (eventMap.get(Constants.GOORUID) != null) {
					eventMap = this.getUserInfo(eventMap, ((String) eventMap.get(Constants.GOORUID)));
				}
				String userIp = null;
				if (eventMap.containsKey(Constants.USER_IP) && eventMap.get(Constants.USER_IP) != null) {
					userIp = ((String) eventMap.get(Constants.USER_IP)).split(Constants.COMMA)[0];
				}
				if (userIp != null) {
					CityResponse cityResponse = geoLocation.getGeoResponse(userIp);
					if (cityResponse != null) {
						if (StringUtils.isNotBlank(cityResponse.getCity().getName())) {
							eventMap.put(Constants.CITY, cityResponse.getCity().getName());
						}
						if (cityResponse.getLocation().getLatitude() != null) {
							eventMap.put(Constants.LATITUDE, cityResponse.getLocation().getLatitude());
						}
						if (cityResponse.getLocation().getLongitude() != null) {
							eventMap.put(Constants.LONGITUDE, cityResponse.getLocation().getLongitude());
						}
						if (StringUtils.isNotBlank(cityResponse.getMostSpecificSubdivision().getName())) {
							eventMap.put(Constants.REGION, cityResponse.getMostSpecificSubdivision().getName());
						}
					}
				}

				if (eventMap.get("eventId") != null) {
					this.saveInESIndex(eventMap, ESIndexices.EVENTLOGGERINFO.getIndex() + "_" + DataLoggerCaches.getCache().get(Constants.INDEXING_VERSION), IndexType.EVENTDETAIL.getIndexType(),
							((String) eventMap.get("eventId")));
				}
				if (eventMap.get(Constants.EVENT_NAME) != null && eventMap.get(Constants.EVENT_NAME).toString().matches(Constants.INDEX_EVENTS) && eventMap.containsKey(Constants.CONTENT_GOORU_OID)
						&& eventMap.get(Constants.CONTENT_GOORU_OID) != null) {
					try {
						indexResource(eventMap.get(Constants.CONTENT_GOORU_OID).toString());
						if (eventMap.containsKey(Constants.SOURCE_GOORU_OID)) {
							indexResource(eventMap.get(Constants.SOURCE_GOORU_OID).toString());
						}
						if (eventMap.containsKey(Constants.GOORUID) && eventMap.get(Constants.GOORUID) != null && !eventMap.get(Constants.GOORUID).toString().equalsIgnoreCase("ANONYMOUS")) {
							getUserAndIndex(eventMap.get(Constants.GOORUID).toString());
						}
					} catch (Exception e) {
						LOG.error("Exception:Unable to index events in the method indexEvents.", e);
					}
				}
			} else {
				try {
					Iterator<?> keys = jsonField.keys();
					Map<String, Object> eventMap = new HashMap<String, Object>();
					while (keys.hasNext()) {
						String key = String.valueOf(keys.next());

						eventMap.put(key, String.valueOf(jsonField.get(key)));

						/*
						 * if(key.equalsIgnoreCase("contentGooruId") || key.equalsIgnoreCase("gooruOId") || key.equalsIgnoreCase("gooruOid")){ eventMap.put("gooruOid", ((String)jsonField.get(key))); }
						 */

						if (key.equalsIgnoreCase(Constants.EVENT_NAME) && (((String) jsonField.get(key)).equalsIgnoreCase("create-reaction"))) {
							eventMap.put(Constants.EVENT_NAME, "reaction.create");
						}

						if (key.equalsIgnoreCase(Constants.EVENT_NAME)
								&& (((String) jsonField.get(key)).equalsIgnoreCase("collection-play") || ((String) jsonField.get(key)).equalsIgnoreCase("collection-play-dots")
										|| ((String) jsonField.get(key)).equalsIgnoreCase("collections-played") || ((String) jsonField.get(key)).equalsIgnoreCase("quiz-play"))) {

							eventMap.put(Constants.EVENT_NAME, "collection.play");
						}

						if (key.equalsIgnoreCase(Constants.EVENT_NAME)
								&& (((String) jsonField.get(key)).equalsIgnoreCase("signIn-google-login") || ((String) jsonField.get(key)).equalsIgnoreCase("signIn-google-home") || String.valueOf(
										jsonField.get(key)).equalsIgnoreCase("anonymous-login"))) {
							eventMap.put(Constants.EVENT_NAME, "user.login");
						}

						if (key.equalsIgnoreCase(Constants.EVENT_NAME)
								&& (((String) jsonField.get(key)).equalsIgnoreCase("signUp-home") || ((String) jsonField.get(key)).equalsIgnoreCase("signUp-login"))) {
							eventMap.put(Constants.EVENT_NAME, "user.register");
						}

						if (key.equalsIgnoreCase(Constants.EVENT_NAME)
								&& (((String) jsonField.get(key)).equalsIgnoreCase("collection-resource-play") || ((String) jsonField.get(key)).equalsIgnoreCase("collection-resource-player")
										|| ((String) jsonField.get(key)).equalsIgnoreCase("collection-resource-play-dots")
										|| ((String) jsonField.get(key)).equalsIgnoreCase("collection-question-resource-play-dots")
										|| ((String) jsonField.get(key)).equalsIgnoreCase("collection-resource-oe-play-dots") || ((String) jsonField.get(key))
										.equalsIgnoreCase("collection-resource-question-play-dots"))) {
							eventMap.put(Constants.EVENT_NAME, "collection.resource.play");
						}

						if (key.equalsIgnoreCase(Constants.EVENT_NAME)
								&& (((String) jsonField.get(key)).equalsIgnoreCase("resource-player") || ((String) jsonField.get(key)).equalsIgnoreCase("resource-play-dots")
										|| ((String) jsonField.get(key)).equalsIgnoreCase("resourceplayerstart") || ((String) jsonField.get(key)).equalsIgnoreCase("resourceplayerplay")
										|| ((String) jsonField.get(key)).equalsIgnoreCase("resources-played") || ((String) jsonField.get(key)).equalsIgnoreCase("question-oe-play-dots") || String
										.valueOf(jsonField.get(key)).equalsIgnoreCase("question-play-dots"))) {
							eventMap.put(Constants.EVENT_NAME, "resource.play");
						}

						if (key.equalsIgnoreCase("gooruUId") || key.equalsIgnoreCase("gooruUid")) {
							eventMap.put(Constants.GOORUID, ((String) jsonField.get(key)));
						}

					}
					if (eventMap.containsKey(Constants.CONTENT_GOORU_OID) && eventMap.get(Constants.CONTENT_GOORU_OID) != null) {
						eventMap = this.getTaxonomyInfo(eventMap, ((String) eventMap.get(Constants.CONTENT_GOORU_OID)));
						eventMap = this.getContentInfo(eventMap, ((String) eventMap.get(Constants.CONTENT_GOORU_OID)));
					}
					if (eventMap.get(Constants.GOORUID) != null) {
						eventMap = this.getUserInfo(eventMap, ((String) eventMap.get(Constants.GOORUID)));
					}

					if (eventMap.get(Constants.EVENT_NAME).equals(LoaderConstants.CPV1.getName()) && eventMap.containsKey(Constants.CONTENT_GOORU_OID)
							&& StringUtils.isNotBlank(eventMap.get(Constants.CONTENT_GOORU_OID).toString())) {
						ColumnList<String> questionList = baseDao.readWithKey(ColumnFamilySet.QUESTIONCOUNT.getColumnFamily(), ((String) eventMap.get(Constants.CONTENT_GOORU_OID)));
						if (questionList != null && questionList.size() > 0) {
							eventMap.put(Constants.QUESTION_COUNT, questionList.getColumnByName(Constants.QUESTION_COUNT) != null ? questionList.getColumnByName(Constants.QUESTION_COUNT)
									.getLongValue() : 0L);
							eventMap.put(Constants.RESOURCE_COUNT, questionList.getColumnByName(Constants.RESOURCE_COUNT) != null ? questionList.getColumnByName(Constants.RESOURCE_COUNT)
									.getLongValue() : 0L);
							eventMap.put(Constants.OE_COUNT, questionList.getColumnByName(Constants.OE_COUNT) != null ? questionList.getColumnByName(Constants.OE_COUNT).getLongValue() : 0L);
							eventMap.put(Constants.MC_COUNT, questionList.getColumnByName(Constants.MC_COUNT) != null ? questionList.getColumnByName(Constants.MC_COUNT).getLongValue() : 0L);

							eventMap.put(Constants.FIB_COUNT, questionList.getColumnByName(Constants.FIB_COUNT) != null ? questionList.getColumnByName(Constants.FIB_COUNT).getLongValue() : 0L);
							eventMap.put(Constants.MA_COUNT, questionList.getColumnByName(Constants.MA_COUNT) != null ? questionList.getColumnByName(Constants.MA_COUNT).getLongValue() : 0L);
							eventMap.put(Constants.TF_COUNT, questionList.getColumnByName(Constants.TF_COUNT) != null ? questionList.getColumnByName(Constants.TF_COUNT).getLongValue() : 0L);

							eventMap.put(Constants.ITEM_COUNT, questionList.getColumnByName(Constants.ITEM_COUNT) != null ? questionList.getColumnByName(Constants.ITEM_COUNT).getLongValue() : 0L);
						}
					}
					if (eventMap.get("eventId") != null) {
						this.saveInESIndex(eventMap, ESIndexices.EVENTLOGGERINFO.getIndex() + "_" + DataLoggerCaches.getCache().get(Constants.INDEXING_VERSION), IndexType.EVENTDETAIL.getIndexType(),
								((String) eventMap.get("eventId")));
					}
					if (eventMap.get(Constants.EVENT_NAME).toString().matches(Constants.INDEX_EVENTS) && eventMap.containsKey(Constants.CONTENT_GOORU_OID)) {
						indexResource(eventMap.get(Constants.CONTENT_GOORU_OID).toString());
						if (eventMap.containsKey(Constants.SOURCE_GOORU_OID)) {
							indexResource(eventMap.get(Constants.SOURCE_GOORU_OID).toString());
						}
						if (!eventMap.get(Constants.GOORUID).toString().equalsIgnoreCase("ANONYMOUS")) {
							getUserAndIndex(eventMap.get(Constants.GOORUID).toString());
						}
					}
				} catch (Exception e3) {
					LOG.error("Exception : ", e3);
				}
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
		for (String id : ids.split(Constants.COMMA)) {
			LOG.info("Indexing resources : {}", id);
			ColumnList<String> resource = baseDao.readWithKey(ColumnFamilySet.RESOURCE.getColumnFamily(), id);
			if (resource != null && resource.size() > 0) {
				this.getResourceAndIndex(resource, id);
			} else {
				LOG.info("Indexing resources from dim_resource : {}", id);
				ColumnList<String> dimResource = baseDao.readWithKey(ColumnFamilySet.DIMRESOURCE.getColumnFamily(), "GLP~".concat(id));
				if (dimResource != null && dimResource.size() > 0) {
					this.getDimResourceAndIndex(dimResource, id);
				} else {
					LOG.error("Invalid Id !!" + id);
				}
			}
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
		try {
			ColumnList<String> eventDetailsNew = baseDao.readWithKey(ColumnFamilySet.EXTRACTEDUSER.getColumnFamily(), gooruUId);
			if (eventDetailsNew != null && eventDetailsNew.size() > 0) {
				for (int i = 0; i < eventDetailsNew.size(); i++) {
					String columnName = eventDetailsNew.getColumnByIndex(i).getName();
					String value = eventDetailsNew.getColumnByIndex(i).getStringValue();
					if (!columnName.equalsIgnoreCase(Constants.GRADE) && !columnName.equalsIgnoreCase(Constants.TAXONOMY_IDS) && value != null) {
						eventMap.put(columnName, value);
					}
				}
			}
		} catch (Exception e) {
			LOG.error("Exception while get user details." + e);
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

		Set<String> contentItems = baseDao.getAllLevelParents(ColumnFamilySet.COLLECTIONITEM.getColumnFamily(), gooruOId, 0);
		if (contentItems != null && !contentItems.isEmpty()) {
			eventMap.put("contentItems", contentItems);
		}
		ColumnList<String> resource = baseDao.readWithKey(ColumnFamilySet.RESOURCE.getColumnFamily(), gooruOId);
		if (resource != null) {
			if (resource.getColumnByName("title") != null) {
				eventMap.put("title", resource.getColumnByName("title").getStringValue());
			}
			if (resource.getColumnByName("description") != null) {
				eventMap.put("description", resource.getColumnByName("description").getStringValue());
			}
			if (resource.getColumnByName("sharing") != null) {
				eventMap.put("sharing", resource.getColumnByName("sharing").getStringValue());
			}
			if (resource.getColumnByName("organization.partyUid") != null) {
				eventMap.put("contentOrganizationId", resource.getColumnByName("organization.partyUid").getStringValue());
			}

			if (resource.getColumnByName("grade") != null) {
				Set<String> gradeArray = new HashSet<String>();
				for (String gradeId : resource.getColumnByName("grade").getStringValue().split(",")) {
					gradeArray.add(gradeId);
				}
				if (gradeArray != null && !gradeArray.isEmpty()) {
					eventMap.put("grade1", gradeArray);
				}
			}
			if (resource.getColumnByName("license.name") != null) {
				if (DataLoggerCaches.getLicenseCache().containsKey(resource.getColumnByName("license.name").getStringValue())) {
					eventMap.put("licenseId", DataLoggerCaches.getLicenseCache().get(resource.getColumnByName("license.name").getStringValue()));
				}
			}
			if (resource.getColumnByName("resourceType") != null) {
				if (DataLoggerCaches.getResourceTypesCache().containsKey(resource.getColumnByName("resourceType").getStringValue())) {
					eventMap.put("resourceTypeId", DataLoggerCaches.getResourceTypesCache().get(resource.getColumnByName("resourceType").getStringValue()));
				}
				eventMap.put("typeName", resource.getColumnByName("resourceType").getStringValue());
			}
			if (resource.getColumnByName("category") != null) {
				if (DataLoggerCaches.getCategoryCache().containsKey(resource.getColumnByName("category").getStringValue())) {
					eventMap.put("resourceCategoryId", DataLoggerCaches.getCategoryCache().get(resource.getColumnByName("category").getStringValue()));
				}
				eventMap.put("category", resource.getColumnByName("category").getStringValue());
			}
			if (resource.getColumnByName("resourceFormat") != null) {
				eventMap.put("resourceFormat", resource.getColumnByName("resourceFormat").getStringValue());
				eventMap.put("resourceFormatId", DataLoggerCaches.getResourceFormatCache().get(resource.getColumnByName("resourceFormat").getStringValue()));
			}
			if (resource.getColumnByName("instructional") != null) {
				eventMap.put("instructional", resource.getColumnByName("instructional").getStringValue());
				eventMap.put("instructionalId", DataLoggerCaches.getInstructionalCache().get(resource.getColumnByName("instructional").getStringValue()));
			}

			ColumnList<String> questionCount = baseDao.readWithKey(ColumnFamilySet.QUESTIONCOUNT.getColumnFamily(), gooruOId);
			if (questionCount != null && !questionCount.isEmpty()) {
				long questionCounts = questionCount.getLongValue(Constants.QUESTION_COUNT, 0L);
				eventMap.put(Constants.QUESTION_COUNT, questionCounts);
			} else {
				eventMap.put(Constants.QUESTION_COUNT, 0L);
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
		Rows<String, String> eventDetailsNew = baseDao.readIndexedColumnList(ColumnFamilySet.DIMCONTENTCLASSIFICATION.getColumnFamily(), whereColumn);
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
					ColumnList<String> columns = baseDao.readWithKey(ColumnFamilySet.EXTRACTEDCODE.getColumnFamily(), String.valueOf(value));
					long subject = columns.getColumnByName("subject_code_id") != null ? columns.getColumnByName("subject_code_id").getLongValue() : 0L;
					if (subject != 0L)
						subjectCode.add(subject);
					if (value != 0L)
						courseCode.add(value);
				}

				else if (depth == 3L) {
					ColumnList<String> columns = baseDao.readWithKey(ColumnFamilySet.EXTRACTEDCODE.getColumnFamily(), String.valueOf(value));
					long subject = columns.getColumnByName("subject_code_id") != null ? columns.getColumnByName("subject_code_id").getLongValue() : 0L;
					long course = columns.getColumnByName("course_code_id") != null ? columns.getColumnByName("course_code_id").getLongValue() : 0L;
					if (subject != 0L)
						subjectCode.add(subject);
					if (course != 0L)
						courseCode.add(course);
					if (value != 0L)
						unitCode.add(value);
				} else if (depth == 4L) {
					ColumnList<String> columns = baseDao.readWithKey(ColumnFamilySet.EXTRACTEDCODE.getColumnFamily(), String.valueOf(value));
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
					ColumnList<String> columns = baseDao.readWithKey(ColumnFamilySet.EXTRACTEDCODE.getColumnFamily(), String.valueOf(value));
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
					ColumnList<String> columns = baseDao.readWithKey(ColumnFamilySet.EXTRACTEDCODE.getColumnFamily(), String.valueOf(value));
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
	public void getResourceAndIndex(ColumnList<String> columns, String gooruOid) throws ParseException {

		Map<String, Object> resourceMap = new LinkedHashMap<String, Object>();

		if (!columns.isEmpty()) {

			if (columns.getColumnByName("title") != null) {
				resourceMap.put("title", columns.getColumnByName("title").getStringValue());
			}
			if (columns.getColumnByName("description") != null) {
				resourceMap.put("description", columns.getColumnByName("description").getStringValue());
			}
			if (columns.getColumnByName("lastModified") != null) {
				resourceMap.put("lastModified", columns.getColumnByName("lastModified").getDateValue());
			}
			if (columns.getColumnByName("createdOn") != null) {
				resourceMap.put("createdOn", columns.getColumnByName("createdOn").getDateValue());
			}
			if (columns.getColumnByName("creator.userUid") != null) {
				resourceMap.put("creatorUid", columns.getColumnByName("creator.userUid").getStringValue());
			}
			if (columns.getColumnByName("owner.userUid") != null) {
				resourceMap.put("userUid", columns.getColumnByName("owner.userUid").getStringValue());
			}
			if (columns.getColumnByName("recordSource") != null) {
				resourceMap.put("recordSource", columns.getColumnByName("recordSource").getStringValue());
			}
			if (columns.getColumnByName("sharing") != null) {
				resourceMap.put("sharing", columns.getColumnByName("sharing").getStringValue());
			}
			if (columns.getColumnByName("organization.partyUid") != null) {
				resourceMap.put("contentOrganizationId", columns.getColumnByName("organization.partyUid").getStringValue());
			}
			if (StringUtils.isNotBlank(columns.getStringValue("thumbnail", Constants.EMPTY))) {
				if (columns.getColumnByName("thumbnail").getStringValue().startsWith("http") || columns.getColumnByName("thumbnail").getStringValue().startsWith("https")) {
					resourceMap.put("thumbnail", columns.getColumnByName("thumbnail").getStringValue());
				} else {
					resourceMap
							.put("thumbnail", DataLoggerCaches.getREPOPATH() + "/" + columns.getStringValue("folder", Constants.EMPTY) + "/" + columns.getColumnByName("thumbnail").getStringValue());
				}
			}
			if (columns.getColumnByName("grade") != null) {
				Set<String> gradeArray = new HashSet<String>();
				for (String gradeId : columns.getColumnByName("grade").getStringValue().split(",")) {
					gradeArray.add(gradeId);
				}
				if (gradeArray != null && !gradeArray.isEmpty()) {
					resourceMap.put("grade1", gradeArray);
				}
			}
			if (columns.getColumnByName("license.name") != null) {
				if (DataLoggerCaches.getLicenseCache().containsKey(columns.getColumnByName("license.name").getStringValue())) {
					resourceMap.put("licenseId", DataLoggerCaches.getLicenseCache().get(columns.getColumnByName("license.name").getStringValue()));
				}
			}
			if (columns.getColumnByName("resourceType") != null) {
				if (DataLoggerCaches.getResourceTypesCache().containsKey(columns.getColumnByName("resourceType").getStringValue())) {
					resourceMap.put("resourceTypeId", DataLoggerCaches.getResourceTypesCache().get(columns.getColumnByName("resourceType").getStringValue()));
				}
				resourceMap.put("typeName", columns.getColumnByName("resourceType").getStringValue());
			}
			if (columns.getColumnByName("category") != null) {
				if (DataLoggerCaches.getCategoryCache().containsKey(columns.getColumnByName("category").getStringValue())) {
					resourceMap.put("resourceCategoryId", DataLoggerCaches.getCategoryCache().get(columns.getColumnByName("category").getStringValue()));
				}
				resourceMap.put("category", columns.getColumnByName("category").getStringValue());
			}
			if (columns.getColumnByName("resourceFormat") != null) {
				resourceMap.put("resourceFormat", columns.getColumnByName("resourceFormat").getStringValue());
				resourceMap.put("resourceFormatId", DataLoggerCaches.getResourceFormatCache().get(columns.getColumnByName("resourceFormat").getStringValue()));
			}
			if (columns.getColumnByName("instructional") != null) {
				resourceMap.put("instructional", columns.getColumnByName("instructional").getStringValue());
				resourceMap.put("instructionalId", DataLoggerCaches.getInstructionalCache().get(columns.getColumnByName("instructional").getStringValue()));
			}
			if (columns.getColumnByName("owner.userUid") != null) {
				resourceMap = this.getUserInfo(resourceMap, columns.getColumnByName("owner.userUid").getStringValue());
			}
			if (StringUtils.isNotBlank(gooruOid)) {
				Set<String> contentItems = baseDao.getAllLevelParents(ColumnFamilySet.COLLECTIONITEM.getColumnFamily(), gooruOid, 0);
				if (!contentItems.isEmpty()) {
					resourceMap.put("contentItems", contentItems);
				}
				resourceMap.put("gooruOid", gooruOid);

				ColumnList<String> questionList = baseDao.readWithKey(ColumnFamilySet.QUESTIONCOUNT.getColumnFamily(), gooruOid);

				this.getLiveCounterData("all~" + gooruOid, resourceMap);

				if (questionList != null && questionList.size() > 0) {
					resourceMap.put("questionCount", questionList.getColumnByName("questionCount") != null ? questionList.getColumnByName("questionCount").getLongValue() : 0L);
					resourceMap.put("resourceCount", questionList.getColumnByName("resourceCount") != null ? questionList.getColumnByName("resourceCount").getLongValue() : 0L);
					resourceMap.put("oeCount", questionList.getColumnByName("oeCount") != null ? questionList.getColumnByName("oeCount").getLongValue() : 0L);
					resourceMap.put("mcCount", questionList.getColumnByName("mcCount") != null ? questionList.getColumnByName("mcCount").getLongValue() : 0L);

					resourceMap.put("fibCount", questionList.getColumnByName("fibCount") != null ? questionList.getColumnByName("fibCount").getLongValue() : 0L);
					resourceMap.put("maCount", questionList.getColumnByName("maCount") != null ? questionList.getColumnByName("maCount").getLongValue() : 0L);
					resourceMap.put("tfCount", questionList.getColumnByName("tfCount") != null ? questionList.getColumnByName("tfCount").getLongValue() : 0L);

					resourceMap.put("itemCount", questionList.getColumnByName("itemCount") != null ? questionList.getColumnByName("itemCount").getLongValue() : 0L);
				}
				resourceMap = this.getTaxonomyInfo(resourceMap, gooruOid);
				this.saveInESIndex(resourceMap, ESIndexices.CONTENTCATALOGINFO.getIndex() + "_" + DataLoggerCaches.getCache().get(Constants.INDEXINGVERSION), IndexType.DIMRESOURCE.getIndexType(),
						gooruOid);
			}
		}
	}

	public void getDimResourceAndIndex(ColumnList<String> columns, String gooruOid) throws ParseException {

		Map<String, Object> resourceMap = new LinkedHashMap<String, Object>();
		/*
		 * for (int a = 0; a < resource.size(); a++) {
		 * 
		 * ColumnList<String> columns = resource.getRowByIndex(a).getColumns();
		 */
		if (columns.getColumnByName("title") != null) {
			resourceMap.put("title", columns.getColumnByName("title").getStringValue());
		}
		if (columns.getColumnByName("description") != null) {
			resourceMap.put("description", columns.getColumnByName("description").getStringValue());
		}
		if (columns.getColumnByName("last_modified") != null) {
			resourceMap.put("lastModified", columns.getColumnByName("last_modified").getStringValue());
		}
		if (columns.getColumnByName("created_on") != null) {
			resourceMap.put("createdOn", columns.getColumnByName("created_on").getStringValue());
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
		if (columns.getColumnByName("organization_uid") != null) {
			resourceMap.put("contentOrganizationId", columns.getColumnByName("organization_uid").getStringValue());
		}
		if (columns.getColumnByName("thumbnail") != null && StringUtils.isNotBlank(columns.getColumnByName("thumbnail").getStringValue())) {
			if (columns.getColumnByName("thumbnail").getStringValue().startsWith("http") || columns.getColumnByName("thumbnail").getStringValue().startsWith("https")) {
				resourceMap.put("thumbnail", columns.getColumnByName("thumbnail").getStringValue());
			} else {
				resourceMap.put("thumbnail", repoPath + "/" + columns.getColumnByName("folder").getStringValue() + "/" + columns.getColumnByName("thumbnail").getStringValue());
			}
		}
		if (columns.getColumnByName("grade") != null) {
			Set<String> gradeArray = new HashSet<String>();
			for (String gradeId : columns.getColumnByName("grade").getStringValue().split(",")) {
				if (!gradeId.equalsIgnoreCase("null")) {
					gradeArray.add(gradeId);
				}
			}
			if (gradeArray != null && !gradeArray.isEmpty()) {
				resourceMap.put("grade1", gradeArray);
			}
		}
		if (columns.getColumnByName("license_name") != null) {
			if (licenseCache.containsKey(columns.getColumnByName("license_name").getStringValue())) {
				resourceMap.put("licenseId", licenseCache.get(columns.getColumnByName("license_name").getStringValue()));
			}
		}
		if (columns.getColumnByName("type_name") != null) {
			resourceMap.put("typeName", columns.getColumnByName("type_name").getStringValue());
			if (resourceTypesCache.containsKey(columns.getColumnByName("type_name").getStringValue())) {
				resourceMap.put("resourceTypeId", resourceTypesCache.get(columns.getColumnByName("type_name").getStringValue()));
			}
		}
		if (columns.getColumnByName("category") != null) {
			resourceMap.put("category", columns.getColumnByName("category").getStringValue());
			if (categoryCache.containsKey(columns.getColumnByName("category").getStringValue())) {
				resourceMap.put("resourceCategoryId", categoryCache.get(columns.getColumnByName("category").getStringValue()));
			}
		}
		if (columns.getColumnByName("resource_format") != null) {
			resourceMap.put("resourceFormat", columns.getColumnByName("resource_format").getStringValue());
			resourceMap.put("resourceFormatId", DataUtils.getResourceFormatId(columns.getColumnByName("resource_format").getStringValue()));
		}
		if (columns.getColumnByName("instructional") != null) {
			resourceMap.put("instructional", columns.getColumnByName("instructional").getStringValue());
			resourceMap.put("instructionalId", instructionalCache.get(columns.getColumnByName("instructional").getStringValue()));
		}
		if (gooruOid != null) {
			Set<String> contentItems = baseDao.getAllLevelParents(ColumnFamilySet.COLLECTIONITEM.getColumnFamily(), gooruOid, 0);
			if (!contentItems.isEmpty()) {
				resourceMap.put("contentItems", contentItems);
			}
			resourceMap.put("gooruOid", gooruOid);

			ColumnList<String> questionList = baseDao.readWithKey(ColumnFamilySet.QUESTIONCOUNT.getColumnFamily(), gooruOid);

			this.getLiveCounterData("all~" + gooruOid, resourceMap);

			if (questionList != null && questionList.size() > 0) {
				resourceMap.put("questionCount", questionList.getColumnByName("questionCount") != null ? questionList.getColumnByName("questionCount").getLongValue() : 0L);
				resourceMap.put("resourceCount", questionList.getColumnByName("resourceCount") != null ? questionList.getColumnByName("resourceCount").getLongValue() : 0L);
				resourceMap.put("oeCount", questionList.getColumnByName("oeCount") != null ? questionList.getColumnByName("oeCount").getLongValue() : 0L);
				resourceMap.put("mcCount", questionList.getColumnByName("mcCount") != null ? questionList.getColumnByName("mcCount").getLongValue() : 0L);

				resourceMap.put("fibCount", questionList.getColumnByName("fibCount") != null ? questionList.getColumnByName("fibCount").getLongValue() : 0L);
				resourceMap.put("maCount", questionList.getColumnByName("maCount") != null ? questionList.getColumnByName("maCount").getLongValue() : 0L);
				resourceMap.put("tfCount", questionList.getColumnByName("tfCount") != null ? questionList.getColumnByName("tfCount").getLongValue() : 0L);

				resourceMap.put("itemCount", questionList.getColumnByName("itemCount") != null ? questionList.getColumnByName("itemCount").getLongValue() : 0L);
			}
		}
		if (columns.getColumnByName("user_uid") != null) {
			resourceMap = this.getUserInfo(resourceMap, columns.getColumnByName("user_uid").getStringValue());
		}
		if (gooruOid != null) {
			resourceMap = this.getTaxonomyInfo(resourceMap, gooruOid);
			this.saveInESIndex(resourceMap, ESIndexices.CONTENTCATALOGINFO.getIndex() + "_" + cache.get(Constants.INDEXINGVERSION), IndexType.DIMRESOURCE.getIndexType(), gooruOid);
		}
		// }
	}

	/**
	 * 
	 * @param key
	 * @param resourceMap
	 * @return
	 */
	private Map<String, Object> getLiveCounterData(String key, Map<String, Object> resourceMap) {

		ColumnList<String> vluesList = baseDao.readWithKey(ColumnFamilySet.LIVEDASHBOARD.getColumnFamily(), key);

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
			resourceMap.put("reviewCount", vluesList.getColumnByName("count~reviews") != null ? vluesList.getColumnByName("count~reviews").getLongValue() : 0L);
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
				if (DataLoggerCaches.getBeFieldName().containsKey(entry.getKey())) {
					rowKey = DataLoggerCaches.getBeFieldName().get(entry.getKey());
				}
				if (rowKey != null && entry.getValue() != null && !entry.getValue().equals("null") && entry.getValue() != "") {
					contentBuilder.field(rowKey, entry.getValue());
					/**
					 * Disabled in release-3.0. Double check indexing running correctly contentBuilder.field(rowKey, TypeConverter.stringToAny((entry.getValue()),
					 * DataLoggerCaches.getFieldDataTypes().containsKey(entry.getKey()) ? DataLoggerCaches.getFieldDataTypes().get(entry.getKey()) : "String"));
					 */
				}
			}
		} catch (Exception e) {
			LOG.error("Exception while Indexing:", e);
		}
		if (contentBuilder != null) {
			indexingES(indexName, indexType, id, contentBuilder, 0);
		}

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
		if (contentBuilder != null) {
			try {
				contentBuilder.field("index_updated_time", new Date());
				getESClient().prepareIndex(indexName, indexType, id).setSource(contentBuilder).execute().actionGet();
			} catch (Exception e) {
				LOG.error("Exception:Unable to index in the method indexingES.", e);
				if (retryCount < 6) {
					try {
						Thread.sleep(2000);
					} catch (InterruptedException e1) {
						LOG.error("Exception:Thread interrupted in the method indexingES.", e);
					}
					LOG.info("Retrying count: {}  ", retryCount);
					retryCount++;
					indexingES(indexName, indexType, id, contentBuilder, retryCount);
				} else {
					LOG.error("Exception:Unable to index in the method indexingES.", e);
				}
			}
		}
	}

	/**
	 * 
	 * @param userId
	 * @throws Exception
	 */
	public void getUserAndIndex(String userId) throws Exception {

		ColumnList<String> userInfos = baseDao.readWithKey(ColumnFamilySet.USER.getColumnFamily(), userId);

		if (userInfos != null & userInfos.size() > 0) {
			LOG.info("INdexing user : " + userId);
			XContentBuilder contentBuilder = jsonBuilder().startObject();
			if (userId != null) {
				contentBuilder.field("user_uid", userId);
				Map<String, Object> userMap = new HashMap<String, Object>();
				this.getLiveCounterData("all~" + userId, userMap);
				for (Map.Entry<String, Object> entry : userMap.entrySet()) {
					String rowKey = null;
					if (DataLoggerCaches.getBeFieldName().containsKey(entry.getKey())) {
						rowKey = DataLoggerCaches.getBeFieldName().get(entry.getKey());
						contentBuilder.field(rowKey, entry.getValue());
					}
				}
			}
			if (userInfos.getColumnByName("confirmStatus") != null) {
				contentBuilder.field("confirm_status", Long.valueOf(userInfos.getColumnByName("confirmStatus").getStringValue()));
			}
			if (userInfos.getColumnByName("createdOn") != null) {
				contentBuilder.field("registered_on", userInfos.getColumnByName("createdOn").getDateValue());
			}
			if (userInfos.getColumnByName("addedBySystem") != null) {
				contentBuilder.field("added_by_system", userInfos.getColumnByName("addedBySystem").getLongValue());
			}
			if (userInfos.getColumnByName("accountRegisterType") != null) {
				contentBuilder.field("account_created_type", userInfos.getColumnByName("accountRegisterType").getStringValue());
			}
			if (userInfos.getColumnByName("referenceUid") != null) {
				contentBuilder.field("reference_uid", userInfos.getColumnByName("referenceUid").getStringValue());
			}
			if (userInfos.getColumnByName("emailSso") != null) {
				contentBuilder.field("email_sso", userInfos.getColumnByName("emailSso").getStringValue());
			}
			if (userInfos.getColumnByName("deactivatedOn") != null) {
				contentBuilder.field("deactivated_on", userInfos.getColumnByName("deactivatedOn").getDateValue());
			}
			if (userInfos.getColumnByName("active") != null) {
				contentBuilder.field("active", userInfos.getColumnByName("active").getShortValue());
			}
			if (userInfos.getColumnByName("lastLogin") != null) {
				contentBuilder.field("last_login", userInfos.getColumnByName("lastLogin").getDateValue());
			}
			if (userInfos.getColumnByName("roleSet") != null) {
				Set<String> roleSet = new HashSet<String>();
				for (String role : userInfos.getColumnByName("roleSet").getStringValue().split(",")) {
					roleSet.add(role);
				}
				contentBuilder.field("roles", roleSet);
			}

			if (userInfos.getColumnByName("identityId") != null) {
				contentBuilder.field("identity_id", userInfos.getColumnByName("identityId").getIntegerValue());
			}
			if (userInfos.getColumnByName("mailStatus") != null) {
				contentBuilder.field("mail_status", userInfos.getColumnByName("mailStatus").getLongValue());
			}
			if (userInfos.getColumnByName("idpId") != null) {
				contentBuilder.field("idp_id", userInfos.getColumnByName("idpId").getIntegerValue());
			}
			if (userInfos.getColumnByName("state") != null) {
				contentBuilder.field("state", userInfos.getColumnByName("state").getStringValue());
			}
			if (userInfos.getColumnByName("loginType") != null) {
				contentBuilder.field("login_type", userInfos.getColumnByName("loginType").getStringValue());
			}
			if (userInfos.getColumnByName("userGroupUid") != null) {
				contentBuilder.field("user_group_uid", userInfos.getColumnByName("userGroupUid").getStringValue());
			}
			if (userInfos.getColumnByName("primaryOrganizationUid") != null) {
				contentBuilder.field("primary_organization_uid", userInfos.getColumnByName("primaryOrganizationUid").getStringValue());
			}
			if (userInfos.getColumnByName("licenseVersion") != null) {
				contentBuilder.field("license_version", userInfos.getColumnByName("licenseVersion").getStringValue());
			}
			if (userInfos.getColumnByName("parentId") != null) {
				contentBuilder.field("parent_id", userInfos.getColumnByName("parentId").getLongValue());
			}
			if (userInfos.getColumnByName("lastname") != null) {
				contentBuilder.field("lastname", userInfos.getColumnByName("lastname").getStringValue());
			}
			if (userInfos.getColumnByName("accountTypeId") != null) {
				contentBuilder.field("account_type_id", userInfos.getColumnByName("accountTypeId").getLongValue());
			}
			if (userInfos.getColumnByName("isDeleted") != null) {
				contentBuilder.field("is_deleted", userInfos.getColumnByName("isDeleted").getBooleanValue() ? 1 : 0);
			}
			if (userInfos.getColumnByName("emailId") != null) {
				contentBuilder.field("external_id", userInfos.getColumnByName("emailId").getStringValue());
			}
			if (userInfos.getColumnByName("organization.partyUid") != null) {
				contentBuilder.field("user_organization_uid", userInfos.getColumnByName("organization.partyUid").getStringValue());
			}
			if (userInfos.getColumnByName("importCode") != null) {
				contentBuilder.field("import_code", userInfos.getColumnByName("importCode").getStringValue());
			}
			if (userInfos.getColumnByName("parentUid") != null) {
				contentBuilder.field("parent_uid", userInfos.getColumnByName("parentUid").getStringValue());
			}
			if (userInfos.getColumnByName("securityGroupUid") != null) {
				contentBuilder.field("security_group_uid", userInfos.getColumnByName("securityGroupUid").getStringValue());
			}
			if (userInfos.getColumnByName("username") != null) {
				contentBuilder.field("username", userInfos.getColumnByName("username").getStringValue());
			}
			if (userInfos.getColumnByName("roleId") != null) {
				contentBuilder.field("role_id", userInfos.getColumnByName("roleId").getLongValue());
			}
			if (userInfos.getColumnByName("firstname") != null) {
				contentBuilder.field("firstname", userInfos.getColumnByName("firstname").getStringValue());
			}
			if (userInfos.getColumnByName("registerToken") != null) {
				contentBuilder.field("register_token", userInfos.getColumnByName("registerToken").getStringValue());
			}
			if (userInfos.getColumnByName("viewFlag") != null) {
				contentBuilder.field("view_flag", userInfos.getColumnByName("viewFlag").getLongValue());
			}
			if (userInfos.getColumnByName("accountUid") != null) {
				contentBuilder.field("account_uid", userInfos.getColumnByName("accountUid").getStringValue());
			}
			if (userInfos.getColumnByName("userProfileImage") != null) {
				contentBuilder.field("user_profile_image", userInfos.getColumnByName("userProfileImage").getStringValue());
			}

			ColumnList<String> extractedUserData = baseDao.readWithKey(ColumnFamilySet.EXTRACTEDUSER.getColumnFamily(), userId);
			if (extractedUserData != null) {
				for (Column<String> column : extractedUserData) {
					if (StringUtils.isNotBlank(column.getStringValue())) {
						if (!column.getName().equalsIgnoreCase(Constants.GRADE) && !column.getName().equalsIgnoreCase(Constants.TAXONOMY_IDS)) {
							contentBuilder.field(column.getName(), column.getStringValue());
						} else {
							Set<String> grades = new HashSet<String>();
							Set<Long> subjectCodes = new HashSet<Long>();
							Set<Long> courseCodes = new HashSet<Long>();
							for (String field : column.getStringValue().split(Constants.COMMA)) {
								if (column.getName().equalsIgnoreCase(Constants.GRADE)) {
									grades.add(field);
								} else {
									ColumnList<String> extractedCodeData = baseDao.readWithKey(ColumnFamilySet.EXTRACTEDCODE.getColumnFamily(), field);
									if (extractedCodeData != null && extractedCodeData.getColumnNames().contains(Constants.SUBJECT_CODE_ID)) {
										long subject = extractedCodeData.getLongValue(Constants.SUBJECT_CODE_ID, 0L);
										if (subject != 0L) {
											subjectCodes.add(subject);
										}
										long course = extractedCodeData.getLongValue(Constants.COURSE_CODE_ID, 0L);
										if (course != 0L) {
											courseCodes.add(course);
										}
									}
								}
							}
							if (!grades.isEmpty()) {
								contentBuilder.field(Constants.GRADE_FIELD, grades);
							}
							if (!subjectCodes.isEmpty() && !courseCodes.isEmpty()) {
								contentBuilder.field(Constants.SUBJECT, subjectCodes);
								contentBuilder.field(Constants.COURSE, courseCodes);
							}
						}
					}
				}
			}
			ColumnList<String> aliasUserData = baseDao.readWithKey(ColumnFamilySet.ANONYMIZEDUSERDATA.getColumnFamily(), userId);

			if (aliasUserData.getColumnNames().contains("firstname_alias")) {
				contentBuilder.field("firstname_alias", aliasUserData.getColumnByName("firstname_alias").getStringValue());
			}
			if (aliasUserData.getColumnNames().contains("lastname_alias")) {
				contentBuilder.field("lastname_alias", aliasUserData.getColumnByName("lastname_alias").getStringValue());
			}
			if (aliasUserData.getColumnNames().contains("username_alias")) {
				contentBuilder.field("username_alias", aliasUserData.getColumnByName("username_alias").getStringValue());
			}
			if (aliasUserData.getColumnNames().contains("external_id_alias")) {
				contentBuilder.field("external_id_alias", aliasUserData.getColumnByName("external_id_alias").getStringValue());
			}

			contentBuilder.field("index_updated_time", new Date());
			getESClient().prepareIndex(ESIndexices.USERCATALOG.getIndex() + "_" + DataLoggerCaches.getCache().get(Constants.INDEXINGVERSION), IndexType.DIMUSER.getIndexType(), userId)
					.setSource(contentBuilder).execute().actionGet();
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
			ColumnList<String> sourceValues = baseDao.readWithKey(ColumnFamilySet.TAXONOMYCODE.getColumnFamily(), id);
			if (sourceValues != null && sourceValues.size() > 0) {
				XContentBuilder contentBuilder = jsonBuilder().startObject();
				for (int i = 0; i < sourceValues.size(); i++) {
					if (DataLoggerCaches.getTaxonomyCodeType().get(sourceValues.getColumnByIndex(i).getName()).equalsIgnoreCase("String")) {
						contentBuilder.field(sourceValues.getColumnByIndex(i).getName(), sourceValues.getColumnByIndex(i).getStringValue());
					}
					if (DataLoggerCaches.getTaxonomyCodeType().get(sourceValues.getColumnByIndex(i).getName()).equalsIgnoreCase("Long")) {
						contentBuilder.field(sourceValues.getColumnByIndex(i).getName(), sourceValues.getColumnByIndex(i).getLongValue());
					}
					if (DataLoggerCaches.getTaxonomyCodeType().get(sourceValues.getColumnByIndex(i).getName()).equalsIgnoreCase("Integer")) {
						contentBuilder.field(sourceValues.getColumnByIndex(i).getName(), sourceValues.getColumnByIndex(i).getIntegerValue());
					}
					if (DataLoggerCaches.getTaxonomyCodeType().get(sourceValues.getColumnByIndex(i).getName()).equalsIgnoreCase("Double")) {
						contentBuilder.field(sourceValues.getColumnByIndex(i).getName(), sourceValues.getColumnByIndex(i).getDoubleValue());
					}
					/**
					 * Disabled in release-3.0. Double check indexing running correctly if
					 * (DataLoggerCaches.getTaxonomyCodeType().get(sourceValues.getColumnByIndex(i).getName()).equalsIgnoreCase("Date")) {
					 * contentBuilder.field(sourceValues.getColumnByIndex(i).getName(), TypeConverter.stringToAny(sourceValues.getColumnByIndex(i).getStringValue(), "Date")); }
					 */
				}
				contentBuilder.field("index_updated_time", new Date());
				getESClient().prepareIndex(ESIndexices.TAXONOMYCATALOG.getIndex() + "_" + DataLoggerCaches.getCache().get(Constants.INDEXING_VERSION), IndexType.TAXONOMYCODE.getIndexType(), id)
						.setSource(contentBuilder).execute().actionGet();
			}
		}
	}
}
