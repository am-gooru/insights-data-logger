package org.logger.event.cassandra.loader.dao;

import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.ednovo.data.geo.location.GeoLocation;
import org.ednovo.data.model.GeoData;
import org.ednovo.data.model.TypeConverter;
import org.json.JSONException;
import org.json.JSONObject;
import org.logger.event.cassandra.loader.CassandraConnectionProvider;
import org.logger.event.cassandra.loader.ColumnFamily;
import org.logger.event.cassandra.loader.Constants;
import org.logger.event.cassandra.loader.DataLoggerCaches;
import org.logger.event.cassandra.loader.DataUtils;
import org.logger.event.cassandra.loader.LoaderConstants;
import org.restlet.data.Form;
import org.restlet.resource.ClientResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.maxmind.geoip2.model.CityResponse;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.model.ColumnList;

public class LiveDashBoardDAOImpl extends BaseDAOCassandraImpl implements LiveDashBoardDAO, Constants {

	private static final Logger logger = LoggerFactory.getLogger(LiveDashBoardDAOImpl.class);

	private CassandraConnectionProvider connectionProvider;

	private MicroAggregatorDAOmpl microAggregatorDAOmpl;

	private SimpleDateFormat secondDateFormatter = new SimpleDateFormat("yyyyMMddkkmmss");

	private SimpleDateFormat minDateFormatter = new SimpleDateFormat("yyyyMMddkkmm");

	private SimpleDateFormat customDateFormatter;

	private BaseCassandraRepoImpl baseDao;
	
	ColumnList<String> eventKeys = null;

	Collection<String> esEventFields = null;
	
	public LiveDashBoardDAOImpl(CassandraConnectionProvider connectionProvider) {
		super(connectionProvider);
		this.connectionProvider = connectionProvider;
		this.microAggregatorDAOmpl = new MicroAggregatorDAOmpl(this.connectionProvider);
		this.baseDao = new BaseCassandraRepoImpl(this.connectionProvider);	}

	/**
	 * 
	 * @param eventMap
	 */
	public <T> void realTimeMetricsCounter(Map<String, Object> eventMap) {
		MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
		if ((eventMap.containsKey(EVENT_NAME))) {
			eventKeys = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), String.valueOf(eventMap.get(EVENT_NAME)), 0);
			for (int i = 0; i < eventKeys.size(); i++) {
				String columnName = eventKeys.getColumnByIndex(i).getName();
				String columnValue = eventKeys.getColumnByIndex(i).getStringValue();
				String key = this.formOrginalKeys(columnName, eventMap);
				if (key != null) {
					for (String value : columnValue.split(COMMA)) {
						String orginalColumn = this.formOrginalKeys(value, eventMap);
						if (orginalColumn != null) {
							if (!(eventMap.containsKey(TYPE) && String.valueOf(eventMap.get(TYPE)).equalsIgnoreCase(STOP) && orginalColumn.startsWith(COUNT + SEPERATOR))) {
								if (!orginalColumn.startsWith(_TIME_SPENT + SEPERATOR) && !orginalColumn.startsWith(SUM + SEPERATOR) && !(eventMap.containsKey(PREVIOUS_RATE) && eventMap.get(PREVIOUS_RATE) != null && eventMap.get(PREVIOUS_RATE).toString().isEmpty())) {
									baseDao.generateCounter(ColumnFamily.LIVEDASHBOARD.getColumnFamily(), key, orginalColumn, 1L, m);
								} else if (orginalColumn.startsWith(_TIME_SPENT + SEPERATOR)) {
									baseDao.generateCounter(ColumnFamily.LIVEDASHBOARD.getColumnFamily(), key, orginalColumn, Long.valueOf(String.valueOf(eventMap.get(TOTALTIMEINMS))), m);
								} else if (orginalColumn.startsWith(SUM + SEPERATOR)  && !(eventMap.containsKey(PREVIOUS_RATE) && eventMap.get(PREVIOUS_RATE) != null && eventMap.get(PREVIOUS_RATE).toString().isEmpty())) {
									String[] rowKey = orginalColumn.split(SEPERATOR);
									baseDao.generateCounter(
											ColumnFamily.LIVEDASHBOARD.getColumnFamily(),
											key,
											orginalColumn,
											rowKey[1].equalsIgnoreCase(REACTION_TYPE) ? DataUtils.formatReactionString(String.valueOf(eventMap.get(rowKey[1]))) : Long.valueOf(String.valueOf(eventMap
													.get(rowKey[1].trim()) == null ? "0" : eventMap.get(rowKey[1].trim()))), m);

								}
							}
						}
					}
				}
			}

			try {
				m.execute();
			} catch (Exception e) {
				logger.error("Exception: Real Time counter failed." + e);
			}
		}
	}

	/**
	 * 
	 * @param atmosphereEndPoint
	 * @param eventMap
	 * @throws JSONException
	 */
	public void pushEventForAtmosphere(String atmosphereEndPoint, Map<String, String> eventMap) throws JSONException {

		JSONObject filtersObj = new JSONObject();
		// filtersObj.put("eventName", eventMap.get("eventName") + "," +
		// customEventsConfig);

		JSONObject mainObj = new JSONObject();
		mainObj.put("filters", filtersObj);

		ClientResource clientResource = null;
		clientResource = new ClientResource(atmosphereEndPoint + "/push/message");
		Form forms = new Form();
		forms.add("data", mainObj.toString());
		clientResource.post(forms.getWebRepresentation());
		logger.info("atmos status : {} ", clientResource.getStatus());
	}

	/**
	 * 
	 * @param atmosphereEndPoint
	 * @param eventMap
	 * @throws JSONException
	 */
	public void pushEventForAtmosphereProgress(String atmosphereEndPoint, Map<String, Object> eventMap) throws JSONException {

		JSONObject filtersObj = new JSONObject();
		JSONObject paginateObj = new JSONObject();
		Collection<String> fields = new ArrayList<String>();
		fields.add("timeSpent");
		fields.add("avgTimeSpent");
		fields.add("resourceGooruOId");
		fields.add("OE");
		fields.add("questionType");
		fields.add("category");
		fields.add("gooruUId");
		fields.add("userName");
		fields.add("userData");
		fields.add("metaData");
		fields.add("title");
		fields.add("reaction");
		fields.add("description");
		fields.add("options");
		fields.add("skip");
		filtersObj.put("session", "FS");
		paginateObj.put("sortBy", "itemSequence");
		paginateObj.put("sortOrder", "ASC");

		List<String> classpage = microAggregatorDAOmpl.getClassPages(eventMap);

		for (String classId : classpage) {
			filtersObj.put("classId", classId);
			JSONObject mainObj = new JSONObject();
			mainObj.put("filters", filtersObj);
			mainObj.put("paginate", paginateObj);
			mainObj.put("fields", fields);

			ClientResource clientResource = null;
			clientResource = new ClientResource(atmosphereEndPoint + "/classpage/users/usage");
			Form forms = new Form();
			forms.add("data", mainObj.toString());
			forms.add("collectionId", eventMap.get(PARENT_GOORU_OID).toString());
			clientResource.post(forms.getWebRepresentation());
		}

	}

	/**
	 * 
	 * @param geoData
	 * @param eventMap
	 */
	private void saveGeoLocation(GeoData geoData, Map<String, String> eventMap) {
		ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
		String json = null;
		String rowKey = "geo~locations";
		String columnName = null;
		if (geoData.getLatitude() != null && geoData.getLongitude() != null) {
			columnName = geoData.getLatitude() + "x" + geoData.getLongitude();
		}
		try {
			json = ow.writeValueAsString(geoData);
		} catch (Exception e) {
			logger.error("Exception:unable to get geo locations");
		}
		if (columnName != null) {
			baseDao.saveStringValue(ColumnFamily.MICROAGGREGATION.getColumnFamily(), rowKey, columnName, json);
		}
		baseDao.increamentCounter(ColumnFamily.LIVEDASHBOARD.getColumnFamily(), columnName, COUNT + SEPERATOR + eventMap.get(EVENT_NAME), 1);
	}

	/**
	 * 
	 * @param eventMap
	 */
	public void addApplicationSession(Map<String, String> eventMap) {

		int expireTime = 3600;
		int contentExpireTime = 600;

		MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
		if (eventMap.get(GOORUID).equalsIgnoreCase("ANONYMOUS")) {
			baseDao.generateTTLColumns(ColumnFamily.MICROAGGREGATION.getColumnFamily(), ANONYMOUS_SESSION, eventMap.get(SESSION_TOKEN) + SEPERATOR + eventMap.get(GOORUID),
					secondDateFormatter.format(new Date()).toString(), expireTime, m);
		} else {
			baseDao.generateTTLColumns(ColumnFamily.MICROAGGREGATION.getColumnFamily(), USER_SESSION, eventMap.get(SESSION_TOKEN) + SEPERATOR + eventMap.get(GOORUID),
					secondDateFormatter.format(new Date()).toString(), expireTime, m);
		}
		baseDao.generateTTLColumns(ColumnFamily.MICROAGGREGATION.getColumnFamily(), ALL_USER_SESSION, eventMap.get(SESSION_TOKEN) + SEPERATOR + eventMap.get(GOORUID),
				secondDateFormatter.format(new Date()).toString(), expireTime, m);

		if (eventMap.get(EVENT_NAME).equalsIgnoreCase("logOut") || eventMap.get(EVENT_NAME).equalsIgnoreCase("user.logout")) {
			if (eventMap.get(GOORUID).equalsIgnoreCase("ANONYMOUS")) {
				baseDao.deleteColumn(ColumnFamily.MICROAGGREGATION.getColumnFamily(), ALL_USER_SESSION, eventMap.get(SESSION_TOKEN) + SEPERATOR + eventMap.get(GOORUID));
			} else {
				baseDao.deleteColumn(ColumnFamily.MICROAGGREGATION.getColumnFamily(), USER_SESSION, eventMap.get(SESSION_TOKEN) + SEPERATOR + eventMap.get(GOORUID));
			}
			baseDao.deleteColumn(ColumnFamily.MICROAGGREGATION.getColumnFamily(), ALL_USER_SESSION, eventMap.get(SESSION_TOKEN) + SEPERATOR + eventMap.get(GOORUID));
		}

		if (eventMap.get(EVENT_NAME).equalsIgnoreCase(LoaderConstants.CPV1.getName())) {
			if (eventMap.get(TYPE).equalsIgnoreCase(STOP)) {
				baseDao.deleteColumn(ColumnFamily.MICROAGGREGATION.getColumnFamily(), ACTIVE_COLLECTION_PLAYS, eventMap.get(CONTENT_GOORU_OID) + SEPERATOR + eventMap.get(GOORUID));
			} else {
				baseDao.generateTTLColumns(ColumnFamily.MICROAGGREGATION.getColumnFamily(), ACTIVE_COLLECTION_PLAYS, eventMap.get(CONTENT_GOORU_OID) + SEPERATOR + eventMap.get(GOORUID),
						secondDateFormatter.format(new Date()).toString(), contentExpireTime, m);
			}
		}
		if (eventMap.get(EVENT_NAME).equalsIgnoreCase(LoaderConstants.RP1.getName())) {
			if (eventMap.get(TYPE).equalsIgnoreCase(STOP)) {
				baseDao.deleteColumn(ColumnFamily.MICROAGGREGATION.getColumnFamily(), ACTIVE_RESOURCE_PLAYS, eventMap.get(CONTENT_GOORU_OID) + SEPERATOR + eventMap.get(GOORUID));
			} else {
				baseDao.generateTTLColumns(ColumnFamily.MICROAGGREGATION.getColumnFamily(), ACTIVE_RESOURCE_PLAYS, eventMap.get(CONTENT_GOORU_OID) + SEPERATOR + eventMap.get(GOORUID), secondDateFormatter
						.format(new Date()).toString(), contentExpireTime, m);
			}
		}
		if (eventMap.get(EVENT_NAME).equalsIgnoreCase(LoaderConstants.CRPV1.getName())) {
			if (eventMap.get(TYPE).equalsIgnoreCase(STOP)) {
				baseDao.deleteColumn(ColumnFamily.MICROAGGREGATION.getColumnFamily(), ACTIVE_COLLECTION_RESOURCE_PLAYS, eventMap.get(CONTENT_GOORU_OID) + SEPERATOR + eventMap.get(GOORUID));
			} else {
				baseDao.generateTTLColumns(ColumnFamily.MICROAGGREGATION.getColumnFamily(), ACTIVE_COLLECTION_RESOURCE_PLAYS, eventMap.get(CONTENT_GOORU_OID) + SEPERATOR + eventMap.get(GOORUID),
						secondDateFormatter.format(new Date()).toString(), contentExpireTime, m);
			}
		}

		try {
			m.executeAsync();
		} catch (Exception e) {
			logger.error("Exception: saving session time failed" + e);
		}
	}

	/**
	 * Adding Queue for viewed resources/collections
	 * @param eventMaps
	 */
	public <T> void addContentForPostViews(Map<String, T> eventMap) {
		String dateKey = minDateFormatter.format(new Date()).toString();
		MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);

		logger.info("Key- view : {} ", VIEWS + SEPERATOR + dateKey);
		if(eventMap.get(CONTENT_GOORU_OID) != null){
			baseDao.generateTTLColumns(ColumnFamily.MICROAGGREGATION.getColumnFamily(), VIEWS + SEPERATOR + dateKey, eventMap.get(CONTENT_GOORU_OID).toString(), eventMap.get(CONTENT_GOORU_OID).toString(), 2592000, m);
		}
		try {
			m.execute();
		} catch (Exception e) {
			logger.error("Exception:saving data for view count update is failed."+e);
		}
	}

	/**
	 * C: defines => Constant D: defines => Date format lookup E: defines => eventMap param lookup
	 * 
	 * @param value
	 * @param eventMap
	 * @return
	 */
	public String formOrginalKeys(String value, Map<String, Object> eventMap) {
		Date eventDateTime = new Date();
		String key = "";
		for (String splittedKey : value.split("~")) {
			String[] subKey = null;
			if (splittedKey.startsWith("C:")) {
				subKey = splittedKey.split(":");
				key += "~" + subKey[1];
			}
			if (splittedKey.startsWith("D:")) {
				subKey = splittedKey.split(":");
				customDateFormatter = new SimpleDateFormat(subKey[1]);
				if (eventMap != null) {
					eventDateTime = new Date(Long.valueOf(String.valueOf(eventMap.get("startTime"))));
				}
				key += "~" + customDateFormatter.format(eventDateTime).toString();
			}
			if (splittedKey.startsWith("E:") && eventMap != null) {
				subKey = splittedKey.split(":");
				if (eventMap.get(subKey[1]) != null) {
					key += "~" + String.valueOf(eventMap.get(subKey[1])).toLowerCase();
				} else {
					return null;
				}
			}
			if (!splittedKey.startsWith("C:") && !splittedKey.startsWith("D:") && !splittedKey.startsWith("E:")) {
				try {
					throw new AccessDeniedException("Unsupported key format : " + splittedKey);
				} catch (Exception e) {
					logger.error("Exception:Unsupported real time generator Key."+e);
				}
			}
		}
		return key != null ? key.substring(1).trim() : null;
	}

	/**
	 * 
	 * @param eventMap
	 */
	public void saveInStaging(Map<String, Object> eventMap) {
		try {
			MutationBatch mutationBatch = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
			ColumnListMutation<String> m = mutationBatch.withRow(baseDao.accessColumnFamily(ColumnFamily.STAGING.getColumnFamily()), eventMap.get("eventId").toString());
			String rowKey = null;
			for (Map.Entry<String, Object> entry : eventMap.entrySet()) {

				if (DataLoggerCaches.getBeFieldName().containsKey(entry.getKey()) && rowKey != null) {
					rowKey = DataLoggerCaches.getBeFieldName().get(entry.getKey());
				} else {
					rowKey = entry.getKey();
				}

				String typeToChange = DataLoggerCaches.getFieldDataTypes().containsKey(entry.getKey()) ? DataLoggerCaches.getFieldDataTypes().get(entry.getKey()) : "String";
				baseDao.generateNonCounter(rowKey, TypeConverter.stringToAny(String.valueOf(entry.getValue()), typeToChange), m);
			}

			mutationBatch.execute();
		} catch (Exception e) {
			logger.error("Exception:Storing data into staging table."+ e);
		}
	}

	/**
	 * 
	 * @param eventMap
	 * @throws IOException
	 */
	public void saveGeoLocations(Map<String, String> eventMap) throws IOException {

		if (eventMap.containsKey("userIp") && eventMap.get("userIp") != null && !eventMap.get("userIp").isEmpty()) {

			GeoData geoData = new GeoData();

			GeoLocation geo = new GeoLocation();

			CityResponse res = geo.getGeoResponse(eventMap.get("userIp"));

			if (res != null && res.getCountry().getName() != null) {
				geoData.setCountry(res.getCountry().getName());
				eventMap.put("country", res.getCountry().getName());
			}
			if (res != null && res.getCity().getName() != null) {
				geoData.setCity(res.getCity().getName());
				eventMap.put("city", res.getCity().getName());
			}
			if (res != null && res.getLocation().getLatitude() != null) {
				geoData.setLatitude(res.getLocation().getLatitude());
			}
			if (res != null && res.getLocation().getLongitude() != null) {
				geoData.setLongitude(res.getLocation().getLongitude());
			}
			if (res != null && res.getMostSpecificSubdivision().getName() != null) {
				geoData.setState(res.getMostSpecificSubdivision().getName());
				eventMap.put("state", res.getMostSpecificSubdivision().getName());
			}

			if (geoData.getLatitude() != null && geoData.getLongitude() != null) {
				this.saveGeoLocation(geoData, eventMap);
			}
		}
	}
}
