package org.logger.event.cassandra.loader.dao;

import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.Map;

import org.ednovo.data.geo.location.GeoLocation;
import org.ednovo.data.model.EventBuilder;
import org.ednovo.data.model.GeoData;
import org.ednovo.data.model.TypeConverter;
import org.logger.event.cassandra.loader.ColumnFamilySet;
import org.logger.event.cassandra.loader.Constants;
import org.logger.event.cassandra.loader.DataLoggerCaches;
import org.logger.event.cassandra.loader.DataUtils;
import org.logger.event.cassandra.loader.LoaderConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.maxmind.geoip2.model.CityResponse;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.model.ColumnList;

public class LiveDashBoardDAOImpl extends BaseDAOCassandraImpl {

	private static final Logger LOG = LoggerFactory.getLogger(LiveDashBoardDAOImpl.class);

	private SimpleDateFormat secondDateFormatter = new SimpleDateFormat("yyyyMMddkkmmss");

	private SimpleDateFormat minDateFormatter = new SimpleDateFormat("yyyyMMddkkmm");

	private SimpleDateFormat customDateFormatter;

	private BaseCassandraRepo baseDao;	

	Collection<String> esEventFields = null;
	
	private static String charactersToRemove = "^\\[|\\s|\\]$";
	
	private GeoLocation geoLocation;
	
	public LiveDashBoardDAOImpl() {
		baseDao = BaseCassandraRepo.instance();
		this.geoLocation = GeoLocation.getInstance();
	}

	/**
	 * 
	 * @param eventMap
	 */
	public <T> void realTimeMetricsCounter(EventBuilder eventMap) {
		try {
			MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
			if (eventMap.getEventName().matches(Constants.VIEW_CALC_EVENTS)) {
				baseDao.generateCounter(ColumnFamilySet.LIVEDASHBOARD.getColumnFamily(), (Constants.ALL_TILT + eventMap.getContentGooruId()), "count~views", eventMap.getViews(), m);
			}
			m.execute();
		} catch (Exception e) {
			LOG.error("Exception", e);
		}
	}
	public <T> void realTimeMetricsCounter(Map<String, Object> eventMap) {
		if ((eventMap.containsKey(Constants.EVENT_NAME))) {
			try {
			MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
			ColumnList<String> eventKeys = baseDao.readWithKey(ColumnFamilySet.CONFIGSETTINGS.getColumnFamily(), (String)eventMap.get(Constants.EVENT_NAME));
			for (int i = 0; i < eventKeys.size(); i++) {
				String columnName = eventKeys.getColumnByIndex(i).getName();
				String columnValue = eventKeys.getColumnByIndex(i).getStringValue();
				
				if(eventMap.get(Constants.EVENT_NAME).equals(LoaderConstants.ITEM_DOT_EDIT.getName()) && columnName.contains(Constants.COLLECTION_ITEM_IDS)) {
					if (eventMap.get(Constants.PREVIOUS_SHARING) != null && eventMap.get(Constants.CONTENT_SHARING) != null && 
							eventMap.get(Constants.COLLECTION_ITEM_IDS) != null && !eventMap.get(Constants.PREVIOUS_SHARING).equals(eventMap.get(Constants.CONTENT_SHARING))) {
						for (String collectionItemId : eventMap.get(Constants.COLLECTION_ITEM_IDS).toString().replaceAll(charactersToRemove, Constants.EMPTY).split(Constants.COMMA)) {
							prepareColumnAndStoreMetrics(Constants.ALL_TILT.concat(collectionItemId), columnValue, eventMap, m);
						}
					}
				} else {				
					String key = this.formOriginalKeys(columnName, eventMap);
					if (key != null) {
						prepareColumnAndStoreMetrics(key, columnValue, eventMap, m);
					}
				}
			}
				m.execute();
			} catch (Exception e) {
				LOG.error("Exception event: {}",eventMap.get(Constants.EVENT_ID));
				LOG.error("Exception: Real Time counter failed:" , e);
			}
		}
	}

	private void performCounter(String key,String column,Map<String, Object> eventMap,MutationBatch m){
		
		if (column.startsWith(Constants._TIME_SPENT + Constants.SEPERATOR)) {
			baseDao.generateCounter(ColumnFamilySet.LIVEDASHBOARD.getColumnFamily(), key, column, ((Number)eventMap.get(Constants.TOTALTIMEINMS)).longValue(), m);
		} else if (column.startsWith(Constants.SUM + Constants.SEPERATOR)) {
			String rowKey = column.split(Constants.SEPERATOR)[1];
			baseDao.generateCounter(
					ColumnFamilySet.LIVEDASHBOARD.getColumnFamily(),
					key,
					column,
					rowKey.equalsIgnoreCase(Constants.REACTION_TYPE) ? DataUtils.formatReactionString((String)eventMap.get(rowKey)) : eventMap.containsKey(rowKey.trim()) ? ((Number)eventMap.get(rowKey.trim())).longValue() : 0L, m);

		} else {
			baseDao.generateCounter(ColumnFamilySet.LIVEDASHBOARD.getColumnFamily(), key, column, 1L, m);
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
			LOG.error("Exception:unable to get geo locations");
		}
		if (columnName != null) {
			baseDao.saveStringValue(ColumnFamilySet.MICROAGGREGATION.getColumnFamily(), rowKey, columnName, json);
		}
		baseDao.increamentCounter(ColumnFamilySet.LIVEDASHBOARD.getColumnFamily(), columnName, Constants.COUNT + Constants.SEPERATOR + eventMap.get(Constants.EVENT_NAME), 1);
	}

	/**
	 * 
	 * @param eventMap
	 */
	public void addApplicationSession(Map<String, String> eventMap) {

		int expireTime = 3600;
		int contentExpireTime = 600;

		MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
		if (eventMap.get(Constants.GOORUID).equalsIgnoreCase("ANONYMOUS")) {
			baseDao.generateTTLColumns(ColumnFamilySet.MICROAGGREGATION.getColumnFamily(), Constants.ANONYMOUS_SESSION, eventMap.get(Constants.SESSION_TOKEN) + Constants.SEPERATOR + eventMap.get(Constants.GOORUID),
					secondDateFormatter.format(new Date()).toString(), expireTime, m);
		} else {
			baseDao.generateTTLColumns(ColumnFamilySet.MICROAGGREGATION.getColumnFamily(), Constants.USER_SESSION, eventMap.get(Constants.SESSION_TOKEN) + Constants.SEPERATOR + eventMap.get(Constants.GOORUID),
					secondDateFormatter.format(new Date()).toString(), expireTime, m);
		}
		baseDao.generateTTLColumns(ColumnFamilySet.MICROAGGREGATION.getColumnFamily(), Constants.ALL_USER_SESSION, eventMap.get(Constants.SESSION_TOKEN) + Constants.SEPERATOR + eventMap.get(Constants.GOORUID),
				secondDateFormatter.format(new Date()).toString(), expireTime, m);

		if (eventMap.get(Constants.EVENT_NAME).equalsIgnoreCase("logOut") || eventMap.get(Constants.EVENT_NAME).equalsIgnoreCase("user.logout")) {
			if (eventMap.get(Constants.GOORUID).equalsIgnoreCase("ANONYMOUS")) {
				baseDao.deleteColumn(ColumnFamilySet.MICROAGGREGATION.getColumnFamily(), Constants.ALL_USER_SESSION, eventMap.get(Constants.SESSION_TOKEN) + Constants.SEPERATOR + eventMap.get(Constants.GOORUID));
			} else {
				baseDao.deleteColumn(ColumnFamilySet.MICROAGGREGATION.getColumnFamily(), Constants.USER_SESSION, eventMap.get(Constants.SESSION_TOKEN) + Constants.SEPERATOR + eventMap.get(Constants.GOORUID));
			}
			baseDao.deleteColumn(ColumnFamilySet.MICROAGGREGATION.getColumnFamily(), Constants.ALL_USER_SESSION, eventMap.get(Constants.SESSION_TOKEN) + Constants.SEPERATOR + eventMap.get(Constants.GOORUID));
		}

		if (eventMap.get(Constants.EVENT_NAME).equalsIgnoreCase(LoaderConstants.CPV1.getName())) {
			if (eventMap.get(Constants.TYPE).equalsIgnoreCase(Constants.STOP) || eventMap.get(Constants.TYPE).equalsIgnoreCase(Constants.PAUSE)) {
				baseDao.deleteColumn(ColumnFamilySet.MICROAGGREGATION.getColumnFamily(), Constants.ACTIVE_COLLECTION_PLAYS, eventMap.get(Constants.CONTENT_GOORU_OID) + Constants.SEPERATOR + eventMap.get(Constants.GOORUID));
			} else {
				baseDao.generateTTLColumns(ColumnFamilySet.MICROAGGREGATION.getColumnFamily(), Constants.ACTIVE_COLLECTION_PLAYS, eventMap.get(Constants.CONTENT_GOORU_OID) + Constants.SEPERATOR + eventMap.get(Constants.GOORUID),
						secondDateFormatter.format(new Date()).toString(), contentExpireTime, m);
			}
		}
		if (eventMap.get(Constants.EVENT_NAME).equalsIgnoreCase(LoaderConstants.RP1.getName())) {
			if (eventMap.get(Constants.TYPE).equalsIgnoreCase(Constants.STOP)) {
				baseDao.deleteColumn(ColumnFamilySet.MICROAGGREGATION.getColumnFamily(), Constants.ACTIVE_RESOURCE_PLAYS, eventMap.get(Constants.CONTENT_GOORU_OID) + Constants.SEPERATOR + eventMap.get(Constants.GOORUID));
			} else {
				baseDao.generateTTLColumns(ColumnFamilySet.MICROAGGREGATION.getColumnFamily(), Constants.ACTIVE_RESOURCE_PLAYS, eventMap.get(Constants.CONTENT_GOORU_OID) + Constants.SEPERATOR + eventMap.get(Constants.GOORUID), secondDateFormatter
						.format(new Date()).toString(), contentExpireTime, m);
			}
		}
		if (eventMap.get(Constants.EVENT_NAME).equalsIgnoreCase(LoaderConstants.CRPV1.getName())) {
			if (eventMap.get(Constants.TYPE).equalsIgnoreCase(Constants.STOP)) {
				baseDao.deleteColumn(ColumnFamilySet.MICROAGGREGATION.getColumnFamily(), Constants.ACTIVE_COLLECTION_RESOURCE_PLAYS, eventMap.get(Constants.CONTENT_GOORU_OID) + Constants.SEPERATOR + eventMap.get(Constants.GOORUID));
			} else {
				baseDao.generateTTLColumns(ColumnFamilySet.MICROAGGREGATION.getColumnFamily(), Constants.ACTIVE_COLLECTION_RESOURCE_PLAYS, eventMap.get(Constants.CONTENT_GOORU_OID) + Constants.SEPERATOR + eventMap.get(Constants.GOORUID),
						secondDateFormatter.format(new Date()).toString(), contentExpireTime, m);
			}
		}

		try {
			m.executeAsync();
		} catch (Exception e) {
			LOG.error("Exception: saving session time failed" + e);
		}
	}

	/**
	 * Adding Queue for viewed resources/collections
	 * @param eventMaps
	 */
	public <T> void addContentForPostViews(Map<String, T> eventMap) {
		String dateKey = minDateFormatter.format(new Date()).toString();
		MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);

		LOG.info("Key- view : {} ", Constants.VIEWS + Constants.SEPERATOR + dateKey);
		if(eventMap.get(Constants.CONTENT_GOORU_OID) != null){
			baseDao.generateTTLColumns(ColumnFamilySet.MICROAGGREGATION.getColumnFamily(), Constants.VIEWS + Constants.SEPERATOR + dateKey, eventMap.get(Constants.CONTENT_GOORU_OID).toString(), eventMap.get(Constants.CONTENT_GOORU_OID).toString(), 2592000, m);
		}
		try {
			m.execute();
		} catch (Exception e) {
			LOG.error("Exception:saving data for view count update is failed:",e);
		}
	}

	/**
	 * C: defines => Constant D: defines => Date format lookup E: defines => eventMap param lookup
	 * 
	 * @param value
, * @param eventMap
	 * @return
	 */
	public String formOriginalKeys(String value, Map<String, Object> eventMap) {
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
					eventDateTime = new Date(((Number)eventMap.get("startTime")).longValue());
				}
				key += "~" + customDateFormatter.format(eventDateTime).toString();
			}
			if (splittedKey.startsWith("E:") && eventMap != null) {
				subKey = splittedKey.split(":");
				if (eventMap.get(subKey[1]) != null) {
					key += "~" + (String.valueOf(eventMap.get(subKey[1]))).toLowerCase();
				} else {
					return null;
				}
			}
			if (!splittedKey.startsWith("C:") && !splittedKey.startsWith("D:") && !splittedKey.startsWith("E:")) {
				try {
					throw new AccessDeniedException("Unsupported key format : " + splittedKey);
				} catch (Exception e) {
					LOG.error("Exception:Unsupported real time generator Key."+e);
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
			ColumnListMutation<String> m = mutationBatch.withRow(baseDao.accessColumnFamily(ColumnFamilySet.STAGING.getColumnFamily()), eventMap.get("eventId").toString());
			String rowKey = null;
			for (Map.Entry<String, Object> entry : eventMap.entrySet()) {

				if (DataLoggerCaches.getBeFieldName().containsKey(entry.getKey()) && rowKey != null) {
					rowKey = DataLoggerCaches.getBeFieldName().get(entry.getKey());
				} else {
					rowKey = entry.getKey();
				}

				String typeToChange = DataLoggerCaches.getFieldDataTypes().containsKey(entry.getKey()) ? DataLoggerCaches.getFieldDataTypes().get(entry.getKey()) : "String";
				baseDao.generateNonCounter(rowKey, TypeConverter.stringToAny(entry.getValue(), typeToChange), m);
			}

			mutationBatch.execute();
		} catch (Exception e) {
			LOG.error("Exception:Storing data into staging table."+ e);
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
			CityResponse res = geoLocation.getGeoResponse(eventMap.get("userIp"));

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
	
	private void performRating(String key, String column, Map<String, Object> map, MutationBatch m) {

		Long previousRate = map.get(Constants.PREVIOUS_RATE) != null ? ((Number) map.get(Constants.PREVIOUS_RATE)).longValue() : 0;
		if ((previousRate != 0) && column.equals(Constants.COUNT_SEPARATOR_RATINGS)) {
			baseDao.generateCounter(ColumnFamilySet.LIVEDASHBOARD.getColumnFamily(), key, Constants.COUNT + Constants.SEPERATOR + previousRate, (1L * -1), m);
			baseDao.generateCounter(ColumnFamilySet.LIVEDASHBOARD.getColumnFamily(), key, Constants.SUM + Constants.SEPERATOR + Constants.RATE, (previousRate * -1), m);
		} else {
			Long rate = map.get(Constants.RATE) != null ? ((Number) map.get(Constants.RATE)).longValue() : 0;
			String rateColumn = column.replace(map.get(Constants.RATE).toString(), rate.toString());
			performCounter(key, rateColumn, map, m);
		}
	}
	
	private void prepareColumnAndStoreMetrics(String key, String columnValue, Map<String, Object> eventMap, MutationBatch m) {
		for (String value : columnValue.split(Constants.COMMA)) {
			String originalColumn = this.formOriginalKeys(value, eventMap);
			if (originalColumn != null) {
				if(eventMap.get(Constants.EVENT_NAME).equals(LoaderConstants.ITEM_DOT_RATE.getName())){
					performRating(key, originalColumn,eventMap,m);		
				} else if(value.matches(Constants.RESOURCE_USED_USER_VALIDATION)) { 
					resourceUsedUserCount(key, value, originalColumn,eventMap,m);
				} else if (!(eventMap.containsKey(Constants.TYPE) && (eventMap.get(Constants.TYPE).equals(Constants.STOP) || eventMap.get(Constants.TYPE).equals(Constants.PAUSE)) && originalColumn.startsWith(Constants.COUNT + Constants.SEPERATOR))) {
					performCounter(key, originalColumn, eventMap, m);					
				}
			}
		}
	}

	private void resourceUsedUserCount(String key, String value, String originalColumn, Map<String, Object> eventMap, MutationBatch m) {
		String userKey = key.concat(Constants.SEPERATOR).concat(eventMap.get(Constants.GOORUID).toString());
		if(!baseDao.checkColumnExist(ColumnFamilySet.LIVEDASHBOARD.getColumnFamily(),userKey,originalColumn)) {
			baseDao.generateCounter(ColumnFamilySet.LIVEDASHBOARD.getColumnFamily(), key, originalColumn, 1L, m);
		}
		baseDao.generateCounter(ColumnFamilySet.LIVEDASHBOARD.getColumnFamily(), userKey, originalColumn, 1L, m);
	}
}
