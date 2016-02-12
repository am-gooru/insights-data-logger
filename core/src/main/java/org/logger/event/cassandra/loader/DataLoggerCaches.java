package org.logger.event.cassandra.loader;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.logger.event.cassandra.loader.dao.BaseCassandraRepo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;

public class DataLoggerCaches {

	private BaseCassandraRepo baseDao;

	public static Map<String, String> cache;

	public static Map<String, Object> gooruTaxonomy;

	public static Collection<String> pushingEvents;

	public static Boolean canRunScheduler = false;

	public static Boolean canRunIndexing = false;

	public static Map<String, Object> licenseCache;

	public static Map<String, Object> resourceTypesCache;

	public static Map<String, Object> categoryCache;

	public static Map<String, Object> resourceFormatCache;
	
	public static Map<String, Object> instructionalCache;
	
	public static Map<String, String> taxonomyCodeType;

	public static Map<String, Map<String, String>> kafkaConfigurationCache = new HashMap<String, Map<String, String>>();;

	public static Map<String, String> fieldDataTypes = null;

	public static Map<String, String> beFieldName = null;

	public static String repoPath = null;
	
	private static final Logger LOG = LoggerFactory.getLogger(CassandraDataLoader.class);

	/**
	 * Loading information that requires while tomcat service start up
	 */
	public DataLoggerCaches() {
		// TODO Auto-generated constructor stub
		init();
	}

	private void init() {
		try {
			/**
			 * Disabled in release-3.0 Rows<String, String> operators = baseDao.readAllRows(ColumnFamily.REALTIMECONFIG.getColumnFamily(), 0); cache = new LinkedHashMap<String, String>(); for
			 * (Row<String, String> row : operators) { cache.put(row.getKey(), row.getColumns().getStringValue(AGG_JSON, null)); }
			 */
			//cache.put(VIEW_EVENTS, baseDao.readWithKeyColumn(ColumnFamilySet.CONFIGSETTINGS.getColumnFamily(), _VIEW_EVENTS, DEFAULT_COLUMN, 0).getStringValue());
			/**
			 * Disabled in release-3.0 cache.put(ATMOSPHERE_END_POINT, baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), ATM_END_POINT, DEFAULT_COLUMN, 0).getStringValue());
			 */
			/*cache.put(VIEW_UPDATE_END_POINT, baseDao.readWithKeyColumn(ColumnFamilySet.CONFIGSETTINGS.getColumnFamily(), LoaderConstants.VIEW_COUNT_REST_API_END_POINT.getName(), DEFAULT_COLUMN, 0)
					.getStringValue());
			cache.put(SESSION_TOKEN, baseDao.readWithKeyColumn(ColumnFamilySet.CONFIGSETTINGS.getColumnFamily(), LoaderConstants.SESSIONTOKEN.getName(), DEFAULT_COLUMN, 0).getStringValue());
			cache.put(SEARCH_INDEX_API, baseDao.readWithKeyColumn(ColumnFamilySet.CONFIGSETTINGS.getColumnFamily(), LoaderConstants.SEARCHINDEXAPI.getName(), DEFAULT_COLUMN, 0).getStringValue());
			cache.put(STAT_FIELDS, baseDao.readWithKeyColumn(ColumnFamilySet.CONFIGSETTINGS.getColumnFamily(), STAT_FIELDS, DEFAULT_COLUMN, 0).getStringValue());
			cache.put(_BATCH_SIZE, baseDao.readWithKeyColumn(ColumnFamilySet.CONFIGSETTINGS.getColumnFamily(), _BATCH_SIZE, DEFAULT_COLUMN, 0).getStringValue());
			cache.put(INDEXING_VERSION, baseDao.readWithKeyColumn(ColumnFamilySet.CONFIGSETTINGS.getColumnFamily(), INDEXING_VERSION, DEFAULT_COLUMN, 0).getStringValue());

			ColumnList<String> schdulersStatus = baseDao.readWithKey(ColumnFamilySet.CONFIGSETTINGS.getColumnFamily(), SCH_STATUS, 0);
			for (int i = 0; i < schdulersStatus.size(); i++) {
				cache.put(schdulersStatus.getColumnByIndex(i).getName(), schdulersStatus.getColumnByIndex(i).getStringValue());
			}
			pushingEvents = baseDao.readWithKey(ColumnFamilySet.CONFIGSETTINGS.getColumnFamily(), DEFAULTKEY, 0).getColumnNames();
			String host = baseDao.readWithKey(ColumnFamilySet.CONFIGSETTINGS.getColumnFamily(), SCH_HOST, 0).getStringValue(HOST, "localhost");

			String localHost = "" + InetAddress.getLocalHost();
			logger.debug("localHost: " + localHost);
			logger.debug("Host: " + host);
			logger.debug("canRunScheduler before: " + canRunScheduler);
			if (localHost.contains(host)) {
				canRunScheduler = true;
			} else {
				canRunScheduler = false;
			}
			logger.debug("canRunScheduler after: " + canRunScheduler);

			String realTimeIndexing = baseDao.readWithKey(ColumnFamilySet.CONFIGSETTINGS.getColumnFamily(), REAL_TIME_INDEXING, 0).getStringValue(DEFAULT_COLUMN, null);
			if (realTimeIndexing.equalsIgnoreCase(STOP)) {
				canRunIndexing = false;
			} else {
				canRunIndexing = true;
			}*/

				kafkaConfigurationCache = new HashMap<String, Map<String, String>>();
				String[] kafkaMessager = new String[] { Constants.V2_KAFKA_CONSUMER, Constants.V2_KAFKA_LOG_WRITER_PRODUCER, Constants.V2_KAFKA_LOG_WRITER_CONSUMER, Constants.V2_KAFKA_MICRO_PRODUCER, Constants.V2_KAFKA_MICRO_CONSUMER };
				Rows<String, String> result = baseDao.readCommaKeyList(Constants.CONFIG_SETTINGS, kafkaMessager);
				for (Row<String, String> row : result) {
					Map<String, String> properties = new HashMap<String, String>();
					for (Column<String> column : row.getColumns()) {
						properties.put(column.getName(), column.getStringValue());
					}
					kafkaConfigurationCache.put(row.getKey(), properties);
				}
			
			beFieldName = new LinkedHashMap<String, String>();
			fieldDataTypes = new LinkedHashMap<String, String>();
			Rows<String, String> fieldDescrption = baseDao.readAllRows(ColumnFamilySet.EVENTFIELDS.getColumnFamily());
			for (Row<String, String> row : fieldDescrption) {
				fieldDataTypes.put(row.getKey(), row.getColumns().getStringValue("description", ""));
				beFieldName.put(row.getKey(), row.getColumns().getStringValue("be_column", ""));
			}

			Rows<String, String> licenseRows = baseDao.readAllRows(ColumnFamilySet.LICENSE.getColumnFamily());
			licenseCache = new LinkedHashMap<String, Object>();
			for (Row<String, String> row : licenseRows) {
				licenseCache.put(row.getKey(), row.getColumns().getLongValue("id", 0L));
			}
			Rows<String, String> resourceTypesRows = baseDao.readAllRows(ColumnFamilySet.RESOURCETYPES.getColumnFamily());
			resourceTypesCache = new LinkedHashMap<String, Object>();
			for (Row<String, String> row : resourceTypesRows) {
				resourceTypesCache.put(row.getKey(), row.getColumns().getLongValue("id", 0L));
			}
			Rows<String, String> categoryRows = baseDao.readAllRows(ColumnFamilySet.CATEGORY.getColumnFamily());
			categoryCache = new LinkedHashMap<String, Object>();
			for (Row<String, String> row : categoryRows) {
				categoryCache.put(row.getKey(), row.getColumns().getLongValue("id", 0L));
			}

			taxonomyCodeType = new LinkedHashMap<String, String>();

			ColumnList<String> taxonomyCodeTypeList = baseDao.readWithKey(ColumnFamilySet.TABLEDATATYPES.getColumnFamily(), "taxonomy_code");
			for (int i = 0; i < taxonomyCodeTypeList.size(); i++) {
				taxonomyCodeType.put(taxonomyCodeTypeList.getColumnByIndex(i).getName(), taxonomyCodeTypeList.getColumnByIndex(i).getStringValue());
			}
			Rows<String, String> resourceFormatRows = baseDao.readAllRows(ColumnFamilySet.RESOURCEFORMAT.getColumnFamily());
			resourceFormatCache = new LinkedHashMap<String, Object>();
			for (Row<String, String> row : resourceFormatRows) {
				resourceFormatCache.put(row.getKey(), row.getColumns().getLongValue("id", 0L));
			}
			Rows<String, String> instructionalRows = baseDao.readAllRows(ColumnFamilySet.INSTRUCTIONAL.getColumnFamily());

			instructionalCache = new LinkedHashMap<String, Object>();
			for (Row<String, String> row : instructionalRows) {
				instructionalCache.put(row.getKey(), row.getColumns().getLongValue("id", null));
			}
			repoPath = baseDao.readWithKeyColumn(ColumnFamilySet.CONFIGSETTINGS.getColumnFamily(), "repo.path", Constants.DEFAULT_COLUMN).getStringValue();
		} catch (Exception e) {
			LOG.error("Exception : " + e);
		}
	}

	public static Map<String, String> getCache() {
		return cache;
	}

	public static Collection<String> getPushingEvents() {
		return pushingEvents;
	}

	public static Boolean getCanRunScheduler() {
		return canRunScheduler;
	}

	public static Boolean getCanRunIndexing() {
		return canRunIndexing;
	}

	public static Map<String, Object> getLicenseCache() {
		return licenseCache;
	}


	public static Map<String, Object> getResourceTypesCache() {
		return resourceTypesCache;
	}


	public static Map<String, Object> getCategoryCache() {
		return categoryCache;
	}


	public static Map<String, String> getTaxonomyCodeType() {
		return taxonomyCodeType;
	}


	public static Map<String, String> getBeFieldName() {
		return beFieldName;
	}

	public static Map<String, Object> getGooruTaxonomy() {
		return gooruTaxonomy;
	}

	public static Map<String, Map<String, String>> getKafkaConfigurationCache() {
		return kafkaConfigurationCache;
	}

	
	public static Map<String, String> getFieldDataTypes() {
		return fieldDataTypes;
	}

	public static Map<String, Object> getResourceFormatCache() {
		return resourceFormatCache;
	}


	public static Map<String, Object> getInstructionalCache() {
		return instructionalCache;
	}


	public static String getREPOPATH() {
		return repoPath;
	}

}
