package org.logger.event.cassandra.loader;

import java.net.InetAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.logger.event.cassandra.loader.dao.BaseCassandraRepoImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;

public class DataLoggerCaches implements Constants {

	private CassandraConnectionProvider connectionProvider;

	private BaseCassandraRepoImpl baseDao;

	public static Map<String, String> cache;

	public static Map<String, Object> gooruTaxonomy;

	public static Collection<String> pushingEvents;

	public static Boolean canRunScheduler;

	public static Boolean canRunIndexing;

	public static Map<String, Object> licenseCache;

	public static Map<String, Object> resourceTypesCache;

	public static Map<String, Object> categoryCache;

	public static Map<String, Object> resourceFormatCache;
	
	public static Map<String, Object> instructionalCache;
	
	public static Map<String, String> taxonomyCodeType;

	public static Map<String, Map<String, String>> kafkaConfigurationCache;

	public static Map<String, String> fieldDataTypes = null;

	public static Map<String, String> beFieldName = null;

	public static String REPOPATH = null;
	
	private static final Logger logger = LoggerFactory.getLogger(CassandraDataLoader.class);

	/**
	 * Loading information that requires while tomcat service start up
	 */
	public DataLoggerCaches() {
		// TODO Auto-generated constructor stub
		init();
	}

	public void init() {

		this.setConnectionProvider(new CassandraConnectionProvider());
		this.getConnectionProvider().init(null);

		baseDao = new BaseCassandraRepoImpl(getConnectionProvider());
		try {
			Rows<String, String> operators = baseDao.readAllRows(ColumnFamily.REALTIMECONFIG.getColumnFamily(), 0);
			cache = new LinkedHashMap<String, String>();
			for (Row<String, String> row : operators) {
				cache.put(row.getKey(), row.getColumns().getStringValue(AGG_JSON, null));
			}
			cache.put(VIEW_EVENTS, baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), _VIEW_EVENTS, DEFAULT_COLUMN, 0).getStringValue());
			cache.put(ATMOSPHERE_END_POINT, baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), ATM_END_POINT, DEFAULT_COLUMN, 0).getStringValue());
			cache.put(VIEW_UPDATE_END_POINT, baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), LoaderConstants.VIEW_COUNT_REST_API_END_POINT.getName(), DEFAULT_COLUMN, 0)
					.getStringValue());
			cache.put(SESSION_TOKEN, baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), LoaderConstants.SESSIONTOKEN.getName(), DEFAULT_COLUMN, 0).getStringValue());
			cache.put(SEARCH_INDEX_API, baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), LoaderConstants.SEARCHINDEXAPI.getName(), DEFAULT_COLUMN, 0).getStringValue());
			cache.put(STAT_FIELDS, baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), STAT_FIELDS, DEFAULT_COLUMN, 0).getStringValue());
			cache.put(_BATCH_SIZE, baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), _BATCH_SIZE, DEFAULT_COLUMN, 0).getStringValue());
			cache.put(INDEXING_VERSION, baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), INDEXING_VERSION, DEFAULT_COLUMN, 0).getStringValue());

			ColumnList<String> schdulersStatus = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), SCH_STATUS, 0);
			for (int i = 0; i < schdulersStatus.size(); i++) {
				cache.put(schdulersStatus.getColumnByIndex(i).getName(), schdulersStatus.getColumnByIndex(i).getStringValue());
			}
			pushingEvents = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), DEFAULTKEY, 0).getColumnNames();
			String host = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), SCH_HOST, 0).getStringValue(HOST, null);

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

			String realTimeIndexing = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), REAL_TIME_INDEXING, 0).getStringValue(DEFAULT_COLUMN, null);
			if (realTimeIndexing.equalsIgnoreCase(STOP)) {
				canRunIndexing = false;
			} else {
				canRunIndexing = true;
			}
			if (kafkaConfigurationCache == null) {

				kafkaConfigurationCache = new HashMap<String, Map<String, String>>();
				String[] kafkaMessager = new String[] { V2_KAFKA_CONSUMER, V2_KAFKA_LOG_WRITER_PRODUCER, V2_KAFKA_LOG_WRITER_CONSUMER, V2_KAFKA_MICRO_PRODUCER, V2_KAFKA_MICRO_CONSUMER };
				Rows<String, String> result = baseDao.readCommaKeyList(CONFIG_SETTINGS, 0, kafkaMessager);
				for (Row<String, String> row : result) {
					Map<String, String> properties = new HashMap<String, String>();
					for (Column<String> column : row.getColumns()) {
						properties.put(column.getName(), column.getStringValue());
					}
					kafkaConfigurationCache.put(row.getKey(), properties);
				}
			}
			beFieldName = new LinkedHashMap<String, String>();
			fieldDataTypes = new LinkedHashMap<String, String>();
			Rows<String, String> fieldDescrption = baseDao.readAllRows(ColumnFamily.EVENTFIELDS.getColumnFamily(), 0);
			for (Row<String, String> row : fieldDescrption) {
				fieldDataTypes.put(row.getKey(), row.getColumns().getStringValue("description", null));
				beFieldName.put(row.getKey(), row.getColumns().getStringValue("be_column", null));
			}

			Rows<String, String> licenseRows = baseDao.readAllRows(ColumnFamily.LICENSE.getColumnFamily(), 0);
			licenseCache = new LinkedHashMap<String, Object>();
			for (Row<String, String> row : licenseRows) {
				licenseCache.put(row.getKey(), row.getColumns().getLongValue("id", null));
			}
			Rows<String, String> resourceTypesRows = baseDao.readAllRows(ColumnFamily.RESOURCETYPES.getColumnFamily(), 0);
			resourceTypesCache = new LinkedHashMap<String, Object>();
			for (Row<String, String> row : resourceTypesRows) {
				resourceTypesCache.put(row.getKey(), row.getColumns().getLongValue("id", null));
			}
			Rows<String, String> categoryRows = baseDao.readAllRows(ColumnFamily.CATEGORY.getColumnFamily(), 0);
			categoryCache = new LinkedHashMap<String, Object>();
			for (Row<String, String> row : categoryRows) {
				categoryCache.put(row.getKey(), row.getColumns().getLongValue("id", null));
			}

			taxonomyCodeType = new LinkedHashMap<String, String>();

			ColumnList<String> taxonomyCodeTypeList = baseDao.readWithKey(ColumnFamily.TABLEDATATYPES.getColumnFamily(), "taxonomy_code", 0);
			for (int i = 0; i < taxonomyCodeTypeList.size(); i++) {
				taxonomyCodeType.put(taxonomyCodeTypeList.getColumnByIndex(i).getName(), taxonomyCodeTypeList.getColumnByIndex(i).getStringValue());
			}
			Rows<String, String> resourceFormatRows = baseDao.readAllRows(ColumnFamily.RESOURCEFORMAT.getColumnFamily(), 0);
			resourceFormatCache = new LinkedHashMap<String, Object>();
			for (Row<String, String> row : resourceFormatRows) {
				resourceFormatCache.put(row.getKey(), row.getColumns().getLongValue("id", null));
			}
			Rows<String, String> instructionalRows = baseDao.readAllRows(ColumnFamily.INSTRUCTIONAL.getColumnFamily(), 0);

			instructionalCache = new LinkedHashMap<String, Object>();
			for (Row<String, String> row : instructionalRows) {
				instructionalCache.put(row.getKey(), row.getColumns().getLongValue("id", null));
			}
			REPOPATH = baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "repo.path", DEFAULT_COLUMN, 0).getStringValue();
		} catch (Exception e) {
			logger.error("Exception : " + e);
		}
	}
	
	
	public CassandraConnectionProvider getConnectionProvider() {
		return connectionProvider;
	}

	public void setConnectionProvider(CassandraConnectionProvider connectionProvider) {
		this.connectionProvider = connectionProvider;
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
		return REPOPATH;
	}

}
