package org.logger.event.cassandra.loader;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.logger.event.cassandra.loader.dao.BaseCassandraRepoImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;

@Repository
public final class DataLoggerCaches implements Constants {

	private CassandraConnectionProvider connectionProvider;

	private BaseCassandraRepoImpl baseDao;

	public Map<String, String> cache;

	public Map<String, Object> gooruTaxonomy;

	public Collection<String> pushingEvents;

	public boolean canRunScheduler = false;

	public boolean canRunIndexing = true;

	public Map<String, Object> licenseCache;

	public Map<String, Object> resourceTypesCache;

	public Map<String, Object> categoryCache;

	public Map<String, String> taxonomyCodeType;

	public Map<String, Map<String, String>> kafkaConfigurationCache;

	public Map<String, String> fieldDataTypes = null;

	public Map<String, String> beFieldName = null;

	private static final Logger logger = LoggerFactory.getLogger(CassandraDataLoader.class);

	/**
	 * Loading information that requires while tomcat service start up
	 */
	public void init() {

		this.setConnectionProvider(new CassandraConnectionProvider());
		this.getConnectionProvider().init(null);

		baseDao = new BaseCassandraRepoImpl(getConnectionProvider());

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

		try {
			String localHost = "" + InetAddress.getLocalHost();
			logger.info("Host : " + localHost);
			if (localHost.contains(host)) {
				canRunScheduler = true;
			}
		} catch (UnknownHostException e) {
			logger.error("Exception : " + e);
		}

		String realTimeIndexing = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), REAL_TIME_INDEXING, 0).getStringValue(DEFAULT_COLUMN, null);
		if (realTimeIndexing.equalsIgnoreCase(STOP)) {
			canRunIndexing = false;
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
	}

	public Map<String, String> getCaches() {
		return cache;
	}

	public CassandraConnectionProvider getConnectionProvider() {
		return connectionProvider;
	}

	public void setConnectionProvider(CassandraConnectionProvider connectionProvider) {
		this.connectionProvider = connectionProvider;
	}

	/**
	 * Returns boolean value to scheduler in servers
	 * 
	 * @return
	 */
	public boolean canRunScheduler() {
		return canRunScheduler;
	}

	/**
	 * Returns boolean value to run activity index in servers
	 * 
	 * @return
	 */
	public boolean canRunIndexing() {
		return canRunIndexing;
	}

	/**
	 * Returns kafka configurations
	 * 
	 * @return
	 */
	public Map<String, Map<String, String>> getKafkaConfigurationCache() {
		return kafkaConfigurationCache;
	}

	/**
	 * Returns all licenses ids
	 * 
	 * @return
	 */
	public Map<String, Object> getLicense() {
		return licenseCache;
	}

	/**
	 * Returns all resource type ids
	 * 
	 * @return
	 */
	public Map<String, Object> getResourceTypes() {
		return resourceTypesCache;
	}

	/**
	 * Returns all category ids
	 * 
	 * @return
	 */
	public Map<String, Object> getCategory() {
		return categoryCache;
	}

	/**
	 * Returns all the taxonomy codes
	 * 
	 * @return
	 */
	public Map<String, String> getTaxonomyCodes() {
		return taxonomyCodeType;
	}

	/**
	 * Returns all the back end data types in column family
	 * 
	 * @return
	 */
	public Map<String, String> getFieldDataTypes() {
		return fieldDataTypes;
	}

	/**
	 * Returns all the back end referral understandable names
	 * 
	 * @return
	 */
	public Map<String, String> getBeFieldNames() {
		return beFieldName;
	}

}
