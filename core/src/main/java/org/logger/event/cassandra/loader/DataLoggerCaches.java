package org.logger.event.cassandra.loader;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
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
public class DataLoggerCaches implements Constants {

	private CassandraConnectionProvider connectionProvider;

	private BaseCassandraRepoImpl baseDao;

	public Map<String, String> cache = new HashMap<String, String>();

	public Map<String, Object> gooruTaxonomy = new HashMap<String, Object>();

	public Collection<String> pushingEvents = new ArrayList<String>();

	public Boolean canRunScheduler;

	public Boolean canRunIndexing;

	public Map<String, Object> licenseCache = new HashMap<String, Object>();

	public Map<String, Object> resourceTypesCache = new HashMap<String, Object>();

	public Map<String, Object> categoryCache = new HashMap<String, Object>();

	public Map<String, Object> resourceFormatCache = new HashMap<String, Object>();
	
	public Map<String, Object> instructionalCache = new HashMap<String, Object>();
	
	public Map<String, String> taxonomyCodeType = new HashMap<String, String>();

	public Map<String, Map<String, String>> kafkaConfigurationCache = new HashMap<String, Map<String,String>>();

	public Map<String, String> fieldDataTypes = new HashMap<String, String>();

	public Map<String, String> beFieldName = new HashMap<String, String>();

	public String REPOPATH = null;
	
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
		this.getProperties();
	}
	
	public void clearCache(){
		cache.clear();

		gooruTaxonomy.clear();

		pushingEvents.clear();;

		canRunScheduler = null;

		canRunIndexing = null;

		licenseCache.clear();

		resourceTypesCache.clear();

		categoryCache.clear();

		resourceFormatCache.clear();
		
		instructionalCache.clear();
		
		taxonomyCodeType.clear();

		kafkaConfigurationCache.clear();

		fieldDataTypes.clear();

		beFieldName.clear();

		REPOPATH = null;
		
		this.getProperties();
	}
	
	public void getProperties(){
		
		Rows<String, String> operators = baseDao.readAllRows(ColumnFamily.REALTIMECONFIG.getColumnFamily(), 0);
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
		this.setPushingEvents(pushingEvents);
		String host = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), SCH_HOST, 0).getStringValue(HOST, null);

		try {
			String localHost = "" + InetAddress.getLocalHost();
			logger.debug("localHost: " + localHost);
			logger.debug("Host: " + host);
			logger.debug("canRunScheduler before: " + canRunScheduler);
			if (localHost.contains(host)) {
				this.setCanRunScheduler(true);
			} else {
				this.setCanRunScheduler(false);
			}
			logger.debug("canRunScheduler after: " + canRunScheduler);
		} catch (UnknownHostException e) {
			logger.error("Exception : " + e);
		}

		String realTimeIndexing = baseDao.readWithKey(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), REAL_TIME_INDEXING, 0).getStringValue(DEFAULT_COLUMN, null);
		if (realTimeIndexing.equalsIgnoreCase(STOP)) {
			this.setCanRunIndexing(false);
		} else {
			this.setCanRunIndexing(true);
		}

			String[] kafkaMessager = new String[] { V2_KAFKA_CONSUMER, V2_KAFKA_LOG_WRITER_PRODUCER, V2_KAFKA_LOG_WRITER_CONSUMER, V2_KAFKA_MICRO_PRODUCER, V2_KAFKA_MICRO_CONSUMER };
			Rows<String, String> result = baseDao.readCommaKeyList(CONFIG_SETTINGS, 0, kafkaMessager);
			for (Row<String, String> row : result) {
				Map<String, String> properties = new HashMap<String, String>();
				for (Column<String> column : row.getColumns()) {
					properties.put(column.getName(), column.getStringValue());
				}
				kafkaConfigurationCache.put(row.getKey(), properties);
			}
			this.setKafkaConfigurationCache(kafkaConfigurationCache);
		
		Rows<String, String> fieldDescrption = baseDao.readAllRows(ColumnFamily.EVENTFIELDS.getColumnFamily(), 0);
		for (Row<String, String> row : fieldDescrption) {
			fieldDataTypes.put(row.getKey(), row.getColumns().getStringValue("description", null));
			beFieldName.put(row.getKey(), row.getColumns().getStringValue("be_column", null));
		}
		this.setFieldDataTypes(fieldDataTypes);
		this.setBeFieldName(beFieldName);

		Rows<String, String> licenseRows = baseDao.readAllRows(ColumnFamily.LICENSE.getColumnFamily(), 0);
		for (Row<String, String> row : licenseRows) {
			licenseCache.put(row.getKey(), row.getColumns().getLongValue("id", null));
		}
		this.setLicenseCache(licenseCache);
		;
		Rows<String, String> resourceTypesRows = baseDao.readAllRows(ColumnFamily.RESOURCETYPES.getColumnFamily(), 0);
		for (Row<String, String> row : resourceTypesRows) {
			resourceTypesCache.put(row.getKey(), row.getColumns().getLongValue("id", null));
		}
		this.setResourceTypesCache(resourceTypesCache);
		Rows<String, String> categoryRows = baseDao.readAllRows(ColumnFamily.CATEGORY.getColumnFamily(), 0);
		for (Row<String, String> row : categoryRows) {
			categoryCache.put(row.getKey(), row.getColumns().getLongValue("id", null));
		}
		this.setCategoryCache(categoryCache);
		ColumnList<String> taxonomyCodeTypeList = baseDao.readWithKey(ColumnFamily.TABLEDATATYPES.getColumnFamily(), "taxonomy_code", 0);
		for (int i = 0; i < taxonomyCodeTypeList.size(); i++) {
			taxonomyCodeType.put(taxonomyCodeTypeList.getColumnByIndex(i).getName(), taxonomyCodeTypeList.getColumnByIndex(i).getStringValue());
		}
		this.setTaxonomyCodeType(taxonomyCodeType);
		Rows<String, String> resourceFormatRows = baseDao.readAllRows(ColumnFamily.RESOURCEFORMAT.getColumnFamily(), 0);
		for (Row<String, String> row : resourceFormatRows) {
			resourceFormatCache.put(row.getKey(), row.getColumns().getLongValue("id", null));
		}
		this.setResourceFormatCache(resourceFormatCache);
		Rows<String, String> instructionalRows = baseDao.readAllRows(ColumnFamily.INSTRUCTIONAL.getColumnFamily(), 0);

		for (Row<String, String> row : instructionalRows) {
			instructionalCache.put(row.getKey(), row.getColumns().getLongValue("id", null));
		}
		this.setInstructionalCache(instructionalCache);
		REPOPATH = baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), "repo.path", DEFAULT_COLUMN, 0).getStringValue();
		this.setREPOPATH(REPOPATH);
		this.setCache(cache);
	}
	
	public CassandraConnectionProvider getConnectionProvider() {
		return connectionProvider;
	}

	public void setConnectionProvider(CassandraConnectionProvider connectionProvider) {
		this.connectionProvider = connectionProvider;
	}

	public Map<String, String> getCache() {
		return cache;
	}

	public void setCache(Map<String, String> cache) {
		this.cache = cache;
	}

	public Collection<String> getPushingEvents() {
		return pushingEvents;
	}

	public void setPushingEvents(Collection<String> pushingEvents) {
		this.pushingEvents = pushingEvents;
	}

	public Boolean getCanRunScheduler() {
		return canRunScheduler;
	}

	public void setCanRunScheduler(Boolean canRunScheduler) {
		this.canRunScheduler = canRunScheduler;
	}

	public Boolean getCanRunIndexing() {
		return canRunIndexing;
	}

	public void setCanRunIndexing(Boolean canRunIndexing) {
		this.canRunIndexing = canRunIndexing;
	}

	public Map<String, Object> getLicenseCache() {
		return licenseCache;
	}

	public void setLicenseCache(Map<String, Object> licenseCache) {
		this.licenseCache = licenseCache;
	}

	public Map<String, Object> getResourceTypesCache() {
		return resourceTypesCache;
	}

	public void setResourceTypesCache(Map<String, Object> resourceTypesCache) {
		this.resourceTypesCache = resourceTypesCache;
	}

	public Map<String, Object> getCategoryCache() {
		return categoryCache;
	}

	public void setCategoryCache(Map<String, Object> categoryCache) {
		this.categoryCache = categoryCache;
	}

	public Map<String, String> getTaxonomyCodeType() {
		return taxonomyCodeType;
	}

	public void setTaxonomyCodeType(Map<String, String> taxonomyCodeType) {
		this.taxonomyCodeType = taxonomyCodeType;
	}

	public Map<String, String> getBeFieldName() {
		return beFieldName;
	}

	public void setBeFieldName(Map<String, String> beFieldName) {
		this.beFieldName = beFieldName;
	}

	public Map<String, Object> getGooruTaxonomy() {
		return gooruTaxonomy;
	}

	public void setGooruTaxonomy(Map<String, Object> gooruTaxonomy) {
		this.gooruTaxonomy = gooruTaxonomy;
	}

	public Map<String, Map<String, String>> getKafkaConfigurationCache() {
		return kafkaConfigurationCache;
	}

	public void setKafkaConfigurationCache(Map<String, Map<String, String>> kafkaConfigurationCache) {
		this.kafkaConfigurationCache = kafkaConfigurationCache;
	}

	public void setFieldDataTypes(Map<String, String> fieldDataTypes) {
		this.fieldDataTypes = fieldDataTypes;
	}
	
	public Map<String, String> getFieldDataTypes() {
		return fieldDataTypes;
	}

	public Map<String, Object> getResourceFormatCache() {
		return resourceFormatCache;
	}

	public void setResourceFormatCache(Map<String, Object> resourceFormatCache) {
		this.resourceFormatCache = resourceFormatCache;
	}

	public Map<String, Object> getInstructionalCache() {
		return instructionalCache;
	}

	public void setInstructionalCache(Map<String, Object> instructionalCache) {
		this.instructionalCache = instructionalCache;
	}

	public String getREPOPATH() {
		return REPOPATH;
	}

	public void setREPOPATH(String rEPOPATH) {
		REPOPATH = rEPOPATH;
	}

}
