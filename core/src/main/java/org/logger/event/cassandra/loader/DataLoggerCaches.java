package org.logger.event.cassandra.loader;

import java.util.HashMap;
import java.util.Map;

import org.logger.event.cassandra.loader.dao.BaseCassandraRepo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataLoggerCaches {

	private BaseCassandraRepo baseDao;

	public static Map<String, Map<String, String>> kafkaConfigurationCache = new HashMap<String, Map<String, String>>();;

	private static final Logger LOG = LoggerFactory.getLogger(CassandraDataLoader.class);

	/**
	 * Loading information that requires while tomcat service start up
	 */
	public DataLoggerCaches() {
		// TODO Auto-generated constructor stub
		baseDao = BaseCassandraRepo.instance();
		init();
	}

	private void init() {
		try {

			kafkaConfigurationCache = new HashMap<String, Map<String, String>>();
			/*String[] kafkaMessager = new String[] { Constants.V2_KAFKA_CONSUMER, Constants.V2_KAFKA_LOG_WRITER_PRODUCER, Constants.V2_KAFKA_LOG_WRITER_CONSUMER, Constants.V2_KAFKA_MICRO_PRODUCER,
					Constants.V2_KAFKA_MICRO_CONSUMER };
			//It will be reconstructed after 3.0 release
			//Rows<String, String> result = baseDao.readCommaKeyList(Constants.CONFIG_SETTINGS, kafkaMessager);
			Rows<String, String> result = null;
			if (result != null) {
				for (Row<String, String> row : result) {
					Map<String, String> properties = new HashMap<String, String>();
					for (Column<String> column : row.getColumns()) {
						properties.put(column.getName(), column.getStringValue());
					}
					kafkaConfigurationCache.put(row.getKey(), properties);
				}
			}*/
		} catch (Exception e) {
			LOG.error("Exception : ", e);
		}
	}

	public static Map<String, Map<String, String>> getKafkaConfigurationCache() {
		return kafkaConfigurationCache;
	}
}
