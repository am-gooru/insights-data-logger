package org.logger.event.cassandra.loader.dao;

import java.util.Map;

public interface MicroAggregatorDAO {
	void eventProcessor(Map<String, Object> eventMap);
}
