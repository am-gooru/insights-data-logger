package org.logger.event.cassandra.loader.dao;

import java.util.Map;

public interface MicroAggregatorDAO {
	
	static MicroAggregatorDAO instance(){
		return new MicroAggregatorDAOImpl();
	}
	void eventProcessor(Map<String, Object> eventMap);
}
