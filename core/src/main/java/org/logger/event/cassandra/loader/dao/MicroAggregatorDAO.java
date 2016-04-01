package org.logger.event.cassandra.loader.dao;

import org.ednovo.data.model.EventBuilder;

public interface MicroAggregatorDAO {
	
	static MicroAggregatorDAO instance(){
		return new MicroAggregatorDAOImpl();
	}
	
	void eventProcessor(EventBuilder event);
	
	void statAndRawUpdate(EventBuilder event);
}
