package org.logger.event.cassandra.loader.dao;

import org.ednovo.data.model.EventBuilder;

public interface ReComputationDAO {
	static ReComputationDAO instance() {
		return new ReComputationDAOImpl();
	}

	void reComputeData(EventBuilder event);
}
