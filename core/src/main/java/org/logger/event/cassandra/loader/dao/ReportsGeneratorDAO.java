package org.logger.event.cassandra.loader.dao;

import org.ednovo.data.model.EventBuilder;

public interface ReportsGeneratorDAO {
	static ReportsGeneratorDAO instance() {
		return new ReportsGeneratorDAOImpl();
	}

	void eventProcessor(EventBuilder event);
}
