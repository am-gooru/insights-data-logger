package org.logger.event.cassandra.loader.dao;

import org.ednovo.data.model.EventBuilder;

public interface EventsUpdateDAO {
	static EventsUpdateDAO instance() {
		return new EventsUpdateDAOImpl();
	}
	void eventsHadler(EventBuilder event);
}
