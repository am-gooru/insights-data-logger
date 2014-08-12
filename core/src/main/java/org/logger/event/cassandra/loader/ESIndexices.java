package org.logger.event.cassandra.loader;

public enum ESIndexices {
	
	EVENTLOGGERINSIGHTS("event_logger_insights"),
	
	;
	
	String name;

	
	private ESIndexices(String name) {
		this.name = name;
	}


	public String getESIndex(){
		return name;
	}
}
