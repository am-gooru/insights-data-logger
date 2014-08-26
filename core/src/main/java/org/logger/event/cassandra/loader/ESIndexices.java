package org.logger.event.cassandra.loader;

public enum ESIndexices {
	
	EVENTLOGGERINSIGHTS("event_logger_insights" , new String[] {"event_detail" , "dim_events_list"}),
	
	EVENTLOGGER("event_logger" , new String[] {"event_detail" , "dim_events_list"}),
	
	;
	
	String name;
	
	String[] type;
	
	private ESIndexices(String name, String[] type) {
		this.name = name;
		this.type = type;
	}
	
	private ESIndexices(String name) {
		this.name = name;
		this.type = new String[]{name};
	}

	public String getIndex() {
		return name;
	}

	public String[] getType() {
		return type;
	}
}
