package org.logger.event.cassandra.loader;

public enum ColumnFamily {

	APIKEY("app_api_key"),
	
	EVENTDETAIL("event_detail");
	
	;
	
	String name;

	
	private ColumnFamily(String name) {
		this.name = name;
	}


	public String getColumnFamily(){
		return name;
	}

}
