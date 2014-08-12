package org.logger.event.cassandra.loader;

public enum IndexType {

	EVENTDETAIL("event_detail"),
	
	;
	
	String name;

	
	private IndexType(String name) {
		this.name = name;
	}


	public String getIndexType(){
		return name;
	}
}
