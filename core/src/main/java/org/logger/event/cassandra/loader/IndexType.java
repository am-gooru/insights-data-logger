package org.logger.event.cassandra.loader;

public enum IndexType {

	EVENTDETAIL("event_detail"),
	
	DIMRESOURCE("dim_resource"),
	
	DIMUSER("dim_user"),
	
	TAXONOMYCODE("taxonomy_code"),
	
	;
	
	String name;

	
	private IndexType(String name) {
		this.name = name;
	}


	public String getIndexType(){
		return name;
	}
}
