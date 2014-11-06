package org.logger.event.cassandra.loader;

public enum ESIndexices {
	
	EVENTLOGGERINFO("activity_catalog" , new String[] {"event_detail"}),
	
	CONTENTCATALOGINFO("content_catalog" , new String[] {"dim_resource"}),
	
	USERCATALOG("user_catalog_info" , new String[] {"dim_user"}),
	
	TAXONOMYCATALOG("taxonomy_catalog" , new String[] {"taxonomy_code"}),

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
