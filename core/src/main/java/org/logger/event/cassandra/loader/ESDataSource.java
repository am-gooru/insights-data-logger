package org.logger.event.cassandra.loader;

public enum ESDataSource {
		ACTIVITY("activity",ESIndexices.EVENTLOGGERINFO),
		CONTENT("content",ESIndexices.CONTENTCATALOGINFO),
		USERDATA("userdata",ESIndexices.USERCATALOG),
		TAXONOMYDATA("taxonomydata",ESIndexices.TAXONOMYCATALOG);
		
		String dataSource;

		ESIndexices index;
		
		private ESDataSource(String dataSource, ESIndexices index) {
			this.dataSource = dataSource;
			this.index = index;
		}

		public String getDataSource() {
			return dataSource;
		}
		public ESIndexices getIndex() {
			return index;
		}
		
		
}



