package org.logger.event.cassandra.loader.dao;

import com.netflix.astyanax.model.Rows;

public class CallBackRowsImpl implements CallBackRows {

	private Rows<String, String> rows;
	
	@Override
	public void getRows(Rows<String, String> rows) {
		this.rows = rows;
	}
}
