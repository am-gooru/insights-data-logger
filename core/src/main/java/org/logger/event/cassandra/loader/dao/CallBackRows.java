package org.logger.event.cassandra.loader.dao;

import com.netflix.astyanax.model.Rows;

public interface CallBackRows {
	public void getRows(Rows<String, String> rows);
}
