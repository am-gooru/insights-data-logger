package org.logger.event.cassandra.loader.dao;
import org.logger.event.cassandra.loader.CassandraConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.serializers.StringSerializer;

public class DimResourceDAOImpl extends BaseDAOCassandraImpl implements  DimResourceDAO{

	 private static final Logger logger = LoggerFactory.getLogger(DimResourceDAOImpl.class);

	 private final ColumnFamily<String, String> dimResourceCF;
	 
	 private static final String CF_DIM_RESOURCE = "dim_resource";
	    
	public DimResourceDAOImpl(CassandraConnectionProvider connectionProvider) {
		super(connectionProvider);
		dimResourceCF = new ColumnFamily<String, String>(
				CF_DIM_RESOURCE, // Column Family Name
                StringSerializer.get(), // Key Serializer
                StringSerializer.get()); // Column Serializer
        
	}

	public Rows<String, String> getRowsByIndexedColumn(String value,String whereColumn){
		Rows<String, String> resources = null;
		try {
			resources = getKeyspace().prepareQuery(dimResourceCF)
				.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
			 	.searchWithIndex().setRowLimit(1)
				.addExpression()
				.whereColumn(whereColumn)
				.equals()
				.value(value).execute().getResult();
		} catch (ConnectionException e) {
			
			logger.info("Error while retieveing data : {}" ,e);
		}		
		return resources;
	}
}
