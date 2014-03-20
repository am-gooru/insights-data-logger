package org.kafka.event.microaggregator.dao;

import java.util.ArrayList;
import java.util.List;

import org.kafka.event.microaggregator.core.CassandraConnectionProvider;
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

	public String resourceType(String gooruOid){
		Rows<String, String> resources = null;
		String type = null;
		try {
			resources = getKeyspace().prepareQuery(dimResourceCF)
				.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
			 	.searchWithIndex().setRowLimit(1)
				.addExpression()
				.whereColumn("gooru_oid")
				.equals()
				.value(gooruOid).execute().getResult();
		} catch (ConnectionException e) {
			
			logger.info("Error while retieveing data : {}" ,e);
		}
		if(resources != null){
			for(Row<String, String> resource : resources){
				type =  resource.getColumns().getColumnByName("tye_name").getStringValue() ;
			 }
		}
		
		return gooruOid;
	}
}
