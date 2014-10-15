package org.logger.event.cassandra.loader.dao;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.ednovo.data.model.EventData;
import org.ednovo.data.model.EventObject;
import org.logger.event.cassandra.loader.CassandraConnectionProvider;
import org.logger.event.cassandra.loader.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;

import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.ExceptionCallback;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.IndexQuery;
import com.netflix.astyanax.retry.ConstantBackoff;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.util.RangeBuilder;
import com.netflix.astyanax.util.TimeUUIDUtils;

public class BaseCassandraRepoImpl extends BaseDAOCassandraImpl implements Constants {
	
	private static CassandraConnectionProvider connectionProvider;
	
	private static final Logger logger = LoggerFactory.getLogger(BaseCassandraRepoImpl.class);
	
	
	public BaseCassandraRepoImpl(CassandraConnectionProvider connectionProvider) {
		super(connectionProvider);
		// TODO Auto-generated constructor stub
	}

	public BaseCassandraRepoImpl() {
		super(connectionProvider);
	}

    public Column<String> readWithKeyColumn(String cfName,String key,String columnName,int retryCount){
    	
    	Column<String> result = null;
    	ColumnList<String> columnList = null;
    	try {
    		 columnList = getKeyspace().prepareQuery(this.accessColumnFamily(cfName))
                    .setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5))
                    .getKey(key)
                    .execute()
                    .getResult()
                    ;

        } catch (ConnectionException e) {
        	if(retryCount < 6){
        		retryCount++;
        		return readWithKeyColumn(cfName,key,columnName ,retryCount++);
        	}else{
        		e.printStackTrace();
        	}
        }
    	if(columnList != null){
    		result = columnList.getColumnByName(columnName);
    	}
    	return result;}

    public ColumnList<String> readWithKeyColumnList(String cfName,String key,Collection<String> columnList,int retryCount){
        
    	ColumnList<String> result = null;
    	try {
              result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName))
                    .setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5))
                    .getKey(key)
                    .withColumnSlice(columnList)
                    .execute()
                    .getResult()
                    ;

        } catch (ConnectionException e) {
        	if(retryCount < 6){
        		retryCount++;
        		return readWithKeyColumnList(cfName,key,columnList,retryCount++);
        	}else{
        		e.printStackTrace();
        	}
        }
    	
    	return result;
    }
    
    public Rows<String, String> readWithKeyListColumnList(String cfName,Collection<String> keys,Collection<String> columnList ,int retryCount){
        
    	Rows<String, String> result = null;
    	try {
              result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName))
                    .setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5))
                    .getKeySlice(keys)
                    .withColumnSlice(columnList)
                    .execute()
                    .getResult()
                    ;

        } catch (ConnectionException e) {
        	if(retryCount < 6){
        		retryCount++;
        		return readWithKeyListColumnList(cfName,keys,columnList ,retryCount++);
        	}else{
        		e.printStackTrace();
        	}
        }
    	
    	return result;
    }
    public ColumnList<String> readWithKey(String cfName,String key,int retryCount){
        
    	ColumnList<String> result = null;
    	try {
              result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName))
                    .setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5))
                    .getKey(key)
                    .execute()
                    .getResult()
                    ;

        } catch (ConnectionException e) {
        	if(retryCount < 6){
        		retryCount++;
        		return readWithKey(cfName,key,retryCount++);
        	}else{
        		e.printStackTrace();
        		logger.info("Error in while read : " + e);
        	}
        }
    	
    	return result;
    }
    
    public Rows<String, String> readWithKeyList(String cfName,Collection<String> key,int retryCount){
        
    	Rows<String, String> result = null;
    	try {
              result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName))
                    .setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5))
                    .getKeySlice(key)
                    .execute()
                    .getResult()
                    ;

        } catch (ConnectionException e) {
        	if(retryCount < 6){
        		retryCount++;
        		return readWithKeyList(cfName, key,retryCount++);
        	}else{
        		e.printStackTrace();
        	}
        }
    	
    	return result;
    }

    public Rows<String, String> readCommaKeyList(String cfName,String... key){

    	Rows<String, String> result = null;
    	try {
              result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName))
                    .setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5))
                    .getKeySlice(key)
                    .execute()
                    .getResult()
                    ;

        } catch (ConnectionException e) {
            logger.info("Error while fetching data from method : readWithKey {}", e);
        }
    	
    	return result;
    }
    
    public Rows<String, String> readIterableKeyList(String cfName,Iterable<String> keys){

    	Rows<String, String> result = null;
    	try {
              result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName))
                    .setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5))
                    .getKeySlice(keys)
                    .execute()
                    .getResult()
                    ;

        } catch (ConnectionException e) {
            logger.info("Error while fetching data from method : readWithKey {} ", e);
        }
    	
    	return result;
    }
    
    public Rows<String, String> readIndexedColumn(String cfName,String columnName,String value ,int retryCount){
    	
    	Rows<String, String> result = null;
    	try{
    		result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName))
			.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5))
		 	.searchWithIndex()
			.addExpression()
			.whereColumn(columnName)
			.equals()
			.value(value)
			.execute()
			.getResult()
			;
	    	
    	} catch(ConnectionException e){
    		if(retryCount < 6){
    			retryCount++;
        		return readIndexedColumn(cfName,columnName,value ,retryCount++);
        	}else{
        		e.printStackTrace();
        	}    		
    	}
    	return result;
    }
    
    public Rows<String, String> readIndexedColumn(String cfName,String columnName,long value,int retryCount){
    	
    	Rows<String, String> result = null;
    	try{
    		result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName))
			.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5))
		 	.searchWithIndex()
			.addExpression()
			.whereColumn(columnName)
			.equals()
			.value(value)
			.execute()
			.getResult()
			;
	    	
    	} catch(ConnectionException e){
    		if(retryCount < 6){
    			retryCount++;
        		return readIndexedColumn(cfName,columnName,value ,retryCount++);
        	}else{
        		e.printStackTrace();
        	}    		    		
    	}
    	return result;
    }

    public Rows<String, String> readIndexedColumnLastNrows(String cfName ,String columnName,String value, Integer rowsToRead,int retryCount) {
    	
		Rows<String, String> result = null;
    	try {
    		result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName))
			 			   .setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5))
					 	   .searchWithIndex().autoPaginateRows(true)
					 	   .setRowLimit(rowsToRead.intValue())
					 	   .addExpression().whereColumn(columnName)
					 	   .equals().value(value).execute().getResult();
		} catch (ConnectionException e) {
			if(retryCount < 6){
				retryCount++;
        		return readIndexedColumnLastNrows(cfName ,columnName,value,rowsToRead,retryCount++);
        	}else{
        		e.printStackTrace();
        	}    		    		
		}
    	
    	return result;
	}

    public ColumnList<String> readKeyLastNColumns(String cfName,String key, Integer columnsToRead , int retryCount) {
    	
    	ColumnList<String> result = null;
    	try {
    		result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName))
    		.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5))
    		.getKey(key)
    		.withColumnRange(new RangeBuilder().setReversed().setLimit(columnsToRead.intValue()).build())
    		.execute().getResult();
    	} catch (ConnectionException e) {
    		if(retryCount < 6){
        		return readKeyLastNColumns(cfName,key, columnsToRead , retryCount);
        	}else{
        		e.printStackTrace();
        	}
    	}
    	
    	return result;
    }
    
    public long getCount(String cfName,String key){

    	Integer columns = null;
		
    	try {
			columns = getKeyspace().prepareQuery(this.accessColumnFamily(cfName))
					.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5))
					.getKey(key)
					.getCount()
					.execute().getResult();
		} catch (ConnectionException e) {
			e.printStackTrace();
		}
		
		return columns.longValue();
	}
    
    public Rows<String, String> readIndexedColumnList(String cfName,Map<String,String> columnList,int retryCount){
    	
    	Rows<String, String> result = null;
    	IndexQuery<String, String> query = null;

    	query = getKeyspace().prepareQuery(this.accessColumnFamily(cfName))
    				.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5))
    				.searchWithIndex();
    	
    	for (Map.Entry<String, String> entry : columnList.entrySet()) {
    		query.addExpression().whereColumn(entry.getKey()).equals().value(entry.getValue())
    	    ;
    	}
    	
    	try{
    		result = query.execute().getResult()
			;
    	} catch(ConnectionException e){
    		if(retryCount < 6){
    			retryCount++;
        		return readIndexedColumnList(cfName,columnList,retryCount++);
        	}else{
        		e.printStackTrace();
        		logger.info("Error in while read : " + e);
        	}    		
    	}

    	return result;
    }
    
    public boolean isRowKeyExists(String cfName,String key,int retryCount) {

		try {
			return getKeyspace().prepareQuery(this.accessColumnFamily(cfName))
				.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5))
				.getKey(key).execute()
				.getResult()
				.isEmpty();
		} catch (ConnectionException e) {
			if(retryCount < 6){
				retryCount++;
        		return isRowKeyExists(cfName,key,retryCount++);
        	}else{
        		e.printStackTrace();
        	}
		}
		return false;
	}
    
    public boolean isValueExists(String cfName,Map<String,Object> columns) {

		try {
			IndexQuery<String, String> cfQuery = getKeyspace().prepareQuery(this.accessColumnFamily(cfName))
				.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5)).searchWithIndex();
			
			for(Map.Entry<String, Object> map : columns.entrySet()){
				cfQuery.addExpression().whereColumn(map.getKey()).equals().value(map.getValue().toString());
			}
			return cfQuery.execute().getResult().isEmpty();
		} catch (ConnectionException e) {
			logger.info("Error while fetching data from method : isRowKeyExists {} ", e);
		}
		return false;
	}
    
    public Collection<String> getKey(String cfName,Map<String,Object> columns) {

		try {
			
			IndexQuery<String, String> cfQuery = getKeyspace().prepareQuery(this.accessColumnFamily(cfName))
				.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5)).searchWithIndex();
			
			for(Map.Entry<String, Object> map : columns.entrySet()){
				cfQuery.addExpression().whereColumn(map.getKey()).equals().value(map.getValue().toString());
			}
			Rows<String, String> rows = cfQuery.execute().getResult();
			return rows.getKeys();
		} catch (ConnectionException e) {
			logger.info("Error while fetching data from method : isRowKeyExists {} ", e);
		}
		return new ArrayList();
	}
    
	public ColumnList<String> readColumnsWithPrefix(String cfName,String rowKey, String startColumnNamePrefix, String endColumnNamePrefix, Integer rowsToRead){
		ColumnList<String> result = null;
		String startColumnPrefix = null;
		String endColumnPrefix = null;
		startColumnPrefix = startColumnNamePrefix+"~\u0000";
		endColumnPrefix = endColumnNamePrefix+"~\uFFFF";
	
		try {
			result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName))
			.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5))
			.getKey(rowKey) .withColumnRange(new RangeBuilder().setLimit(rowsToRead)
			.setStart(startColumnPrefix)
			.setEnd(endColumnPrefix)
			.build()).execute().getResult();
		} catch (ConnectionException e) {
			logger.info("Error while fetching data from method : readColumnsWithPrefix {} ", e);
		} 
		
		return result;
	}
	
    public Rows<String, String> readAllRows(String cfName,int retryCount){
    	
    	Rows<String, String> result = null;
		try {
			result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName))
					.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5))
					.getAllRows()
					.withColumnRange(new RangeBuilder().setMaxSize(10).build())
			        .setExceptionCallback(new ExceptionCallback() {
			        @Override
			        public boolean onException(ConnectionException e) {
			                 try {
			                     Thread.sleep(1000);
			                 } catch (InterruptedException e1) {
			                 }
			                 return true;
			             }})
			        .execute().getResult();
		} catch (ConnectionException e) {
			if(retryCount < 6){
				retryCount++;
        		return readAllRows(cfName,retryCount++);
        	}else{
        		e.printStackTrace();
        	}
		}
		
		return result;
	}
    
    
    public void saveBulkStringList(String cfName, String key,Map<String,String> columnValueList) {

        MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(WRITE_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));

        for (Map.Entry<String,String> entry : columnValueList.entrySet()) {
    			m.withRow(this.accessColumnFamily(cfName), key).putColumnIfNotNull(entry.getKey(), entry.getValue(), null);
    	    ;
    	}

        try {
            m.execute();
        } catch (ConnectionException e) {
            logger.info("Error while save in method : saveBulkStringList {}", e);
        }
    }
    
    public void saveBulkLongList(String cfName, String key,Map<String,Long> columnValueList) {

        MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(WRITE_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));

        for (Map.Entry<String,Long> entry : columnValueList.entrySet()) {
    			m.withRow(this.accessColumnFamily(cfName), key).putColumnIfNotNull(entry.getKey(), entry.getValue(), null);
    	    ;
    	}

        try {
            m.execute();
        } catch (ConnectionException e) {
            logger.info("Error while save in method : saveBulkLongList {}", e);
        }
    }
    
    public void saveBulkList(String cfName, String key,Map<String,Object> columnValueList) {
    	
    	MutationBatch mutation = getKeyspace().prepareMutationBatch().setConsistencyLevel(WRITE_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));
    	
    	ColumnListMutation<String> m = mutation.withRow(this.accessColumnFamily(cfName), key);
            	
        for (Entry<String, Object> entry : columnValueList.entrySet()) {
        	if(entry.getValue().getClass().getSimpleName().equalsIgnoreCase("String")){        		
        		m.putColumnIfNotNull(entry.getKey(), String.valueOf(entry.getValue()), null);
        	}
        	if(entry.getValue().getClass().getSimpleName().equalsIgnoreCase("Integer")){        		
        		m.putColumnIfNotNull(entry.getKey(), Integer.valueOf(String.valueOf(entry.getValue())));
        	}
        	if(entry.getValue().getClass().getSimpleName().equalsIgnoreCase("Long")){        		
        		m.putColumnIfNotNull(entry.getKey(), Long.valueOf(String.valueOf(entry.getValue())));
        	}
        	if(entry.getValue().getClass().getSimpleName().equalsIgnoreCase("Boolean")){        		
        		m.putColumnIfNotNull(entry.getKey(), Boolean.valueOf(String.valueOf(entry.getValue())));
        	}
    	}
        try {
        	mutation.execute();	
        } catch (ConnectionException e) {
            logger.info("Error while save in method : saveBulkList {}", e);
        }
    }
    public void saveStringValue(String cfName, String key,String columnName,String value) {

        MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(WRITE_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));

        m.withRow(this.accessColumnFamily(cfName), key).putColumnIfNotNull(columnName, value, null);

        try {
            m.execute();
        } catch (ConnectionException e) {
            logger.info("Error while save in method : saveStringValue {}", e);
        }
    }

    public void saveStringValue(String cfName, String key,String columnName,String value,int expireTime) {

        MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(WRITE_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));

        m.withRow(this.accessColumnFamily(cfName), key).putColumnIfNotNull(columnName, value, expireTime);

        try {
            m.execute();
        } catch (ConnectionException e) {
            logger.info("Error while save in method : saveStringValue {}", e);
        }
    }
    public void saveLongValue(String cfName, String key,String columnName,long value,int expireTime) {

        MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(CONSISTENCY_LEVEL_ONE).withRetryPolicy(new ConstantBackoff(2000, 5));

        m.withRow(this.accessColumnFamily(cfName), key).putColumnIfNotNull(columnName, value, expireTime);

        try {
            m.execute();
        } catch (ConnectionException e) {
            logger.info("Error while save in method : saveLongValue {}", e);
        }
    }
    
    public void saveLongValue(String cfName, String key,String columnName,long value) {

        MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(CONSISTENCY_LEVEL_ONE).withRetryPolicy(new ConstantBackoff(2000, 5));

        m.withRow(this.accessColumnFamily(cfName), key).putColumnIfNotNull(columnName, value, null);

        try {
            m.execute();
        } catch (ConnectionException e) {
            logger.info("Error while save in method : saveLongValue {}", e);
        }
    }
    
    public void saveValue(String cfName, String key,String columnName,Object value) {

        MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(WRITE_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));
        if(value.getClass().getSimpleName().equalsIgnoreCase("String")){        		
    		m.withRow(this.accessColumnFamily(cfName), key).putColumnIfNotNull(columnName, String.valueOf(value), null);
    	}
    	if(value.getClass().getSimpleName().equalsIgnoreCase("Integer")){        		
    		m.withRow(this.accessColumnFamily(cfName), key).putColumnIfNotNull(columnName, Integer.valueOf(""+value), null);
    	}
    	if(value.getClass().getSimpleName().equalsIgnoreCase("Long")){        		
    		m.withRow(this.accessColumnFamily(cfName), key).putColumnIfNotNull(columnName, Long.valueOf(""+value), null);
    	}
    	
    	if(value.getClass().getSimpleName().equalsIgnoreCase("Boolean")){        		
    		m.withRow(this.accessColumnFamily(cfName), key).putColumnIfNotNull(columnName, Boolean.valueOf(""+value), null);
    	}else{
    		m.withRow(this.accessColumnFamily(cfName), key).putColumnIfNotNull(columnName, String.valueOf(value), null);
    	}
    	
	    ;
	
	    
        try {
            m.execute();
        } catch (ConnectionException e) {
            logger.info("Error while save in method : saveLongValue {}", e);
        }
    }
    public void increamentCounter(String cfName, String key,String columnName,long value) {

        MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(WRITE_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));

        m.withRow(this.accessColumnFamily(cfName), key)
        .incrementCounterColumn(columnName, value);

        try {
            m.execute();
        } catch (ConnectionException e) {
            logger.info("Error while save in method : saveLongValue {}", e);
        }
    }
    
    public void generateCounter(String cfName,String key,String columnName, long value ,MutationBatch m) {
        m.withRow(this.accessColumnFamily(cfName), key)
        .incrementCounterColumn(columnName, value);
    }
    
    public void generateNonCounter(String columnName, Object value ,ColumnListMutation<String> m) {
    	
        if(value.getClass().getSimpleName().equalsIgnoreCase("String")){        		
    		m.putColumnIfNotNull(columnName, String.valueOf(value), null);
    	}
    	if(value.getClass().getSimpleName().equalsIgnoreCase("Integer")){        		
    		m.putColumnIfNotNull(columnName, Integer.valueOf(String.valueOf(value)));
    	}
    	if(value.getClass().getSimpleName().equalsIgnoreCase("Long")){     
    		m.putColumnIfNotNull(columnName,Long.valueOf(String.valueOf(value)));
    	}
    	if(value.getClass().getSimpleName().equalsIgnoreCase("Boolean")){        		
    		m.putColumnIfNotNull(columnName, Boolean.valueOf(String.valueOf(value)));
    	}
    }
    
    public void generateNonCounter(String cfName,String key,String columnName, String value ,MutationBatch m) {
        m.withRow(this.accessColumnFamily(cfName), key)
        .putColumnIfNotNull(columnName, value);
    }
    
    public void generateNonCounter(String cfName,String key,String columnName, long value ,MutationBatch m) {
        m.withRow(this.accessColumnFamily(cfName), key)
        .putColumnIfNotNull(columnName, value);
    }
    public void generateNonCounter(String cfName,String key,String columnName, Integer value ,MutationBatch m) {
        m.withRow(this.accessColumnFamily(cfName), key)
        .putColumnIfNotNull(columnName, value);
    }
    
    public void generateNonCounter(String cfName,String key,String columnName, Boolean value ,MutationBatch m) {
        m.withRow(this.accessColumnFamily(cfName), key)
        .putColumnIfNotNull(columnName, value);
    }
    
    public void generateTTLColumns(String cfName,String key,String columnName, long value,int expireTime ,MutationBatch m) {
        m.withRow(this.accessColumnFamily(cfName), key)
        .putColumnIfNotNull(columnName, value,expireTime);
    }
    public void generateTTLColumns(String cfName,String key,String columnName, String value,int expireTime ,MutationBatch m) {
        m.withRow(this.accessColumnFamily(cfName), key)
        .putColumn(columnName, value,expireTime);
    }
    
    public void  deleteAll(String cfName){
		try {
			getKeyspace().truncateColumnFamily(this.accessColumnFamily(cfName));
		} catch (ConnectionException e) {
			 logger.info("Error while deleting rows in method :deleteAll {} ",e);
		} 
    }
    
    public void  deleteRowKey(String cfName,String key){
    	MutationBatch m = getKeyspace().prepareMutationBatch();
		try {
			m.withRow(this.accessColumnFamily(cfName), key)
			.delete()
			;
			
			m.execute();
		} catch (ConnectionException e) {
			 logger.info("Error while deleting rows in method :deleteRowKey {} ",e);
		} 
    }
    
    public void  deleteColumn(String cfName,String key,String columnName){
    	MutationBatch m = getKeyspace().prepareMutationBatch();
		try {
			m.withRow(this.accessColumnFamily(cfName), key)
			.deleteColumn(columnName)
			;
			m.execute();
		} catch (ConnectionException e) {
			 logger.info("Error while deleting rows in method :deleteColumn {} ",e);
		} 
    }
    public ColumnFamily<String, String> accessColumnFamily(String columnFamilyName) {

		ColumnFamily<String, String> aggregateColumnFamily;

		aggregateColumnFamily = new ColumnFamily<String, String>(columnFamilyName, StringSerializer.get(), StringSerializer.get());

		return aggregateColumnFamily;
	}

    
    /*
     *These are custom methods which is supporting in out application
     * 
     */
    
    public String saveEvent(String cfName , EventData eventData) {
    	String key = null;
    	if(eventData.getEventId() == null){
    		UUID eventKeyUUID = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
    		key = eventKeyUUID.toString();
    	}else{
    		key	= eventData.getEventId(); 
    	}
    	String	appOid = "GLP";

    	String gooruOid = eventData.getContentGooruId();
    	
    	if(gooruOid == null){
    		gooruOid = eventData.getGooruOId();
    	}
    	if(gooruOid == null){
    		gooruOid = eventData.getGooruId();
    	}
    	if((gooruOid == null || gooruOid.isEmpty()) && eventData.getResourceId() != null){
    		gooruOid = eventData.getResourceId();
    	}
    	String eventValue = eventData.getQuery();
    	if(eventValue == null){
    		eventValue = "NA";
    	}
    	String parentGooruOid = eventData.getParentGooruId();
    	if((parentGooruOid == null || parentGooruOid.isEmpty()) && eventData.getCollectionId() != null){
    		parentGooruOid = eventData.getCollectionId();
    	}
    	if(parentGooruOid == null || parentGooruOid.isEmpty()){
    		parentGooruOid = "NA";
    	}
    	if(gooruOid == null || gooruOid.isEmpty()){
    		gooruOid = "NA";
    	}	
    	String organizationUid  = eventData.getOrganizationUid();
    	if(organizationUid == null){
    		organizationUid = "NA";
    	}
    	String GooruUId = eventData.getGooruUId();

    	String appUid = appOid+"~"+gooruOid;    	
        Date dNow = new Date();
        SimpleDateFormat ft = new SimpleDateFormat("yyyyMMddkkmm");
        String date = ft.format(dNow).toString();

        String trySeq= null;
        String attemptStatus= null;
        String answereIds= null;
        
        if(eventData.getAttemptTrySequence() !=null){
        	trySeq = eventData.getAttemptTrySequence().toString();
        }
        if( eventData.getAttemptStatus() != null){
        	attemptStatus = eventData.getAttemptStatus().toString();
        }
        if(eventData.getAnswerId() != null){
        	answereIds = eventData.getAnswerId().toString();
        }
        // Inserting data
        MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(WRITE_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));
             
        m.withRow(this.accessColumnFamily(cfName), key)
                .putColumn("date_time", date, null)
                .putColumnIfNotNull("start_time", eventData.getStartTime(), null)
                .putColumnIfNotNull("user_ip", eventData.getUserIp(), null)
                .putColumnIfNotNull("fields", eventData.getFields(), null)
                .putColumnIfNotNull("user_agent", eventData.getUserAgent(), null)
                .putColumnIfNotNull("session_token",eventData.getSessionToken(), null)
                .putColumnIfNotNull("end_time", eventData.getEndTime(), null)
                .putColumnIfNotNull("content_gooru_oid", gooruOid, null)
                .putColumnIfNotNull("parent_gooru_oid",parentGooruOid, null)
                .putColumnIfNotNull("event_name", eventData.getEventName(), null)
                .putColumnIfNotNull("api_key", eventData.getApiKey(), null)
                .putColumnIfNotNull("time_spent_in_millis", eventData.getTimeInMillSec())
                .putColumnIfNotNull("event_source", eventData.getEventSource())
                .putColumnIfNotNull("content_id", eventData.getContentId(), null)
                .putColumnIfNotNull("event_value", eventValue, null)
                .putColumnIfNotNull("gooru_uid", GooruUId,null)
                .putColumnIfNotNull("event_type", eventData.getEventType(),null)
                .putColumnIfNotNull("user_id", eventData.getUserId(),null)
                .putColumnIfNotNull("organization_uid", organizationUid)
                .putColumnIfNotNull("app_oid", appOid, null)
                .putColumnIfNotNull("app_uid", appUid, null)
		        .putColumnIfNotNull("city", eventData.getCity(), null)
		        .putColumnIfNotNull("state", eventData.getState(), null)
		        .putColumnIfNotNull("attempt_number_of_try_sequence", eventData.getAttemptNumberOfTrySequence(), null)
		        .putColumnIfNotNull("attempt_first_status", eventData.getAttemptFirstStatus(), null)
		        .putColumnIfNotNull("answer_first_id", eventData.getAnswerFirstId(), null)
		        .putColumnIfNotNull("attempt_try_sequence", trySeq, null)
		        .putColumnIfNotNull("attempt_status", attemptStatus, null)
		        .putColumnIfNotNull("answer_ids", answereIds, null)
		        .putColumnIfNotNull("country",eventData.getCountry(), null)
		        .putColumnIfNotNull("contextInfo",eventData.getContextInfo(), null)
		        .putColumnIfNotNull("collaboratorIds",eventData.getCollaboratorIds(), null)
		        .putColumnIfNotNull("mobileData",eventData.isMobileData(), null)
		        .putColumnIfNotNull("hintId",eventData.getHintId(), null)
		        .putColumnIfNotNull("open_ended_text",eventData.getOpenEndedText(), null)
		        .putColumnIfNotNull("parent_event_id",eventData.getParentEventId(), null);
        
        try {
            m.execute();
        } catch (ConnectionException e) {
            logger.info("Error while inserting to cassandra - JSON - ", e);
            return null;
        }
        return key;
    }


    @Async
    public String  saveEventObject(String cfName ,String key,EventObject eventObject){
    
    	if(eventObject.getEventId() == null){
    		UUID eventKeyUUID = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
    		key = eventKeyUUID.toString();
    	}else{
    		key	= eventObject.getEventId(); 
    	}
    	
    	MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(WRITE_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));
    	
        m.withRow(this.accessColumnFamily(cfName), key)
                .putColumnIfNotNull("start_time", eventObject.getStartTime(), null)
                .putColumnIfNotNull("end_time", eventObject.getEndTime(),null)
                .putColumnIfNotNull("fields", eventObject.getFields(),null)
                .putColumnIfNotNull("time_spent_in_millis",eventObject.getTimeInMillSec(),null)
                .putColumnIfNotNull("content_gooru_oid",eventObject.getContentGooruId(),null)
                .putColumnIfNotNull("parent_gooru_oid",eventObject.getParentGooruId(),null)
                .putColumnIfNotNull("event_name", eventObject.getEventName(),null)
                .putColumnIfNotNull("session",eventObject.getSession(),null)
                .putColumnIfNotNull("metrics",eventObject.getMetrics(),null)
                .putColumnIfNotNull("pay_load_object",eventObject.getPayLoadObject(),null)
                .putColumnIfNotNull("user",eventObject.getUser(),null)
                .putColumnIfNotNull("context",eventObject.getContext(),null)
        		.putColumnIfNotNull("event_type",eventObject.getEventType(),null)
        		.putColumnIfNotNull("organization_uid",eventObject.getOrganizationUid(),null)
        		.putColumnIfNotNull("parent_event_id",eventObject.getParentEventId(), null);
        try {
            m.execute();
        } catch (ConnectionException e) {
            logger.info("Error while inserting Event Object to cassandra - JSON - ", e);
            return null;
        }
		return key;
                
                
    }
    
    @Async
    public void updateTimelineObject(String cfName,String rowKey,String CoulmnValue,EventObject eventObject) {

        UUID eventColumnTimeUUID = TimeUUIDUtils.getUniqueTimeUUIDinMillis();

        MutationBatch eventTimelineMutation = getKeyspace().prepareMutationBatch().setConsistencyLevel(WRITE_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));

        eventTimelineMutation.withRow(this.accessColumnFamily(cfName), rowKey).putColumn(
                eventColumnTimeUUID.toString(), CoulmnValue, null);

        eventTimelineMutation.withRow(this.accessColumnFamily(cfName), (rowKey+"~"+eventObject.getEventName())).putColumn(
                eventColumnTimeUUID.toString(), CoulmnValue, null);
        
        eventTimelineMutation.withRow(this.accessColumnFamily(cfName), (rowKey.substring(0, 8)+"~"+eventObject.getEventName())).putColumn(
                eventColumnTimeUUID.toString(), CoulmnValue, null);
        
        try {
            eventTimelineMutation.execute();
        } catch (ConnectionException e) {
            logger.info("Error while inserting event data to cassandra", e);
        }
    }

    @Async
    public void updateTimeline(String cfName,EventData eventData, String rowKey) {

        UUID eventColumnTimeUUID = TimeUUIDUtils.getUniqueTimeUUIDinMillis();

        MutationBatch eventTimelineMutation = getKeyspace().prepareMutationBatch().setConsistencyLevel(WRITE_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));

        eventTimelineMutation.withRow(this.accessColumnFamily(cfName), rowKey).putColumn(
                eventColumnTimeUUID.toString(), eventData.getEventKeyUUID(), null);

        eventTimelineMutation.withRow(this.accessColumnFamily(cfName), (rowKey+"~"+eventData.getEventName())).putColumn(
                eventColumnTimeUUID.toString(), eventData.getEventKeyUUID(), null);

        eventTimelineMutation.withRow(this.accessColumnFamily(cfName), (rowKey.substring(0, 8)+"~"+eventData.getEventName())).putColumn(
                eventColumnTimeUUID.toString(), eventData.getEventKeyUUID(), null);
        
        try {
            eventTimelineMutation.execute();
        } catch (ConnectionException e) {
            logger.info("Error while inserting event data to cassandra", e);
        }
    }
    
    public void saveActivity(String cfName,HashMap<String, Object> activities){
		 String rowKey = null;
		 String dateId = "0";
		 String columnName = null;
		 String eventName = null;
		 rowKey = activities.get("userUid") != null ?  activities.get("userUid").toString() : null;
		 dateId = activities.get("dateId") != null ?  activities.get("dateId").toString() : null; 
		 eventName = activities.get("eventName") != null ?  activities.get("eventName").toString() : null;
		 if(activities.get("existingColumnName") == null){
			 columnName = ((dateId.toString() == null ? "0L" : dateId.toString()) + "~" + (eventName == null ? "NA" : activities.get("eventName").toString()) + "~" + activities.get("eventId").toString());
		 } else{
			 columnName = activities.get("existingColumnName").toString();
		 }
	 
	     try {        	
			 MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(WRITE_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));	
			 m.withRow(this.accessColumnFamily(cfName), rowKey)
			 .putColumnIfNotNull(columnName, activities.get("activity") != null ? activities.get("activity").toString():null, null)
			 
			 ;
	            m.execute();
       } catch (ConnectionException e) {
           logger.info("Error while inserting to cassandra ", e);       
       }
	}
    
	public Map<String, Object> isEventIdExists(String cfName,String userUid, String eventId){
		OperationResult<ColumnList<String>> eventColumns = null;
		Map<String,Object> resultMap = new HashMap<String, Object>();
		List<Map<String,Object>> resultList = new ArrayList<Map<String,Object>>();
		Boolean isExists = false;
		try {
			eventColumns = getKeyspace().prepareQuery(this.accessColumnFamily(cfName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5))
			.getKey(userUid).execute();
			if (eventColumns != null)
				for(Column<String> eventColumn : eventColumns.getResult()){
					String columnName = eventColumn.getName();
					if (columnName != null) {						
					if(columnName.contains(eventId)){
						isExists = true;
						resultMap.put("isExists", isExists);
						resultMap.put("jsonString",eventColumn.getStringValue());
						resultMap.put("existingColumnName", columnName);
						resultList.add(resultMap);
						return resultMap;
					}
				}
			}
		} catch (ConnectionException e) {
			logger.info("Cassandra Connection Exception thrown!!");
			e.printStackTrace();
		}
		if(!isExists){
			resultMap.put("isExists", isExists);
			resultMap.put("jsonString",null);
			resultList.add(resultMap);
		}
		return resultMap;
	}
	
    protected static String convertStreamToString(InputStream is) {
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        StringBuilder sb = new StringBuilder();

        String line = null;
        try {
            while ((line = reader.readLine()) != null) {
                sb.append(line + "\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                is.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return sb.toString();
    }

    public List<String> getParentId(String cfName,String Key,int retryCount){

    	Rows<String, String> collectionItem = null;
    	List<String> classPages = new ArrayList<String>();
    	String parentId = null;
    	try {
    		collectionItem = getKeyspace().prepareQuery(this.accessColumnFamily(cfName))
    			.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5))
    		 	.searchWithIndex()
    			.addExpression()
    			.whereColumn("resource_gooru_oid")
    			.equals()
    			.value(Key).execute().getResult();
    	} catch (ConnectionException e) {
    		if(retryCount < 6){
    			retryCount++;
        		return getParentId(cfName,Key,retryCount++);
        	}else{
        		e.printStackTrace();
        	}	
    	}
    	if(collectionItem != null){
    		for(Row<String, String> collectionItems : collectionItem){
    			parentId =  collectionItems.getColumns().getColumnByName("collection_gooru_oid").getStringValue();
    			if(parentId != null){
    				classPages.add(parentId);
    			}
    		 }
    	}
    	return classPages; 
    	}
    
	public void updateCollectionItem(String cfName,Map<String ,String> eventMap){
		
		MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(WRITE_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));
	    
		 m.withRow(this.accessColumnFamily(cfName), eventMap.get(COLLECTIONITEMID))
	     .putColumnIfNotNull(CONTENT_ID,eventMap.get(CONTENTID))
	     .putColumnIfNotNull(PARENT_CONTENT_ID,eventMap.get(PARENTCONTENTID))
	     .putColumnIfNotNull(RESOURCE_GOORU_OID,eventMap.get(CONTENT_GOORU_OID))
	     .putColumnIfNotNull(COLLECTION_GOORU_OID,eventMap.get(PARENT_GOORU_OID))
	     .putColumnIfNotNull(ITEM_SEQUENCE,eventMap.get(ITEMSEQUENCE))
	     .putColumnIfNotNull(ORGANIZATION_UID,eventMap.get(ORGANIZATIONUID))
	    ;
	    
	}
	
	public void updateClasspage(String cfName,Map<String ,String> eventMap){

		MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(WRITE_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));
        
		int isGroupOwner = 0;
		int deleted = 0;
         m.withRow(this.accessColumnFamily(cfName), eventMap.get(CONTENTGOORUOID)+SEPERATOR+eventMap.get(GROUPUID)+SEPERATOR+eventMap.get(GOORUID))
        .putColumnIfNotNull(USER_GROUP_UID,eventMap.get(GROUPUID))
        .putColumnIfNotNull(CLASSPAGE_GOORU_OID,eventMap.get(CONTENTGOORUOID))
        .putColumnIfNotNull("is_group_owner",isGroupOwner)
        .putColumnIfNotNull("deleted",deleted)
        .putColumnIfNotNull(USERID,eventMap.get(GOORUID))
        .putColumnIfNotNull(CLASSPAGE_CODE,eventMap.get(CLASSCODE))
        .putColumnIfNotNull(USER_GROUP_CODE,eventMap.get(CONTENT_GOORU_OID))
        .putColumnIfNotNull(ORGANIZATION_UID,eventMap.get(ORGANIZATIONUID))
        
        ;
        
         try{
          	m.execute();
          } catch (ConnectionException e) {
          	logger.info("Error while inserting to cassandra - JSON - ", e);
          }
	}

	public boolean getClassPageOwnerInfo(String cfName,String key ,String classPageGooruOid,int retryCount){

		Rows<String, String>  result = null;
    	try {
			result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5)).searchWithIndex()
					.addExpression().whereColumn("gooru_uid").equals().value(key)
					.addExpression().whereColumn("classpage_gooru_oid").equals().value(classPageGooruOid)
					.addExpression().whereColumn("is_group_owner").equals().value(1)
					.execute().getResult();
	} catch (ConnectionException e) {
		if(retryCount < 6){
			retryCount++;
    		return getClassPageOwnerInfo(cfName,key ,classPageGooruOid,retryCount++);
    	}else{
    		e.printStackTrace();
    		logger.info("Error in while read : " + e);
    	}
		}
    	if (result != null && !result.isEmpty()) {
    	return true;	
    	}
    	return false;
	}
	
	public boolean isUserPartOfClass(String cfName,String key ,String classPageGooruOid ,int retryCount){

		Rows<String, String>  result = null;
    	try {
    		 result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5))
    		 	.searchWithIndex()
				.addExpression()
				.whereColumn("classpage_gooru_oid")
				.equals().value(classPageGooruOid)
				.addExpression()
				.whereColumn("gooru_uid")
				.equals().value(key)
				.execute().getResult();
		} catch (ConnectionException e) {
			if(retryCount < 6){
				retryCount++;
        		return isUserPartOfClass(cfName,key ,classPageGooruOid ,retryCount++);
        	}else{
        		e.printStackTrace();
        	}
		}
		
    	if (result != null && !result.isEmpty()) {
    		return true;
    	}
    	return false;
	
	}
}
