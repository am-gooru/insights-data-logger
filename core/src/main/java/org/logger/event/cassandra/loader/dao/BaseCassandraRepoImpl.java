package org.logger.event.cassandra.loader.dao;

import java.util.Collection;
import java.util.Map;

import org.logger.event.cassandra.loader.CassandraConnectionProvider;
import org.logger.event.cassandra.loader.service.BaseService;
import org.logger.event.cassandra.loader.service.BaseServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.ExceptionCallback;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.astyanax.connectionpool.exceptions.OperationException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.IndexQuery;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.util.RangeBuilder;

public class BaseCassandraRepoImpl extends BaseDAOCassandraImpl {
	
	private static CassandraConnectionProvider connectionProvider;
	
	private static final Logger logger = LoggerFactory.getLogger(BaseCassandraRepoImpl.class);
	
	
	public BaseCassandraRepoImpl(CassandraConnectionProvider connectionProvider) {
		super(connectionProvider);
		// TODO Auto-generated constructor stub
	}

	public BaseCassandraRepoImpl() {
		super(connectionProvider);
	}

    public Column<String> readWithKeyColumn(String cfName,String key,String columnName){
        
    	Column<String> result = null;
    	try {
              result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName))
                    .setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
                    .getKey(key)
                    .getColumn(columnName)
                    .execute()
                    .getResult()
                    ;

        } catch (Exception e) {
            logger.info("Error while fetching data from method : readWithKeyColumn {} ", e);
        }
    	
    	return result;
    }

    public ColumnList<String> readWithKeyColumnList(String cfName,String key,Collection<String> columnList){
        
    	ColumnList<String> result = null;
    	try {
              result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName))
                    .setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
                    .getKey(key)
                    .withColumnSlice(columnList)
                    .execute()
                    .getResult()
                    ;

        } catch (Exception e) {
            logger.info("Error while fetching data from method : readWithKeyColumnList {} ", e);
        }
    	
    	return result;
    }
    
    public Rows<String, String> readWithKeyListColumnList(String cfName,Collection<String> keys,Collection<String> columnList){
        
    	Rows<String, String> result = null;
    	try {
              result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName))
                    .setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
                    .getKeySlice(keys)
                    .withColumnSlice(columnList)
                    .execute()
                    .getResult()
                    ;

        } catch (Exception e) {
            logger.info("Error while fetching data from method : readWithKeyListColumnList {} ", e);
        }
    	
    	return result;
    }
    public ColumnList<String> readWithKey(String cfName,String key){
        
    	ColumnList<String> result = null;
    	try {
              result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName))
                    .setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
                    .getKey(key)
                    .execute()
                    .getResult()
                    ;

        } catch (Exception e) {
            logger.info("Error while fetching data from method : readWithKey {} ", e);
        }
    	
    	return result;
    }
    
    public Rows<String, String> readWithKeyList(String cfName,Collection<String> key){
        
    	Rows<String, String> result = null;
    	try {
              result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName))
                    .setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
                    .getKeySlice(key)
                    .execute()
                    .getResult()
                    ;

        } catch (Exception e) {
            logger.info("Error while fetching data from method : readWithKey {}", e);
        }
    	
    	return result;
    }

    public Rows<String, String> readCommaKeyList(String cfName,String... key){

    	Rows<String, String> result = null;
    	try {
              result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName))
                    .setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
                    .getKeySlice(key)
                    .execute()
                    .getResult()
                    ;

        } catch (Exception e) {
            logger.info("Error while fetching data from method : readWithKey {}", e);
        }
    	
    	return result;
    }
    
    public Rows<String, String> readIterableKeyList(String cfName,Iterable<String> keys){

    	Rows<String, String> result = null;
    	try {
              result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName))
                    .setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
                    .getKeySlice(keys)
                    .execute()
                    .getResult()
                    ;

        } catch (Exception e) {
            logger.info("Error while fetching data from method : readWithKey {} ", e);
        }
    	
    	return result;
    }
    
    public Rows<String, String> readIndexedColumn(String cfName,String columnName,String value){
    	
    	Rows<String, String> result = null;
    	try{
	    	getKeyspace().prepareQuery(this.accessColumnFamily(cfName))
			.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
		 	.searchWithIndex()
			.addExpression()
			.whereColumn(columnName)
			.equals()
			.value(value)
			.execute()
			.getResult()
			;
	    	
    	} catch(Exception e){
    		logger.info("Error while fetching data from method : readIndexedColumn {} ", e);    		
    	}
    	return result;
    }
    
    public Rows<String, String> readIndexedColumn(String cfName,String columnName,long value){
    	
    	Rows<String, String> result = null;
    	try{
	    	getKeyspace().prepareQuery(this.accessColumnFamily(cfName))
			.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
		 	.searchWithIndex()
			.addExpression()
			.whereColumn(columnName)
			.equals()
			.value(value)
			.execute()
			.getResult()
			;
	    	
    	} catch(Exception e){
    		logger.info("Error while fetching data from method : readIndexedColumn {} ", e);    		
    	}
    	return result;
    }

    public Rows<String, String> readIndexedColumnLastNrows(String cfName ,String columnName,String value, Integer rowsToRead) {
    	
		Rows<String, String> result = null;
    	try {
    		result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName))
			 			   .setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
					 	   .searchWithIndex().autoPaginateRows(true)
					 	   .setRowLimit(rowsToRead.intValue())
					 	   .addExpression().whereColumn(columnName)
					 	   .equals().value(value).execute().getResult();
		} catch (ConnectionException e) {
			logger.info("Error while fetching data from method : readIndexedColumnLastNrows {} ", e);
		}
    	
    	return result;
	}

    public ColumnList<String> readKeyLastNColumns(String cfName,String key, Integer columnsToRead) {
    	
    	ColumnList<String> result = null;
    	try {
    		result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName))
    		.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
    		.getKey(key)
    		.withColumnRange(new RangeBuilder().setReversed().setLimit(columnsToRead.intValue()).build())
    		.execute().getResult();
    	} catch (ConnectionException e) {
    		logger.info("Error while fetching data from method : readKeyLastNColumns {} ", e);
    	}
    	
    	return result;
    }
    
    public Rows<String, String> readIndexedColumnList(String cfName,Map<String,String> columnList){
    	
    	Rows<String, String> result = null;
    	IndexQuery<String, String> query = null;

    	query = getKeyspace().prepareQuery(this.accessColumnFamily(cfName))
    				.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
    				.searchWithIndex();
    	
    	for (Map.Entry<String, String> entry : columnList.entrySet()) {
    		query.addExpression().whereColumn(entry.getKey()).equals().value(entry.getValue())
    	    ;
    	}
    	
    	try{
    		result = query.execute().getResult()
			;
    	} catch(Exception e){
    		logger.info("Error while fetching data from method : readIndexedColumnList {} ", e);    		
    	}

    	return result;
    }
    
    public boolean isRowKeyExists(String cfName,String key) {

		try {
			return getKeyspace().prepareQuery(this.accessColumnFamily(cfName))
				.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
				.getKey(key).execute()
				.getResult()
				.isEmpty();
		} catch (Exception e) {
			logger.info("Error while fetching data from method : isRowKeyExists {} ", e);
		}
		return false;
	}
    
	public ColumnList<String> readColumnsWithPrefix(String cfName,String rowKey, String startColumnNamePrefix, String endColumnNamePrefix, Integer rowsToRead){
		ColumnList<String> result = null;
		String startColumnPrefix = null;
		String endColumnPrefix = null;
		startColumnPrefix = startColumnNamePrefix+"~\u0000";
		endColumnPrefix = endColumnNamePrefix+"~\uFFFF";
	
		try {
			result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName))
			.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
			.getKey(rowKey) .withColumnRange(new RangeBuilder().setLimit(rowsToRead)
			.setStart(startColumnPrefix)
			.setEnd(endColumnPrefix)
			.build()).execute().getResult();
		} catch (Exception e) {
			logger.info("Error while fetching data from method : readColumnsWithPrefix {} ", e);
		} 
		
		return result;
	}
	
    public Rows<String, String> readAllRows(String cfName){
    	
    	Rows<String, String> result = null;
		try {
			result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName))
					.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
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
		} catch (Exception e) {
			logger.info("Error while fetching data from method : readAllRows {} ", e);
		}
		
		return result;
	}
    
    
    public void saveBulkStringList(String cfName, String key,Map<String,String> columnValueList) {

        MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);

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

        MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);

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
    
    public void saveStringValue(String cfName, String key,String columnName,String value) {

        MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);

        m.withRow(this.accessColumnFamily(cfName), key).putColumnIfNotNull(columnName, value, null);

        try {
            m.execute();
        } catch (ConnectionException e) {
            logger.info("Error while save in method : saveStringValue {}", e);
        }
    }

    public void saveLongValue(String cfName, String key,String columnName,String value) {

        MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);

        m.withRow(this.accessColumnFamily(cfName), key).putColumnIfNotNull(columnName, value, null);

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
    
    public void generateNonCounter(String cfName,String key,String columnName, String value ,MutationBatch m) {
        m.withRow(this.accessColumnFamily(cfName), key)
        .putColumnIfNotNull(columnName, value);
    }
    
    public void generateNonCounter(String cfName,String key,String columnName, long value ,MutationBatch m) {
        m.withRow(this.accessColumnFamily(cfName), key)
        .putColumnIfNotNull(columnName, value);
    }
    

    public void  deleteAll(String cfName){
		try {
			getKeyspace().truncateColumnFamily(this.accessColumnFamily(cfName));
		} catch (Exception e) {
			 logger.info("Error while deleting rows in method :deleteAll {} ",e);
		} 
	
}
    public ColumnFamily<String, String> accessColumnFamily(String columnFamilyName) {

		ColumnFamily<String, String> aggregateColumnFamily;

		aggregateColumnFamily = new ColumnFamily<String, String>(columnFamilyName, StringSerializer.get(), StringSerializer.get());

		return aggregateColumnFamily;
	}
    

}
