package org.logger.event.cassandra.loader.dao;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import org.apache.avro.reflect.Nullable;
import org.ednovo.data.model.ClassActivityDatacube;
import org.ednovo.data.model.ContentTaxonomyActivity;
import org.ednovo.data.model.EventBuilder;
import org.ednovo.data.model.EventData;
import org.ednovo.data.model.ResourceCo;
import org.ednovo.data.model.StudentLocation;
import org.ednovo.data.model.StudentsClassActivity;
import org.ednovo.data.model.TaxonomyActivityDataCube;
import org.ednovo.data.model.UserCo;
import org.ednovo.data.model.UserSessionActivity;
import org.json.JSONObject;
import org.logger.event.cassandra.loader.Constants;
import org.logger.event.cassandra.loader.LoaderConstants;
import org.logger.event.cassandra.loader.PreparedStatmentQueries;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.google.common.base.Function;
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
import com.netflix.astyanax.recipes.reader.AllRowsReader;
import com.netflix.astyanax.retry.ConstantBackoff;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.util.RangeBuilder;
import com.netflix.astyanax.util.TimeUUIDUtils;

public class BaseCassandraRepoImpl extends BaseDAOCassandraImpl implements BaseCassandraRepo {

	private static final Logger LOG = LoggerFactory.getLogger(BaseCassandraRepoImpl.class);

	private static final SimpleDateFormat FORMATTER = new SimpleDateFormat("yyyy-MM-dd kk:mm:ss");

	
	/**
	 * This method using to read data with single Key&Indexed column.
	 * 
	 * @param cfName
	 * @param key
	 * @param columnName
	 * @param retryCount
	 * @return
	 */
	@Override
	public Column<String> readWithKeyColumn(String cfName, String key, String columnName) {

		Column<String> result = null;
		ColumnList<String> columnList = null;
		try {
			columnList = getKeyspace().prepareQuery(this.accessColumnFamily(cfName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5)).getKey(key).execute()
					.getResult();

		} catch (Exception e) {
			LOG.error("Exception:",e);
		}
		if (columnList != null) {
			result = columnList.getColumnByName(columnName);
		}
		return result;
	}
	
	@Override
	public boolean checkColumnExist(String cfName, String key, String columnName) {
		boolean result = false;
		ColumnList<String> columnList = null;
		try {
			columnList = getKeyspace().prepareQuery(this.accessColumnFamily(cfName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5)).getKey(key).execute()
					.getResult();

		} catch (Exception e) {
			LOG.error("Exception:",e);
		}
		if (columnList != null && columnList.getColumnNames().contains(columnName)) {
			result = true;
		}
		return result;
	}

	/**
	 * This method using to read data with single Key and multiple Indexed columns.
	 * 
	 * @param cfName
	 * @param key
	 * @param columnList
	 * @param retryCount
	 * @return
	 */
	@Override
	public ColumnList<String> readWithKeyColumnList(String cfName, String key, Collection<String> columnList) {

		ColumnList<String> result = null;
		try {
			result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5)).getKey(key)
					.withColumnSlice(columnList).execute().getResult();

		} catch (Exception e) {
			LOG.error("Exception:",e);
		}

		return result;
	}

	/**
	 * This method using to read data with multiple Keys and multiple Indexed columns.
	 * 
	 * @param cfName
	 * @param keys
	 * @param columnList
	 * @param retryCount
	 * @return
	 */
	@Override
	public Rows<String, String> readWithKeyListColumnList(String cfName, Collection<String> keys, Collection<String> columnList) {

		Rows<String, String> result = null;
		try {
			result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5)).getKeySlice(keys)
					.withColumnSlice(columnList).execute().getResult();

		} catch (Exception e) {
			LOG.error("Exception:",e);
		}

		return result;
	}

	/**
	 * This method using to read data with single Key.
	 * 
	 * @param cfName
	 * @param key
	 * @param retryCount
	 * @return
	 */
	@Override
	public ColumnList<String> readWithKey(String cfName, String key) {

		ColumnList<String> result = null;
		try {
			result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5)).getKey(key).execute()
					.getResult();

		} catch (Exception e) {
			LOG.error("Exception:",e);
		}

		return result;
	}

	/**
	 * This method using to read data with multiple Keys.
	 * 
	 * @param cfName
	 * @param key
	 * @param retryCount
	 * @return
	 */
	@Override
	public Rows<String, String> readWithKeyList(String cfName, Collection<String> key) {

		Rows<String, String> result = null;
		try {
			result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5)).getKeySlice(key)
					.execute().getResult();

		} catch (Exception e) {
			LOG.error("Exception:",e);
		}

		return result;
	}

	/**
	 * This method is using to get the data with multiple Keys in comma separated.
	 * 
	 * @param cfName
	 * @param retryCount
	 * @param key
	 * @return
	 */
	@Override
	public Rows<String, String> readCommaKeyList(String cfName, String... key) {

		Rows<String, String> result = null;
		try {
			result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5)).getKeySlice(key)
					.execute().getResult();

		} catch (Exception e) {
			LOG.error("Exception:",e);
		}

		return result;
	}

	/**
	 * 
	 * @param cfName
	 * @param keys
	 * @return
	 */
	@Override
	public Rows<String, String> readIterableKeyList(String cfName, Iterable<String> keys) {

		Rows<String, String> result = null;
		try {
			result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5)).getKeySlice(keys)
					.execute().getResult();

		} catch (Exception e) {
			LOG.error("Exception:",e);
		}

		return result;
	}

	/**
	 * This method is using to get data with single String data type Indexed column.
	 * 
	 * @param cfName
	 * @param columnName
	 * @param value
	 * @param retryCount
	 * @return
	 */
	@Override
	public Rows<String, String> readIndexedColumn(String cfName, String columnName, String value) {

		Rows<String, String> result = null;
		try {
			result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5)).searchWithIndex()
					.addExpression().whereColumn(columnName).equals().value(value).execute().getResult();

		} catch (Exception e) {
			LOG.error("Exception:",e);
		}
		return result;
	}

	/**
	 * This method is using to get data with single Long data type Indexed column.
	 * 
	 * @param cfName
	 * @param columnName
	 * @param value
	 * @param retryCount
	 * @return
	 */
	@Override
	public Rows<String, String> readIndexedColumn(String cfName, String columnName, long value) {

		Rows<String, String> result = null;
		try {
			result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5)).searchWithIndex()
					.addExpression().whereColumn(columnName).equals().value(value).execute().getResult();

		} catch (Exception e) {
			LOG.error("Exception:",e);
		}
		return result;
	}

	/**
	 * This method is using to read rows from bottom.
	 * 
	 * @param cfName
	 * @param columnName
	 * @param value
	 * @param rowsToRead
	 * @param retryCount
	 * @return
	 */
	@Override
	public Rows<String, String> readIndexedColumnLastNrows(String cfName, String columnName, String value, Integer rowsToRead) {

		Rows<String, String> result = null;
		try {
			result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5)).searchWithIndex()
					.autoPaginateRows(true).setRowLimit(rowsToRead.intValue()).addExpression().whereColumn(columnName).equals().value(value).execute().getResult();
		} catch (Exception e) {
			LOG.error("Exception:",e);
		}

		return result;
	}

	/**
	 * This method is using to read columns which have recently introduced.To use this method, Cassandra has to store columns order by enable NTP config.
	 * 
	 * @param cfName
	 * @param key
	 * @param columnsToRead
	 * @param retryCount
	 * @return
	 */@Override
	public ColumnList<String> readKeyLastNColumns(String cfName, String key, Integer columnsToRead) {

		ColumnList<String> result = null;
		try {
			result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5)).getKey(key)
					.withColumnRange(new RangeBuilder().setReversed().setLimit(columnsToRead.intValue()).build()).execute().getResult();
		} catch (Exception e) {
			LOG.error("Exception:",e);
		}

		return result;
	}

	/**
	 * This method is using to get row count.
	 * 
	 * @param cfName
	 * @param key
	 * @return
	 */
	 @Override
	public long getCount(String cfName, String key) {

		Integer columns = null;

		try {
			columns = getKeyspace().prepareQuery(this.accessColumnFamily(cfName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5)).getKey(key).getCount()
					.execute().getResult();
		} catch (Exception e) {
			LOG.info("Error while getting column count for - {}",key);
			LOG.error("Exception:",e);
		}

		return columns.longValue();
	}

	/**
	 * This method is using to get data with more number of indexed columns.
	 * 
	 * @param cfName
	 * @param columnList
	 * @param retryCount
	 * @return
	 */
	@Override 
	public Rows<String, String> readIndexedColumnList(String cfName, Map<String, String> columnList) {

		Rows<String, String> result = null;
		IndexQuery<String, String> query = null;

		query = getKeyspace().prepareQuery(this.accessColumnFamily(cfName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5)).searchWithIndex();

		for (Map.Entry<String, String> entry : columnList.entrySet()) {
			query.addExpression().whereColumn(entry.getKey()).equals().value(entry.getValue());
		}

		try {
			result = query.execute().getResult();
		} catch (Exception e) {
			LOG.error("Exception:",e);
		}

		return result;
	}

	/**
	 * Just to check if row is available or not for given key.
	 * 
	 * @param cfName
	 * @param key
	 * @param retryCount
	 * @return
	 */
	@Override
	public boolean isRowKeyExists(String cfName, String key) {

		try {
			return getKeyspace().prepareQuery(this.accessColumnFamily(cfName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5)).getKey(key).execute()
					.getResult().isEmpty();
		} catch (Exception e) {
			LOG.error("Exception:",e);
		}
		return false;
	}

	/**
	 * Just to check if rows available while use indexed columns.
	 * 
	 * @param cfName
	 * @param columns
	 * @return
	 */
	@Override
	public boolean isValueExists(String cfName, Map<String, Object> columns) {

		try {
			IndexQuery<String, String> cfQuery = getKeyspace().prepareQuery(this.accessColumnFamily(cfName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
					.withRetryPolicy(new ConstantBackoff(2000, 5)).searchWithIndex();

			for (Map.Entry<String, Object> map : columns.entrySet()) {
				cfQuery.addExpression().whereColumn(map.getKey()).equals().value(map.getValue().toString());
			}
			return cfQuery.execute().getResult().isEmpty();
		} catch (Exception e) {
			LOG.error("Exception:",e);
		}
		return false;
	}

	/**
	 * Get all the Keys for indexed columns.
	 * 
	 * @param cfName
	 * @param columns
	 * @return
	 */
	@Override
	public Collection<String> getKey(String cfName, Map<String, Object> columns) {

		try {

			IndexQuery<String, String> cfQuery = getKeyspace().prepareQuery(this.accessColumnFamily(cfName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
					.withRetryPolicy(new ConstantBackoff(2000, 5)).searchWithIndex();

			for (Map.Entry<String, Object> map : columns.entrySet()) {
				cfQuery.addExpression().whereColumn(map.getKey()).equals().value(map.getValue().toString());
			}
			Rows<String, String> rows = cfQuery.execute().getResult();
			return rows.getKeys();
		} catch (Exception e) {
			LOG.error("Exception:",e);
		}
		return null;
	}

	/**
	 * Get data based on column range.
	 * 
	 * @param cfName
	 * @param rowKey
	 * @param startColumnNamePrefix
	 * @param endColumnNamePrefix
	 * @param rowsToRead
	 * @return
	 */
	@Override
	public ColumnList<String> readColumnsWithPrefix(String cfName, String rowKey, String startColumnNamePrefix, String endColumnNamePrefix, Integer rowsToRead) {
		ColumnList<String> result = null;
		String startColumnPrefix = null;
		String endColumnPrefix = null;
		startColumnPrefix = startColumnNamePrefix + "~\u0000";
		endColumnPrefix = endColumnNamePrefix + "~\uFFFF";

		try {
			result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5)).getKey(rowKey)
					.withColumnRange(new RangeBuilder().setLimit(rowsToRead).setStart(startColumnPrefix).setEnd(endColumnPrefix).build()).execute().getResult();
		} catch (Exception e) {
			LOG.error("Exception:",e);
		}

		return result;
	}

	/**
	 * Get all the rows from ColumnFamily.Please use this method for less number of data.
	 * 
	 * @param cfName
	 * @param retryCount
	 * @return
	 */
	@Override
	public Rows<String, String> readAllRows(String cfName) {

		Rows<String, String> result = null;
		try {
			result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5)).getAllRows()
					.withColumnRange(new RangeBuilder().setMaxSize(10).build()).setExceptionCallback(new ExceptionCallback() {
						@Override
						public boolean onException(ConnectionException e) {
							try {
								Thread.sleep(1000);
							} catch (InterruptedException e1) {
							}
							return true;
						}
					}).execute().getResult();
		} catch (Exception e) {
			LOG.error("Exception:",e);
		}

		return result;
	}

	@Override
	public Rows<String, String> readAllRows(final String cfName, final CallBackRows allRows ) {
        try {
                boolean result = new AllRowsReader.Builder<String, String>(getKeyspace(), this.accessColumnFamily(cfName)).withPageSize(100) // Read 100 rows at a time
                                .withConcurrencyLevel(10) // Split entire token range into 10. Default is by number of nodes.
                                .withPartitioner(null) // this will use keyspace's partitioner
                                .forEachPage(new Function<Rows<String, String>, Boolean>() {
                                        @Override
                                        public Boolean apply(@Nullable Rows<String, String> rows) {
                                                allRows.getRows(rows);
                                                return true;
                                        }
                                }).build().call();

        } catch (Exception e) {
                LOG.error("Exception",e);
        }
        return null;
}

	/**
	 * Saving multiple String columns & values for single Key.
	 * 
	 * @param cfName
	 * @param key
	 * @param columnValueList
	 */
	@Override
	public void saveBulkStringList(String cfName, String key, Map<String, String> columnValueList) {

		MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));

		for (Map.Entry<String, String> entry : columnValueList.entrySet()) {
			m.withRow(this.accessColumnFamily(cfName), key).putColumnIfNotNull(entry.getKey(), entry.getValue(), null);
			;
		}

		try {
			m.execute();
		} catch (Exception e) {
			LOG.error("Exception:",e);
		}
	}

	/**
	 * Saving multiple Long columns & values for single Key.
	 * 
	 * @param cfName
	 * @param key
	 * @param columnValueList
	 */
	@Override
	public void saveBulkLongList(String cfName, String key, Map<String, Long> columnValueList) {

		MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));

		for (Map.Entry<String, Long> entry : columnValueList.entrySet()) {
			m.withRow(this.accessColumnFamily(cfName), key).putColumnIfNotNull(entry.getKey(), entry.getValue(), null);
			;
		}

		try {
			m.execute();
		} catch (Exception e) {
			LOG.error("Exception:",e);
		}
	}

	/**
	 * Saving multiple (String,Integer,Long,Boolean) columns & values for single Key.
	 * 
	 * @param cfName
	 * @param key
	 * @param columnValueList
	 */
	@Override
	public void saveBulkList(String cfName, String key, Map<String, Object> columnValueList) {

		MutationBatch mutation = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));

		ColumnListMutation<String> m = mutation.withRow(this.accessColumnFamily(cfName), key);

		for (Entry<String, Object> entry : columnValueList.entrySet()) {
			if (entry.getValue().getClass().getSimpleName().equalsIgnoreCase("String")) {
				m.putColumnIfNotNull(entry.getKey(), String.valueOf(entry.getValue()), null);
			}
			if (entry.getValue().getClass().getSimpleName().equalsIgnoreCase("Integer")) {
				m.putColumnIfNotNull(entry.getKey(), Integer.valueOf(String.valueOf(entry.getValue())));
			}
			if (entry.getValue().getClass().getSimpleName().equalsIgnoreCase("Long")) {
				m.putColumnIfNotNull(entry.getKey(), Long.valueOf(String.valueOf(entry.getValue())));
			}
			if (entry.getValue().getClass().getSimpleName().equalsIgnoreCase("Boolean")) {
				m.putColumnIfNotNull(entry.getKey(), Boolean.valueOf(String.valueOf(entry.getValue())));
			}
			if (entry.getValue().getClass().getSimpleName().equalsIgnoreCase("Date")) {
				m.putColumnIfNotNull(entry.getKey(), (Date) entry.getValue());
			}
		}
		try {
			mutation.execute();
		} catch (Exception e) {
			LOG.error("Exception:",e);
		}
	}

	/**
	 * Saving String single column,value for a key.
	 * 
	 * @param cfName
	 * @param key
	 * @param columnName
	 * @param value
	 */
	@Override
	public void saveStringValue(String cfName, String key, String columnName, String value) {

		MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));

		m.withRow(this.accessColumnFamily(cfName), key).putColumnIfNotNull(columnName, value, null);

		try {
			m.execute();
		} catch (Exception e) {
			LOG.error("Exception:",e);
		}
	}

	/**
	 * Saving String single column,value for a key with expired time so that data will be removed while given time get expire.
	 * 
	 * @param cfName
	 * @param key
	 * @param columnName
	 * @param value
	 * @param expireTime
	 */
	@Override
	public void saveStringValue(String cfName, String key, String columnName, String value, int expireTime) {

		MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));

		m.withRow(this.accessColumnFamily(cfName), key).putColumnIfNotNull(columnName, value, expireTime);

		try {
			m.execute();
		} catch (Exception e) {
			LOG.error("Exception:",e);
		}
	}

	/**
	 * Saving Long single column,value for a key with expired time so that data will be removed while given time get expire.
	 * 
	 * @param cfName
	 * @param key
	 * @param columnName
	 * @param value
	 * @param expireTime
	 */
	@Override
	public void saveLongValue(String cfName, String key, String columnName, long value, int expireTime) {

		MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));

		m.withRow(this.accessColumnFamily(cfName), key).putColumnIfNotNull(columnName, value, expireTime);

		try {
			m.execute();
		} catch (Exception e) {
			LOG.error("Exception:",e);
		}
	}

	/**
	 * 
	 * @param cfName
	 * @param key
	 * @param columnName
	 * @param value
	 */
	@Override
	public void saveLongValue(String cfName, String key, String columnName, long value) {

		MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));

		m.withRow(this.accessColumnFamily(cfName), key).putColumnIfNotNull(columnName, value, null);

		try {
			m.execute();
		} catch (Exception e) {
			LOG.error("Exception:",e);
		}
	}

	/**
	 * Save any type of value.
	 * 
	 * @param cfName
	 * @param key
	 * @param columnName
	 * @param value
	 */
	@Override
	public void saveValue(String cfName, String key, String columnName, Object value) {

		MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));
		if (value.getClass().getSimpleName().equalsIgnoreCase("String")) {
			m.withRow(this.accessColumnFamily(cfName), key).putColumnIfNotNull(columnName, String.valueOf(value), null);
		}else if (value.getClass().getSimpleName().equalsIgnoreCase("Integer")) {
			m.withRow(this.accessColumnFamily(cfName), key).putColumnIfNotNull(columnName, Integer.valueOf("" + value), null);
		}else if (value.getClass().getSimpleName().equalsIgnoreCase("Long")) {
			m.withRow(this.accessColumnFamily(cfName), key).putColumnIfNotNull(columnName, Long.valueOf("" + value), null);
		}else if (value.getClass().getSimpleName().equalsIgnoreCase("Boolean")) {
			m.withRow(this.accessColumnFamily(cfName), key).putColumnIfNotNull(columnName, Boolean.valueOf("" + value), null);
		} else {
			m.withRow(this.accessColumnFamily(cfName), key).putColumnIfNotNull(columnName, String.valueOf(value), null);
		}

		try {
			m.execute();
		} catch (Exception e) {
			LOG.error("Exception:",e);
		}
	}

	/**
	 * Add values in counter columnfamily.
	 * 
	 * @param cfName
	 * @param key
	 * @param columnName
	 * @param value
	 */
	@Override
	public void increamentCounter(String cfName, String key, String columnName, long value) {

		MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));

		m.withRow(this.accessColumnFamily(cfName), key).incrementCounterColumn(columnName, value);

		try {
			m.execute();
		} catch (Exception e) {
			LOG.error("Exception:",e);
		}
	}

	/**
	 * Generate Counter mutation batch for counter column family.
	 * 
	 * @param cfName
	 * @param key
	 * @param columnName
	 * @param value
	 * @param m
	 */
	@Override
	public void generateCounter(String cfName, String key, String columnName, long value, MutationBatch m) {
		m.withRow(this.accessColumnFamily(cfName), key).incrementCounterColumn(columnName, value);
	}
	
	/**
	 * Generate Mutation batch for any type of data
	 * 
	 * @param columnName
	 * @param value
	 * @param m
	 */
	@Override
	public void generateNonCounter(String columnName, Object value, ColumnListMutation<String> m) {
		if (value != null) {
			if (value.getClass().getSimpleName().equalsIgnoreCase("String")) {
				m.putColumnIfNotNull(columnName, String.valueOf(value), null);
			}
			if (value.getClass().getSimpleName().equalsIgnoreCase("Integer")) {
				m.putColumnIfNotNull(columnName, Integer.valueOf(String.valueOf(value)));
			}
			if (value.getClass().getSimpleName().equalsIgnoreCase("Long")) {
				m.putColumnIfNotNull(columnName, Long.valueOf(String.valueOf(value)));
			}
			if (value.getClass().getSimpleName().equalsIgnoreCase("Boolean")) {
				m.putColumnIfNotNull(columnName, Boolean.valueOf(String.valueOf(value)));
			}
			if (value.getClass().getSimpleName().equalsIgnoreCase("Timestamp")) {
				try {
					m.putColumnIfNotNull(columnName, new Timestamp((FORMATTER.parse(value.toString())).getTime()));
				} catch (Exception e) {
					e.printStackTrace();
					LOG.error("Exception : columnName" + columnName + ": value : " + value);
				}
			}
		} else {
			LOG.error("Column Value is null");
		}
	}

	/**
	 * Generate Mutation batch with String value.
	 * 
	 * @param cfName
	 * @param key
	 * @param columnName
	 * @param value
	 * @param m
	 */
	@Override
	public void generateNonCounter(String cfName, String key, String columnName, String value, MutationBatch m) {
		m.withRow(this.accessColumnFamily(cfName), key).putColumnIfNotNull(columnName, value);
	}

	/**
	 * Generate Mutation batch with Long value.
	 * 
	 * @param cfName
	 * @param key
	 * @param columnName
	 * @param value
	 * @param m
	 */
	@Override
	public void generateNonCounter(String cfName, String key, String columnName, long value, MutationBatch m) {
		m.withRow(this.accessColumnFamily(cfName), key).putColumnIfNotNull(columnName, value);
	}

	/**
	 * Generate Mutation batch with Integer value.
	 * 
	 * @param cfName
	 * @param key
	 * @param columnName
	 * @param value
	 * @param m
	 */
	@Override
	public void generateNonCounter(String cfName, String key, String columnName, Integer value, MutationBatch m) {
		m.withRow(this.accessColumnFamily(cfName), key).putColumnIfNotNull(columnName, value);
	}

	/**
	 * Generate Mutation batch with Boolean value.
	 * 
	 * @param cfName
	 * @param key
	 * @param columnName
	 * @param value
	 * @param m
	 */
	@Override
	public void generateNonCounter(String cfName, String key, String columnName, Boolean value, MutationBatch m) {
		m.withRow(this.accessColumnFamily(cfName), key).putColumnIfNotNull(columnName, value);
	}

	/**
	 * Generate Mutation batch with Long value with expire time.
	 * 
	 * @param cfName
	 * @param key
	 * @param columnName
	 * @param value
	 * @param expireTime
	 * @param m
	 */
	@Override
	public void generateTTLColumns(String cfName, String key, String columnName, long value, int expireTime, MutationBatch m) {
		m.withRow(this.accessColumnFamily(cfName), key).putColumnIfNotNull(columnName, value, expireTime);
	}

	/**
	 * Generate Mutation batch with String value with expire time.
	 * 
	 * @param cfName
	 * @param key
	 * @param columnName
	 * @param value
	 * @param expireTime
	 * @param m
	 */
	@Override
	public void generateTTLColumns(String cfName, String key, String columnName, String value, int expireTime, MutationBatch m) {
		m.withRow(this.accessColumnFamily(cfName), key).putColumn(columnName, value, expireTime);
	}

	/**
	 * Delete all the data from ColumnFamily
	 * 
	 * @param cfName
	 */
	@Override
	public void deleteAll(String cfName) {
		try {
			getKeyspace().truncateColumnFamily(this.accessColumnFamily(cfName));
		} catch (Exception e) {
			LOG.error("Exception:",e);
		}
	}

	/**
	 * Delete single row key
	 * 
	 * @param cfName
	 * @param key
	 */
	@Override
	public void deleteRowKey(String cfName, String key) {
		MutationBatch m = getKeyspace().prepareMutationBatch();
		try {
			m.withRow(this.accessColumnFamily(cfName), key).delete();
			m.execute();
			LOG.info("Deleted row key - {}",key);
		} catch (Exception e) {
			LOG.info("Failed to delete row key : {}",key);
			LOG.error("Exception:",e);
		}
	}

	/**
	 * Delete a column
	 * 
	 * @param cfName
	 * @param key
	 * @param columnName
	 */
	@Override
	public void deleteColumn(String cfName, String key, String columnName) {
		MutationBatch m = getKeyspace().prepareMutationBatch();
		try {
			m.withRow(this.accessColumnFamily(cfName), key).deleteColumn(columnName);
			m.execute();
			LOG.info("Deleted row key : {} and column : {}",key,columnName);
		} catch (Exception e) {
			LOG.info("Failed to delete row key : {} and column : {}",key,columnName);
			LOG.error("Exception:",e);
		}
	}

	/**
	 * Get ColumnFamily class
	 * 
	 * @param columnFamilyName
	 * @return
	 */
	@Override
	public ColumnFamily<String, String> accessColumnFamily(String columnFamilyName) {

		ColumnFamily<String, String> aggregateColumnFamily;

		aggregateColumnFamily = new ColumnFamily<String, String>(columnFamilyName, StringSerializer.get(), StringSerializer.get());

		return aggregateColumnFamily;
	}

	/*
	 * These are custom methods which is supporting in out application
	 */

	/**
	 * Saving data into event_detail columnFamily for old events.
	 * 
	 * @param cfName
	 * @param eventData
	 * @return
	 */
	@Override
	public String saveEvent(String cfName, EventData eventData) {
		String key = null;
		if (eventData.getEventId() == null) {
			UUID eventKeyUUID = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
			key = eventKeyUUID.toString();
		} else {
			key = eventData.getEventId();
		}
		String appOid = "GLP";

		String gooruOid = eventData.getContentGooruId();

		if (gooruOid == null) {
			gooruOid = eventData.getGooruOId();
		}
		if (gooruOid == null) {
			gooruOid = eventData.getGooruId();
		}
		if ((gooruOid == null || gooruOid.isEmpty()) && eventData.getResourceId() != null) {
			gooruOid = eventData.getResourceId();
		}
		String eventValue = eventData.getQuery();
		if (eventValue == null) {
			eventValue = "NA";
		}
		String parentGooruOid = eventData.getParentGooruId();
		if ((parentGooruOid == null || parentGooruOid.isEmpty()) && eventData.getCollectionId() != null) {
			parentGooruOid = eventData.getCollectionId();
		}
		if (parentGooruOid == null || parentGooruOid.isEmpty()) {
			parentGooruOid = "NA";
		}
		if (gooruOid == null || gooruOid.isEmpty()) {
			gooruOid = "NA";
		}
		String organizationUid = eventData.getOrganizationUid();
		if (organizationUid == null) {
			organizationUid = "NA";
		}
		String gooruUId = eventData.getGooruUId();

		String appUid = appOid + "~" + gooruOid;
		Date dNow = new Date();
		SimpleDateFormat ft = new SimpleDateFormat("yyyyMMddkkmm");
		String date = ft.format(dNow).toString();

		String trySeq = null;
		String attemptStatus = null;
		String answereIds = null;

		if (eventData.getAttemptTrySequence() != null) {
			trySeq = eventData.getAttemptTrySequence().toString();
		}
		if (eventData.getAttemptStatus() != null) {
			attemptStatus = eventData.getAttemptStatus().toString();
		}
		if (eventData.getAnswerId() != null) {
			answereIds = eventData.getAnswerId().toString();
		}
		// Inserting data
		MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));

		m.withRow(this.accessColumnFamily(cfName), key).putColumn("date_time", date, null).putColumnIfNotNull("start_time", eventData.getStartTime(), null)
				.putColumnIfNotNull("user_ip", eventData.getUserIp(), null).putColumnIfNotNull("fields", eventData.getFields(), null).putColumnIfNotNull("user_agent", eventData.getUserAgent(), null)
				.putColumnIfNotNull("session_token", eventData.getSessionToken(), null).putColumnIfNotNull("end_time", eventData.getEndTime(), null)
				.putColumnIfNotNull("content_gooru_oid", gooruOid, null).putColumnIfNotNull("parent_gooru_oid", parentGooruOid, null).putColumnIfNotNull("event_name", eventData.getEventName(), null)
				.putColumnIfNotNull("api_key", eventData.getApiKey(), null).putColumnIfNotNull("time_spent_in_millis", eventData.getTimeInMillSec())
				.putColumnIfNotNull("event_source", eventData.getEventSource()).putColumnIfNotNull("content_id", eventData.getContentId(), null).putColumnIfNotNull("event_value", eventValue, null)
				.putColumnIfNotNull("gooru_uid", gooruUId, null).putColumnIfNotNull("event_type", eventData.getEventType(), null).putColumnIfNotNull("user_id", eventData.getUserId(), null)
				.putColumnIfNotNull("organization_uid", organizationUid).putColumnIfNotNull("app_oid", appOid, null).putColumnIfNotNull("app_uid", appUid, null)
				.putColumnIfNotNull("city", eventData.getCity(), null).putColumnIfNotNull("state", eventData.getState(), null)
				.putColumnIfNotNull("attempt_number_of_try_sequence", eventData.getAttemptNumberOfTrySequence(), null)
				.putColumnIfNotNull("attempt_first_status", eventData.getAttemptFirstStatus(), null).putColumnIfNotNull("answer_first_id", eventData.getAnswerFirstId(), null)
				.putColumnIfNotNull("attempt_try_sequence", trySeq, null).putColumnIfNotNull("attempt_status", attemptStatus, null).putColumnIfNotNull("answer_ids", answereIds, null)
				.putColumnIfNotNull("country", eventData.getCountry(), null).putColumnIfNotNull("contextInfo", eventData.getContextInfo(), null)
				.putColumnIfNotNull("collaboratorIds", eventData.getCollaboratorIds(), null).putColumnIfNotNull("mobileData", eventData.isMobileData(), null)
				.putColumnIfNotNull("hintId", eventData.getHintId(), null).putColumnIfNotNull("open_ended_text", eventData.getOpenEndedText(), null)
				.putColumnIfNotNull("parent_event_id", eventData.getParentEventId(), null);

		try {
			m.execute();
		} catch (Exception e) {
			LOG.error("Exception:",e);
		}
		return key;
	}

	/**
	 * Saving events in event_detail column family for 1.0 logapi version events.
	 * 
	 * @param cfName
	 * @param key
	 * @param event
	 * @return
	 */
	@Override
	public String saveEvent(String cfName, String key, EventBuilder event) {

		if (event.getEventId() == null) {
			UUID eventKeyUUID = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
			key = eventKeyUUID.toString();
		} else {
			key = event.getEventId();
		}

		MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));

		m.withRow(this.accessColumnFamily(cfName), key)
				.putColumnIfNotNull(Constants._START_TIME, event.getStartTime(), null)
				.putColumnIfNotNull(Constants._END_TIME, event.getEndTime(), null)
				.putColumnIfNotNull(Constants.FIELDS, event.getFields(), null)
				.putColumnIfNotNull(Constants._CONTENT_GOORU_OID, event.getContentGooruId(), null)
				.putColumnIfNotNull(Constants._EVENT_NAME, event.getEventName(), null)
				.putColumnIfNotNull(Constants.SESSION, event.getSession().toString(), null)
				.putColumnIfNotNull(Constants.METRICS, event.getMetrics().toString(), null)
				.putColumnIfNotNull(Constants._PAYLOAD_OBJECT, event.getPayLoadObject().toString(), null)
				.putColumnIfNotNull(Constants.USER, event.getUser().toString(), null)
				.putColumnIfNotNull(Constants.CONTEXT, event.getContext().toString(), null)
				;
		try {
			m.execute();
		} catch (Exception e) {
			LOG.error("Exception:",e);
		}
		return key;

	}

	/**
	 * Saving data into event_timeline columnfamily
	 * 
	 * @param cfName
	 * @param rowKey
	 * @param CoulmnValue
	 * @param event
	 */
	@Override
	public void updateTimelineObject(final String cfName, final String rowKey, final String CoulmnValue, final EventBuilder event) {

		// UUID eventColumnTimeUUID = TimeUUIDUtils.getUniqueTimeUUIDinMillis();

		MutationBatch eventTimeline = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));

		eventTimeline.withRow(this.accessColumnFamily(cfName), rowKey).putColumn(CoulmnValue, CoulmnValue, null);

		eventTimeline.withRow(this.accessColumnFamily(cfName), (rowKey + Constants.SEPERATOR + event.getEventName())).putColumn(CoulmnValue, CoulmnValue, null);

		eventTimeline.withRow(this.accessColumnFamily(cfName), (rowKey.substring(0, 8) + Constants.SEPERATOR + event.getEventName())).putColumn(CoulmnValue, CoulmnValue, null);

		try {
			eventTimeline.execute();
		} catch (Exception e) {
			LOG.error("Exception:",e);
		}
	}

	/**
	 * 
	 * @param cfName
	 * @param eventData
	 * @param rowKey
	 */
	@Override
	public void updateTimeline(String cfName, EventData eventData, String rowKey) {

		// UUID eventColumnTimeUUID = TimeUUIDUtils.getUniqueTimeUUIDinMillis();

		MutationBatch eventTimelineMutation = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));

		eventTimelineMutation.withRow(this.accessColumnFamily(cfName), rowKey).putColumn(eventData.getEventKeyUUID(), eventData.getEventKeyUUID(), null);

		eventTimelineMutation.withRow(this.accessColumnFamily(cfName), (rowKey + "~" + eventData.getEventName())).putColumn(eventData.getEventKeyUUID(), eventData.getEventKeyUUID(), null);

		eventTimelineMutation.withRow(this.accessColumnFamily(cfName), (rowKey.substring(0, 8) + "~" + eventData.getEventName())).putColumn(eventData.getEventKeyUUID(), eventData.getEventKeyUUID(),
				null);

		try {
			eventTimelineMutation.execute();
		} catch (Exception e) {
			LOG.error("Exception:",e);
		}
	}

	/**
	 * 
	 * @param cfName
	 * @param activities
	 */
	@Override
	public void saveActivity(String cfName, HashMap<String, Object> activities) {
		String rowKey = null;
		String dateId = "0";
		String columnName = null;
		String eventName = null;
		rowKey = activities.get(Constants.USER_UID) != null ? activities.get(Constants.USER_UID).toString() : null;
		dateId = activities.get("dateId") != null ? activities.get("dateId").toString() : null;
		eventName = activities.get("eventName") != null ? activities.get("eventName").toString() : null;
		if (activities.get("existingColumnName") == null) {
			columnName = ((dateId.toString() == null ? "0L" : dateId.toString()) + "~" + (eventName == null ? "NA" : activities.get("eventName").toString()) + "~" + activities.get("eventId")
					.toString());
		} else {
			columnName = activities.get("existingColumnName").toString();
		}

		try {
			MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));
			m.withRow(this.accessColumnFamily(cfName), rowKey).putColumnIfNotNull(columnName, activities.get("activity") != null ? activities.get("activity").toString() : null, null)

			;
			m.execute();
		} catch (Exception e) {
			LOG.error("Exception:",e);
		}
	}

	/**
	 * 
	 * @param cfName
	 * @param userUid
	 * @param eventId
	 * @return
	 */
	@Override
	public Map<String, Object> isEventIdExists(String cfName, String userUid, String eventId) {
		OperationResult<ColumnList<String>> eventColumns = null;
		Map<String, Object> resultMap = new HashMap<String, Object>();
		List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
		Boolean isExists = false;
		try {
			eventColumns = getKeyspace().prepareQuery(this.accessColumnFamily(cfName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5)).getKey(userUid)
					.execute();
			if (eventColumns != null)
				for (Column<String> eventColumn : eventColumns.getResult()) {
					String columnName = eventColumn.getName();
					if (columnName != null) {
						if (columnName.contains(eventId)) {
							isExists = true;
							resultMap.put("isExists", isExists);
							resultMap.put("jsonString", eventColumn.getStringValue());
							resultMap.put("existingColumnName", columnName);
							resultList.add(resultMap);
							return resultMap;
						}
					}
				}
		} catch (Exception e) {
			LOG.error("Exception:",e);
		}
		if (!isExists) {
			resultMap.put("isExists", isExists);
			resultMap.put("jsonString", null);
			resultList.add(resultMap);
		}
		return resultMap;
	}

	/**
	 * 
	 * @param is
	 * @return
	 */
	protected static String convertStreamToString(InputStream is) {
		BufferedReader reader = new BufferedReader(new InputStreamReader(is));
		StringBuilder sb = new StringBuilder();

		String line = null;
		try {
			while ((line = reader.readLine()) != null) {
				sb.append(line + "\n");
			}
		} catch (IOException e) {
			LOG.error("Exception:",e);
		} finally {
			try {
				is.close();
			} catch (Exception e) {
				LOG.error("Exception:",e);
			}
		}
		return sb.toString();
	}

	/**
	 * Get first level parents.
	 * 
	 * @param cfName
	 * @param key
	 * @param retryCount
	 * @return
	 */
	@Override
	public List<String> getParentIds(final String cfName, final String key) {

		Rows<String, String> collectionItem = null;
		List<String> classPages = new ArrayList<String>();
		String parentId = null;
		try {
			collectionItem = getKeyspace().prepareQuery(this.accessColumnFamily(cfName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5)).searchWithIndex()
					.addExpression().whereColumn(Constants._RESOURCE_GOORU_OID).equals().value(key).execute().getResult();
		} catch (Exception e) {			
				LOG.error("Exception:",e);
		}
		if (collectionItem != null) {
			for (Row<String, String> collectionItems : collectionItem) {
				parentId = collectionItems.getColumns().getColumnByName(Constants._COLLECTION_GOORU_OID).getStringValue();
				if (parentId != null) {
					classPages.add(parentId);
				}
			}
		}
		return classPages;
	}

	/**
	 * 
	 * @param cfName
	 * @param eventMap
	 */
	@Override
	public void updateCollectionItem(String cfName, Map<String, String> eventMap) {

		MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));

		m.withRow(this.accessColumnFamily(cfName), eventMap.get(Constants.COLLECTION_ITEM_ID))
				.putColumnIfNotNull(Constants._CONTENT_ID, eventMap.get(Constants.CONTENT_ID))
				.putColumnIfNotNull(Constants._PARENT_CONTENT_ID, eventMap.get(Constants.PARENT_CONTENT_ID))
				.putColumnIfNotNull(Constants._RESOURCE_GOORU_OID, eventMap.get(Constants._CONTENT_GOORU_OID))
				.putColumnIfNotNull(Constants._COLLECTION_GOORU_OID, eventMap.get(Constants._PARENT_GOORU_OID))
				.putColumnIfNotNull(Constants._ITEM_SEQUENCE, eventMap.get(Constants.ITEM_SEQUENCE))
				.putColumnIfNotNull(Constants._ORGANIZATION_UID, eventMap.get(Constants.ORGANIZATION_UID));

	}

	/**
	 * 
	 * @param cfName
	 * @param eventMap
	 */
	@Override
	public void updateClasspage(String cfName, Map<String, String> eventMap) {

		MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));

		int isGroupOwner = 0;
		int deleted = 0;
		m.withRow(this.accessColumnFamily(cfName), eventMap.get(Constants.CONTENT_GOORU_OID) + Constants.SEPERATOR + eventMap.get(Constants.GROUP_UID) + Constants.SEPERATOR + eventMap.get(Constants.GOORUID))
				.putColumnIfNotNull(Constants._USER_GROUP_UID, eventMap.get(Constants.GROUP_UID))
				.putColumnIfNotNull(Constants._CLASSPAGE_GOORU_OID, eventMap.get(Constants.CONTENT_GOORU_OID))
				.putColumnIfNotNull(Constants._IS_GROUP_OWNER, isGroupOwner)
				.putColumnIfNotNull(Constants.DELETED, deleted)
				.putColumnIfNotNull(Constants._GOORU_UID, eventMap.get(Constants.GOORUID))
				.putColumnIfNotNull(Constants._CLASSPAGE_CODE, eventMap.get(Constants.CLASS_CODE))
				.putColumnIfNotNull(Constants._USER_GROUP_CODE, eventMap.get(Constants._CONTENT_GOORU_OID))
				.putColumnIfNotNull(Constants._ORGANIZATION_UID, eventMap.get(Constants.ORGANIZATION_UID))

		;

		try {
			m.execute();
		} catch (Exception e) {
			LOG.error("Exception:",e);
		}
	}

	/**
	 * 
	 * @param cfName
	 * @param key
	 * @param classPageGooruOid
	 * @param retryCount
	 * @return
	 */
	@Override
	public boolean getClassPageOwnerInfo(String cfName, String key, String classPageGooruOid) {

		Rows<String, String> result = null;
		try {
			result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5)).searchWithIndex()
					.addExpression().whereColumn("gooru_uid").equals().value(key).addExpression().whereColumn("classpage_gooru_oid").equals().value(classPageGooruOid).addExpression()
					.whereColumn("is_group_owner").equals().value(1).execute().getResult();
		} catch (Exception e) {
			LOG.error("Exception:",e);
		}
		if (result != null && !result.isEmpty()) {
			return true;
		}
		return false;
	}

	/**
	 * 
	 * @param cfName
	 * @param key
	 * @param classPageGooruOid
	 * @param retryCount
	 * @return
	 */
	@Override
	public boolean isUserPartOfClass(String cfName, String key, String classPageGooruOid) {

		Rows<String, String> result = null;
		try {
			result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5)).searchWithIndex()
					.addExpression().whereColumn("classpage_gooru_oid").equals().value(classPageGooruOid).addExpression().whereColumn("gooru_uid").equals().value(key).execute().getResult();
		} catch (ConnectionException e) {
			LOG.error("Exception:",e);
		}

		if (result != null && !result.isEmpty()) {
			return true;
		}
		return false;

	}

	/**
	 * 
	 * @param resourceco
	 */
	@Override
	public void updateResourceEntity(ResourceCo resourceco) {
		try {
			getResourceEntityPersister().put(resourceco);
		} catch (Exception e) {
			LOG.error("Exception:",e);
		}
	}

	/**
	 * 
	 * @param userCo
	 */
	@Override
	public void updateUserEntity(UserCo userCo) {
		try {
			getUserEntityPersister().put(userCo);
		} catch (Exception e) {
			LOG.error("Exception:",e);
		}
	}

	/**
	 * 
	 * @param cfName
	 * @param eventMap
	 */
	@Override
	public void updateAssessmentAnswer(String cfName, Map<String, Object> eventMap) {

		MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));
		m.withRow(this.accessColumnFamily(cfName), eventMap.get(Constants.COLLECTION_GOORU_OID) + Constants.SEPERATOR + eventMap.get("questionGooruOid") + Constants.SEPERATOR + eventMap.get("sequence"))
				.putColumnIfNotNull(Constants._COLLECTION_GOORU_OID, eventMap.get(Constants.COLLECTION_GOORU_OID).toString())
				.putColumnIfNotNull("question_id", ((eventMap.containsKey("questionId") && eventMap.get("questionId") != null) ? Long.valueOf(eventMap.get("questionId").toString()) : null))
				.putColumnIfNotNull("answer_id", ((eventMap.containsKey("answerId") && eventMap.get("answerId") != null) ? Long.valueOf(eventMap.get("answerId").toString()) : null))
				.putColumnIfNotNull("answer_text", ((eventMap.containsKey("answerText") && eventMap.get("answerText") != null) ? eventMap.get("answerText").toString() : null))
				.putColumnIfNotNull("is_correct", ((eventMap.containsKey("isCorrect") && eventMap.get("isCorrect") != null) ? Integer.valueOf(eventMap.get("isCorrect").toString()) : null))
				.putColumnIfNotNull("type_name", ((eventMap.containsKey("typeName") && eventMap.get("typeName") != null) ? eventMap.get("typeName").toString() : null))
				.putColumnIfNotNull("answer_hint", ((eventMap.containsKey("answerHint") && eventMap.get("answerHint") != null) ? eventMap.get("answerHint").toString() : null))
				.putColumnIfNotNull("answer_explanation",
						((eventMap.containsKey("answerExplanation") && eventMap.get("answerExplanation") != null) ? eventMap.get("answerExplanation").toString() : null))
				.putColumnIfNotNull("question_gooru_oid", ((eventMap.containsKey("questionGooruOid") && eventMap.get("questionGooruOid") != null) ? eventMap.get("questionGooruOid").toString() : null))
				.putColumnIfNotNull(Constants._QUESTION_TYPE, ((eventMap.containsKey(Constants.QUESTION_TYPE) && eventMap.get(Constants.QUESTION_TYPE) != null) ? eventMap.get(Constants.QUESTION_TYPE).toString() : null))
				.putColumnIfNotNull("sequence", ((eventMap.containsKey("sequence") && eventMap.get("sequence") != null) ? Integer.valueOf(eventMap.get("sequence").toString()) : null));
		try {
			m.execute();
		} catch (Exception e) {
			LOG.error("Exception:",e);
		}
	}

	/**
	 * 
	 * @param cfName
	 * @param eventMap
	 */
	@Override
	public void updateCollection(String cfName, Map<String, Object> eventMap) {
		if (eventMap.containsKey("gooruOid") && eventMap.get("gooruOid") != null) {
			MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));
			m.withRow(this.accessColumnFamily(cfName), eventMap.get("gooruOid").toString()).putColumnIfNotNull("gooru_oid", eventMap.get("gooruOid").toString())
					.putColumnIfNotNull("content_id", (eventMap.get("contentId") != null ? Long.valueOf(eventMap.get("contentId").toString()) : null))
					.putColumnIfNotNull("collection_type", (eventMap.get("collectionType") != null ? eventMap.get("collectionType").toString() : null))
					.putColumnIfNotNull("grade", (eventMap.get("grade") != null ? eventMap.get("grade").toString() : null))
					.putColumnIfNotNull("goals", eventMap.get("goals") != null ? eventMap.get("goals").toString() : null)
					.putColumnIfNotNull("ideas", eventMap.get("ideas") != null ? eventMap.get("ideas").toString() : null)
					.putColumnIfNotNull("performance_tasks", eventMap.get("performanceTasks") != null ? eventMap.get("performanceTasks").toString() : null)
					.putColumnIfNotNull("language", eventMap.get("language") != null ? eventMap.get("language").toString() : null)
					.putColumnIfNotNull("key_points", eventMap.get("keyPoints") != null ? eventMap.get("keyPoints").toString() : null)
					.putColumnIfNotNull("notes", eventMap.get("notes") != null ? eventMap.get("notes").toString() : null)
					.putColumnIfNotNull("language_objective", eventMap.get("languageObjective") != null ? eventMap.get("languageObjective").toString() : null)
					.putColumnIfNotNull("network", eventMap.get("network") != null ? eventMap.get("network").toString() : null)
					.putColumnIfNotNull("mail_notification", (eventMap.get("mailNotification") != null && Boolean.valueOf(eventMap.get("mailNotification").toString())) ? true : false)
					.putColumnIfNotNull("build_type_id", eventMap.get("buildTypeId") != null ? eventMap.get("buildTypeId").toString() : null)
					.putColumnIfNotNull("narration_link", eventMap.get("narrationLink") != null ? eventMap.get("narrationLink").toString() : null)
					.putColumnIfNotNull(Constants._ESTIMATED_TIME, eventMap.get(Constants.ESTIMATED_TIME) != null ? eventMap.get(Constants.ESTIMATED_TIME).toString() : null);
			try {
				m.execute();
			} catch (Exception e) {
				LOG.error("Exception:",e);
			}
		}
	}

	/**
	 * 
	 * @param cfName
	 * @param eventMap
	 */
	@Override
	public void updateCollectionItemCF(String cfName, Map<String, Object> eventMap) {
		if (eventMap.containsKey(Constants.COLLECTION_ITEM_ID) && eventMap.get(Constants.COLLECTION_ITEM_ID) != null) {
			MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));
			m.withRow(this.accessColumnFamily(cfName), eventMap.get(Constants.COLLECTION_ITEM_ID).toString())
					.putColumnIfNotNull(Constants.DELETED, eventMap.get(Constants.DELETED) != null ? Integer.valueOf(eventMap.get(Constants.DELETED).toString()) : null)
					.putColumnIfNotNull(Constants._ITEM_TYPE, (eventMap.get(Constants.COLLECTION_ITEM_TYPE) != null ? eventMap.get(Constants.ITEM_TYPE).toString() : null))
					.putColumnIfNotNull(Constants._RESOURCE_CONTENT_ID, eventMap.get(Constants.RESOURCE_CONTENT_ID) != null ? Long.valueOf(eventMap.get(Constants.RESOURCE_CONTENT_ID).toString()) : null)
					.putColumnIfNotNull(Constants._COLLECTION_GOORU_OID, eventMap.get(Constants.COLLECTION_GOORU_OID) != null ? eventMap.get(Constants.COLLECTION_GOORU_OID).toString() : null)
					.putColumnIfNotNull(Constants._RESOURCE_GOORU_OID, eventMap.get(Constants.RESOURCE_GOORU_OID) != null ? eventMap.get(Constants.RESOURCE_GOORU_OID).toString() : null)
					.putColumnIfNotNull(Constants._ITEM_SEQUENCE, eventMap.get(Constants.COLLECTION_ITEM_SEQUENCE) != null ? Integer.valueOf(eventMap.get(Constants.ITEM_SEQUENCE).toString()) : null)
					.putColumnIfNotNull(Constants._COLLECTION_ITEM_ID, eventMap.get(Constants.COLLECTION_ITEM_ID) != null ? eventMap.get(Constants.COLLECTION_ITEM_ID).toString() : null)
					.putColumnIfNotNull(Constants._COLLECTION_CONTENT_ID, eventMap.get(Constants.COLLECTION_CONTENT_ID) != null ? Long.valueOf(eventMap.get(Constants.COLLECTION_CONTENT_ID).toString()) : null)
					.putColumnIfNotNull(Constants._QUESTION_TYPE, eventMap.get(Constants.COLLECTION_ITEM_QUESTION_TYPE) != null ? eventMap.get(Constants.QUESTION_TYPE).toString() : null)
					.putColumnIfNotNull(Constants._MINIMUM_SCORE, eventMap.get(Constants.COLLECTION_ITEM_MINIMUM_SCORE) != null ? eventMap.get(Constants.MINIMUM_SCORE).toString() : null)
					.putColumnIfNotNull(Constants.NARRATION, eventMap.get(Constants.COLLECTION_ITEM_NARRATION) != null ? eventMap.get(Constants.NARRATION).toString() : null)
					.putColumnIfNotNull(Constants._ESTIMATED_TIME, eventMap.get(Constants.COLLECTION_ITEM_ESTIMATED_TIME) != null ? eventMap.get(Constants.ESTIMATED_TIME).toString() : null)
					.putColumnIfNotNull(Constants.START, eventMap.get(Constants.COLLECTION_ITEM_START) != null ? eventMap.get(Constants.START).toString() : null)
					.putColumnIfNotNull(Constants.STOP, eventMap.get(Constants.COLLECTION_ITEM_STOP) != null ? eventMap.get(Constants.STOP).toString() : null)
					.putColumnIfNotNull(Constants._NARRATION_TYPE, eventMap.get(Constants.COLLECTION_NARRATION_TYPE) != null ? eventMap.get(Constants.NARRATION_TYPE).toString() : null)
					.putColumnIfNotNull(Constants._PLANNED_END_DATE, eventMap.get(Constants.COLLECTION_ITEM_PLANNED_END_DATE) != null ? new Timestamp(Long.valueOf(eventMap.get(Constants.PLANNED_END_DATE).toString())) : null)
					.putColumnIfNotNull(Constants._ASSOCIATION_DATE, eventMap.get(Constants.COLLECTION_ITEM_ASSOCIATION_DATE) != null ? new Timestamp(Long.valueOf(eventMap.get(Constants.ASSOCIATION_DATE).toString())) : null)
					.putColumnIfNotNull(Constants._ASSOCIATED_BY_UID, eventMap.get(Constants.COLLECTION_ITEM_ASSOCIATE_BY_UID) != null ? eventMap.get(Constants.ASSOCIATED_BY_UID).toString() : null)
					.putColumnIfNotNull(Constants._IS_REQUIRED, eventMap.get(Constants.COLLECTION_ITEM_IS_REQUIRED) != null ? Integer.valueOf(eventMap.get(Constants.IS_REQUIRED).toString()) : null);
			try {
				m.execute();
			} catch (Exception e) {
				LOG.error("Exception:",e);
			}
		}
	}

	/**
	 * 
	 * @param cfName
	 * @param eventMap
	 */
	@Override
	public void updateClasspageCF(String cfName, Map<String, Object> eventMap) {
		if (eventMap.get(Constants.CLASS_ID) != null && eventMap.get(Constants.GROUP_UID) != null && eventMap.get(Constants.USER_UID) != null) {
			MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));
			m.withRow(this.accessColumnFamily(cfName), eventMap.get(Constants.CLASS_ID).toString() + Constants.SEPERATOR + eventMap.get(Constants.GROUP_UID).toString() + Constants.SEPERATOR + eventMap.get(Constants.USER_UID).toString())
					.putColumnIfNotNull(Constants.DELETED, eventMap.get(Constants.DELETED) != null ? Integer.valueOf(eventMap.get(Constants.DELETED).toString()) : 0)
					.putColumnIfNotNull(Constants._CLASSPAGE_CONTENT_ID, eventMap.get(Constants.CONTENT_ID) != null ? Long.valueOf(eventMap.get(Constants.CONTENT_ID).toString()) : null)
					.putColumnIfNotNull(Constants._CLASSPAGE_GOORU_OID, eventMap.get(Constants.CLASS_ID) != null ? eventMap.get(Constants.CLASS_ID).toString() : null)
					.putColumnIfNotNull(Constants.USERNAME, eventMap.get(Constants.USERNAME) != null ? eventMap.get(Constants.USERNAME).toString() : null)
					.putColumnIfNotNull(Constants._USER_GROUP_UID, eventMap.get(Constants.GROUP_UID) != null ? eventMap.get(Constants.GROUP_UID).toString() : null)
					.putColumnIfNotNull(Constants._ORGANIZATION_UID, eventMap.get(Constants.ORGANIZATION_UID) != null ? eventMap.get(Constants.ORGANIZATION_UID).toString() : null)
					.putColumnIfNotNull(Constants._USER_GROUP_TYPE, eventMap.get(Constants.USER_GROUP_TYPE) != null ? eventMap.get(Constants.USER_GROUP_TYPE).toString() : null)
					.putColumnIfNotNull(Constants._ACTIVE_FLAG, eventMap.get(Constants.ACTIVE_FLAG) != null ? Integer.valueOf(eventMap.get(Constants.ACTIVE_FLAG).toString()) : null)
					.putColumnIfNotNull(Constants._USER_GROUP_CODE, eventMap.get(Constants.CLASS_CODE) != null ? eventMap.get(Constants.CLASS_CODE).toString() : null)
					.putColumnIfNotNull(Constants._CLASSPAGE_CODE, eventMap.get(Constants.CLASS_CODE) != null ? eventMap.get(Constants.CLASS_CODE).toString() : null)
					.putColumnIfNotNull(Constants._GOORU_UID, eventMap.get(Constants.USER_UID) != null ? eventMap.get(Constants.USER_UID).toString() : null)
					.putColumnIfNotNull(Constants._IS_GROUP_OWNER, eventMap.get(Constants.IS_GROUP_OWNER) != null ? Integer.valueOf(eventMap.get(Constants.IS_GROUP_OWNER).toString()) : null);
			try {
				m.execute();
			} catch (Exception e) {
				LOG.error("Exception:",e);
			}
		}
	}

	/**
	 * Given a child content id gets all it's parent / grand parents. This is primarily to cover cases where collections are in folder / library.
	 * 
	 * @param cfName
	 * @param childOid
	 *            Gooru OID for the resource / folder / collection.
	 * @param depth
	 * @return
	 * 
	 */
	@Override
	public Set<String> getAllLevelParents(String cfName, String childOid, final int depth) {
		Rows<String, String> collectionItemParents = null;
		Set<String> parentIds = new HashSet<String>();
		try {
			collectionItemParents = getKeyspace().prepareQuery(this.accessColumnFamily(cfName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5))
					.searchWithIndex().addExpression().whereColumn(Constants._RESOURCE_GOORU_OID).equals().value(childOid.trim()).execute().getResult();

			if (collectionItemParents != null) {
				for (Row<String, String> collectionItemParent : collectionItemParents) {
					String parentId = collectionItemParent.getColumns().getColumnByName(Constants._COLLECTION_GOORU_OID).getStringValue().trim();

					if (parentId != null && !parentId.equalsIgnoreCase(childOid)) {
						// We have a valid parent other than self.
						parentId = parentId.trim();
						parentIds.add(parentId);
						if (depth > Constants.RECURSION_MAX_DEPTH) {
							// This is safeguard and not supposed to happen in a normal nested folder condition. Identify / Fix if error is found.
							LOG.error("Max recursion depth {} exceeded", Constants.RECURSION_MAX_DEPTH);
							return parentIds;
						}
						Set<String> grandParents = getAllLevelParents(cfName, parentId, depth + 1);
						if (grandParents != null) {
							parentIds.addAll(grandParents);
						}
					}
				}
			}
		} catch (Exception e) {
			LOG.error("Exception:",e);
		}
		return parentIds;
	}

	@Override
	public void saveSession(String sessionActivityId,long eventTime,String status){
		final String INSERT_STATEMENT = "INSERT INTO session(session_activity_id, event_time, status) VALUES (?, ?, ?);";
		try {
			getKeyspace().prepareQuery(accessColumnFamily("session")).withCql(INSERT_STATEMENT).asPreparedStatement().withStringValue(sessionActivityId).withLongValue(eventTime)
					.withStringValue(status).execute();
		} catch (ConnectionException e) {
			LOG.error("Error:",e);
		}
	}
	
	@Override
	public String getParentId(final String cfName, final String key) {
		Rows<String, String> collectionItem = null;
		String parentId = null;
		try {
			collectionItem = getKeyspace().prepareQuery(this.accessColumnFamily(cfName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5)).searchWithIndex()
					.addExpression().whereColumn("resource_gooru_oid").equals().value(key).execute().getResult();
		} catch (ConnectionException e) {
				LOG.error("Exception:",e);
		}
		if (collectionItem != null) {
			for (Row<String, String> collectionItems : collectionItem) {
				parentId = collectionItems.getColumns().getColumnByName("collection_gooru_oid").getStringValue();
			}
		}
		return parentId;
	}
	
	/**
	 * Store all the session IDs if user playing collection from inside and outside of the class
	 * @param sessionId
	 * @param classUid
	 * @param courseUid
	 * @param unitUid
	 * @param lessonUid
	 * @param collectionUid
	 * @param userUid
	 * @param eventType
	 * @param eventTime
	 * @return true/false -- meaning operation success/fail
	 */
	@Override
	public boolean saveUserSession(String sessionId,String classUid,String courseUid,String unitUid,String lessonUid,String collectionUid,String userUid,String collectionType, String eventType,long eventTime) {
		try {
			BoundStatement boundStatement = new BoundStatement(PreparedStatmentQueries.INSERT_USER_SESSION);
			boundStatement.bind(userUid,collectionUid,collectionType,classUid,courseUid,unitUid,lessonUid,eventTime,eventType,sessionId);
			getCassSession().execute(boundStatement);
		} catch (Exception e) {
			LOG.error("Error while storing user sessions" ,e);
			return false;
		}
		return true;
	}
	
	@Override
	public boolean saveLastSession(String classUid,String courseUid,String unitUid,String lessonUid,String collectionUid,String userUid,String sessionId) {
		try {
			BoundStatement boundStatement = new BoundStatement(PreparedStatmentQueries.INSERT_USER_LAST_SESSION);
			boundStatement.bind(classUid,courseUid,unitUid,lessonUid,collectionUid,userUid,sessionId);
			getCassSession().execute(boundStatement);
		} catch (Exception e) {
			LOG.error("Error while storing user last sessions" ,e);
			return false;
		}
		return true;
	}
	/**
	 * Usage metrics will be store here by session wise 
	 * @param sessionId
	 * @param gooruOid
	 * @param collectionItemId
	 * @param answerObject
	 * @param attempts
	 * @param reaction
	 * @param resourceFormat
	 * @param resourceType
	 * @param score
	 * @param timeSpent
	 * @param views
	 * @return true/false -- meaning - operation success/fail
	 */
	@Override
	public boolean saveUserSessionActivity(UserSessionActivity userSessionActivity) {
		try {
			
			BoundStatement boundStatement = new BoundStatement(PreparedStatmentQueries.INSERT_USER_SESSION_ACTIVITY);
			boundStatement.bind(userSessionActivity.getSessionId() ,userSessionActivity.getGooruOid() ,userSessionActivity.getCollectionItemId() ,userSessionActivity.getAnswerObject().toString() ,userSessionActivity.getAttempts() ,userSessionActivity.getCollectionType() ,userSessionActivity.getResourceType() ,userSessionActivity.getQuestionType() ,userSessionActivity.getAnswerStatus() ,userSessionActivity.getEventType() ,userSessionActivity.getParentEventId() ,userSessionActivity.getReaction() ,userSessionActivity.getScore() ,userSessionActivity.getTimeSpent() ,userSessionActivity.getViews());
			getCassSession().execute(boundStatement);
		} catch (Exception e) {
			LOG.error("Error while storing user sessions activity" ,e);
			return false;
		}
		return true;
	}
	
	/**
	 * CULA/C aggregated metrics will be stored.
	 * @param classUid
	 * @param courseUid
	 * @param unitUid
	 * @param lessonUid
	 * @param collectionUid
	 * @param userUid
	 * @param collectionType
	 * @param score
	 * @param timeSpent
	 * @param views
	 * @return true/false -- meaning operation success/fail
	 */
	@Override
	public boolean saveStudentsClassActivity(StudentsClassActivity studentsClassActivity) {
		try {
			BoundStatement boundStatement = new BoundStatement(PreparedStatmentQueries.INSERT_STUDENTS_CLASS_ACTIVITY);
			boundStatement.bind(studentsClassActivity.getClassUid() ,studentsClassActivity.getCourseUid() ,studentsClassActivity.getUnitUid() ,studentsClassActivity.getLessonUid() ,studentsClassActivity.getCollectionUid() ,studentsClassActivity.getUserUid() ,studentsClassActivity.getCollectionType() ,studentsClassActivity.getAttemptStatus() ,studentsClassActivity.getScore() ,studentsClassActivity.getTimeSpent() ,studentsClassActivity.getViews() ,studentsClassActivity.getReaction());
			getCassSession().execute(boundStatement);
		} catch (Exception e) {
			LOG.error("Error while storing class activity" ,e);
			return false;
		}
		return true;
	}
	
	@Override
	public boolean saveContentTaxonomyActivity(ContentTaxonomyActivity contentTaxonomyActivity) {
		try {
			BoundStatement boundStatement = new BoundStatement(PreparedStatmentQueries.INSERT_CONTENT_TAXONOMY_ACTIVITY);
			boundStatement.bind(contentTaxonomyActivity.getUserUid() ,contentTaxonomyActivity.getSubjectId() ,contentTaxonomyActivity.getCourseId() ,contentTaxonomyActivity.getDomainId() ,contentTaxonomyActivity.getStandardsId() ,contentTaxonomyActivity.getLearningTargetsId() ,contentTaxonomyActivity.getGooruOid() ,contentTaxonomyActivity.getClassUid() ,contentTaxonomyActivity.getResourceType() ,contentTaxonomyActivity.getQuestionType() ,contentTaxonomyActivity.getScore() ,contentTaxonomyActivity.getTimeSpent() ,contentTaxonomyActivity.getViews());
			getCassSession().execute(boundStatement);
		} catch (Exception e) {
			LOG.error("Error while storing taxonomy activity" ,e);
			return false;
		}
		return true;
	}
	
	@Override
	public boolean saveContentClassTaxonomyActivity(ContentTaxonomyActivity contentTaxonomyActivity) {
		try {
			BoundStatement boundStatement = new BoundStatement(PreparedStatmentQueries.INSERT_CONTENT_CLASS_TAXONOMY_ACTIVITY);
			boundStatement.bind(contentTaxonomyActivity.getUserUid() ,contentTaxonomyActivity.getClassUid() ,contentTaxonomyActivity.getSubjectId() ,contentTaxonomyActivity.getCourseId() ,contentTaxonomyActivity.getDomainId() ,contentTaxonomyActivity.getStandardsId() ,contentTaxonomyActivity.getLearningTargetsId() ,contentTaxonomyActivity.getGooruOid() ,contentTaxonomyActivity.getClassUid() ,contentTaxonomyActivity.getResourceType() ,contentTaxonomyActivity.getQuestionType() ,contentTaxonomyActivity.getScore() ,contentTaxonomyActivity.getTimeSpent() ,contentTaxonomyActivity.getViews());
			getCassSession().execute(boundStatement);
		} catch (Exception e) {
			LOG.error("Error while storing taxonomy activity" ,e);
			return false;
		}
		return true;
	}
	
	/**
	 * Students current/left off CULA/CR will be stored
	 * @param userUid
	 * @param classUid
	 * @param courseUid
	 * @param unitUid
	 * @param lessonUid
	 * @param collectionUid
	 * @param resourceUid
	 * @param sessionTime
	 * @return true/false -- meaning operation success/fail
	 */
	@Override
	public boolean saveStudentLocation(StudentLocation studentLocation) {
		try {
			BoundStatement boundStatement = new BoundStatement(PreparedStatmentQueries.INSERT_USER_LOCATION);
			boundStatement.bind(studentLocation.getUserUid() ,studentLocation.getClassUid() ,studentLocation.getCourseUid() ,studentLocation.getUnitUid() ,studentLocation.getLessonUid() ,studentLocation.getCollectionUid() ,studentLocation.getCollectionType() ,studentLocation.getResourceUid() ,studentLocation.getSessionTime());
			getCassSession().execute(boundStatement);
		} catch (Exception e) {
			LOG.error("Error while storing taxonomy activity" ,e);
			return false;
		}
		return true;
	}
	
	@Override
	public UserSessionActivity compareAndMergeUserSessionActivity(UserSessionActivity userSessionActivity) {
		try {
			BoundStatement boundStatement = new BoundStatement(PreparedStatmentQueries.SELECT_USER_SESSION_ACTIVITY);
			boundStatement.bind(userSessionActivity.getSessionId(),userSessionActivity.getGooruOid(),userSessionActivity.getCollectionItemId());
			ResultSet result = getCassSession().execute(boundStatement);
			if (result != null) {
				for (com.datastax.driver.core.Row columns : result) {
					userSessionActivity.setAttempts((userSessionActivity.getAttempts()) + columns.getLong(Constants.ATTEMPTS));
					userSessionActivity.setTimeSpent((userSessionActivity.getTimeSpent() + columns.getLong(Constants._TIME_SPENT)));
					userSessionActivity.setViews((userSessionActivity.getViews() + columns.getLong(Constants.VIEWS)));
				}
			}
		} catch (Exception e) {
			LOG.error("Error while retreving user sessions activity" ,e);
		}
		return userSessionActivity;
	}
	
	@Override
	public UserSessionActivity getUserSessionActivity(String sessionId, String gooruOid, String collectionItemId) {
		UserSessionActivity userSessionActivity = new UserSessionActivity();
		try {
			BoundStatement boundStatement = new BoundStatement(PreparedStatmentQueries.SELECT_USER_SESSION_ACTIVITY);
			boundStatement.bind(sessionId,gooruOid,collectionItemId);
			ResultSet result = getCassSession().execute(boundStatement);
			if (result != null) {
				for (com.datastax.driver.core.Row columns : result) {
					userSessionActivity.setSessionId(columns.getString(Constants._SESSION_ID));
					userSessionActivity.setGooruOid(columns.getString(Constants._GOORU_OID));
					userSessionActivity.setCollectionItemId(columns.getString(Constants._COLLECTION_ITEM_ID));
					userSessionActivity.setAnswerObject(new JSONObject(columns.getString(Constants._ANSWER_OBECT)));
					userSessionActivity.setAnswerStatus(columns.getString(Constants._ANSWER_STATUS));
					userSessionActivity.setResourceType(columns.getString(Constants._RESOURCE_TYPE));
					userSessionActivity.setCollectionType(columns.getString(Constants._COLLECTION_TYPE));
					userSessionActivity.setQuestionType(columns.getString(Constants._QUESTION_TYPE));
					userSessionActivity.setAttempts(columns.getLong(Constants.ATTEMPTS));
					userSessionActivity.setEventType(columns.getString(Constants._EVENT_TYPE));
					userSessionActivity.setReaction(columns.getLong(Constants.REACTION));
					userSessionActivity.setScore(columns.getLong(Constants.SCORE));
					userSessionActivity.setTimeSpent(columns.getLong(Constants._TIME_SPENT));
					userSessionActivity.setViews(columns.getLong(Constants.VIEWS));
				}
			}
		} catch (Exception e) {
			LOG.error("Error while retreving user sessions activity" ,e);
		}
		return userSessionActivity;
	}
	@Override
	public StudentsClassActivity compareAndMergeStudentsClassActivity(StudentsClassActivity studentsClassActivity) {
		try {
			BoundStatement boundStatement = new BoundStatement(PreparedStatmentQueries.SELECT_STUDENTS_CLASS_ACTIVITY);
			boundStatement.bind(studentsClassActivity.getClassUid() ,studentsClassActivity.getUserUid() ,studentsClassActivity.getCollectionType() ,studentsClassActivity.getCourseUid() ,studentsClassActivity.getUnitUid() ,studentsClassActivity.getLessonUid() ,studentsClassActivity.getCollectionUid());
			ResultSet result = getCassSession().execute(boundStatement);
			if (result != null) {
				for (com.datastax.driver.core.Row columns : result) {
					studentsClassActivity.setTimeSpent((studentsClassActivity.getTimeSpent() + columns.getLong(Constants._TIME_SPENT)));
					studentsClassActivity.setViews((studentsClassActivity.getViews())+columns.getLong(Constants.VIEWS));
				}
			}
		} catch (Exception e) {
			LOG.error("Error while retreving students class activity" ,e);
		}
		return studentsClassActivity;
	}
	
	@Override
	public boolean updateReaction(UserSessionActivity userSessionActivity) {
		try {
			BoundStatement boundStatement = new BoundStatement(PreparedStatmentQueries.UPDATE_REACTION);
			boundStatement.bind(userSessionActivity.getSessionId(),userSessionActivity.getGooruOid(),userSessionActivity.getCollectionItemId(),userSessionActivity.getReaction());
			getCassSession().execute(boundStatement);
		} catch (Exception e) {
			LOG.error("Error while storing user sessions activity" ,e);
			return false;
		}
		return true;
	}
	
	@Override
	public boolean  hasClassActivity(StudentsClassActivity studentsClassActivity) {
		boolean hasActivity = false;
		try {
			BoundStatement boundStatement = new BoundStatement(PreparedStatmentQueries.SELECT_STUDENTS_CLASS_ACTIVITY);
			boundStatement.bind(studentsClassActivity.getClassUid() ,studentsClassActivity.getCourseUid() ,studentsClassActivity.getUnitUid() ,studentsClassActivity.getLessonUid() ,studentsClassActivity.getCollectionUid() ,studentsClassActivity.getCollectionType() ,studentsClassActivity.getUserUid());
			ResultSet result = getCassSession().execute(boundStatement);
			if(result != null){
				hasActivity = true;
			}
		} catch (Exception e) {
			LOG.error("Error while retreving students class activity" ,e);
		}
		return hasActivity;
	}

	@Override
	public UserSessionActivity getSessionScore(UserSessionActivity userSessionActivity, String eventName) {
		long score = 0L;
		try {
			String gooruOid = "";
			long questionCount = 0;
			long reactionCount = 0;
			long totalReaction = 0;
			if(LoaderConstants.CPV1.getName().equalsIgnoreCase(eventName)){
				gooruOid = userSessionActivity.getGooruOid();
			}else if (LoaderConstants.CRPV1.getName().equalsIgnoreCase(eventName)){
				gooruOid = userSessionActivity.getParentGooruOid();
			}
			
			BoundStatement boundStatement = new BoundStatement(PreparedStatmentQueries.SELECT_USER_SESSION_ACTIVITY_BY_SESSION_ID);
			boundStatement.bind(userSessionActivity.getSessionId());
			ResultSet result = getCassSession().execute(boundStatement);
			
			if (result != null) {
				for (com.datastax.driver.core.Row columns : result) {
						if(!gooruOid.equalsIgnoreCase(columns.getString(Constants._GOORU_OID)) && Constants.QUESTION.equalsIgnoreCase(columns.getString("resource_type"))){
							questionCount++;
							score += columns.getLong(Constants.SCORE);
						}
						if(LoaderConstants.CPV1.getName().equalsIgnoreCase(eventName) && columns.getLong(Constants.REACTION) > 0){
							reactionCount++;
							totalReaction += columns.getLong(Constants.REACTION);
						}
				}
				userSessionActivity.setScore(questionCount > 0 ? (score/questionCount) : 0);
				userSessionActivity.setReaction(reactionCount > 0 ? (totalReaction/reactionCount) : userSessionActivity.getReaction());
			}
		} catch (Exception e) {
			LOG.error("Error while retreving user sessions activity" ,e);
		}
		return userSessionActivity;
	}

	@Override
	public boolean saveClassActivityDataCube(ClassActivityDatacube studentsClassActivity) {
		try {			
			BoundStatement boundStatement = new BoundStatement(PreparedStatmentQueries.INSERT_CLASS_ACTIVITY_DATACUBE);
			boundStatement.bind(studentsClassActivity.getRowKey() ,studentsClassActivity.getLeafNode() ,studentsClassActivity.getCollectionType() ,studentsClassActivity.getUserUid() ,studentsClassActivity.getScore() ,studentsClassActivity.getTimeSpent() ,studentsClassActivity.getViews() ,studentsClassActivity.getReaction() ,studentsClassActivity.getCompletedCount());
			getCassSession().execute(boundStatement);
		} catch (Exception e) {
			LOG.error("Error while storing class activity" ,e);
			return false;
		}
		return true;
	}
	
	@Override
	public ClassActivityDatacube getStudentsClassActivityDatacube(String rowKey, String userUid, String collectionType) {
		ClassActivityDatacube classActivityDatacube = new ClassActivityDatacube();
		long itemCount = 0L;
		
		try {			
			BoundStatement boundStatement = new BoundStatement(PreparedStatmentQueries.SELECT_ALL_CLASS_ACTIVITY_DATACUBE);
			boundStatement.bind(rowKey,collectionType,userUid);
			ResultSet result = getCassSession().execute(boundStatement);
				
			if (result != null) {
				long score = 0L;
				long views = 0L;
				long timeSpent = 0L;
				for (com.datastax.driver.core.Row columns : result) {
					String attemptStatus = columns.getString(Constants._ATTEMPT_STATUS);
					attemptStatus = attemptStatus == null ? Constants.COMPLETED : attemptStatus;
					if(attemptStatus.equals(Constants.COMPLETED)){
						itemCount++;
						score += columns.getLong(Constants.SCORE);
					}
					timeSpent += columns.getLong(Constants._TIME_SPENT);
					views += columns.getLong(Constants.VIEWS);
				}
				classActivityDatacube.setCollectionType(collectionType);
				classActivityDatacube.setUserUid(userUid);
				classActivityDatacube.setScore(itemCount > 0 ? (score/itemCount) : 0L);
				classActivityDatacube.setTimeSpent(timeSpent);
				classActivityDatacube.setViews(views);
				classActivityDatacube.setCompletedCount(itemCount);
			}
		} catch (Exception e) {
			LOG.error("Error while retreving students class activity", e);
		}
		return classActivityDatacube;
	}
	
	@Override
	public ResultSet getTaxonomy(String rowKey){
		ResultSet result = null;
		try {
			BoundStatement boundStatement = new BoundStatement(PreparedStatmentQueries.SELECT_TAXONOMY_PARENT_NODE);
			boundStatement.bind(rowKey);
			result = getCassSession().execute(boundStatement);
			
		} catch (Exception e) {
			LOG.error("Error while retreving txonomy tree", e);
		}
		return result;
	}
	
	@Override
	public ResultSet getContentTaxonomyActivity(ContentTaxonomyActivity contentTaxonomyActivity){
		ResultSet result = null;
		try {
			BoundStatement boundStatement = new BoundStatement(PreparedStatmentQueries.SELECT_CONTENT_TAXONOMY_ACTIVITY);
			boundStatement.bind(contentTaxonomyActivity.getUserUid() ,contentTaxonomyActivity.getResourceType() ,contentTaxonomyActivity.getSubjectId() ,contentTaxonomyActivity.getCourseId() ,contentTaxonomyActivity.getDomainId() ,contentTaxonomyActivity.getStandardsId() ,contentTaxonomyActivity.getLearningTargetsId() ,contentTaxonomyActivity.getGooruOid());
			result = getCassSession().execute(boundStatement);
			
		} catch (Exception e) {
			LOG.error("Error while retreving students class activity", e);
		}
		return result;
	}
	
	@Override
	public ResultSet getContentClassTaxonomyActivity(ContentTaxonomyActivity contentTaxonomyActivity){
		ResultSet result = null;
		try {
			BoundStatement boundStatement = new BoundStatement(PreparedStatmentQueries.SELECT_CONTENT_CLASS_TAXONOMY_ACTIVITY);
			boundStatement.bind(contentTaxonomyActivity.getClassUid() ,contentTaxonomyActivity.getUserUid() ,contentTaxonomyActivity.getResourceType() ,contentTaxonomyActivity.getSubjectId() ,contentTaxonomyActivity.getCourseId() ,contentTaxonomyActivity.getDomainId() ,contentTaxonomyActivity.getStandardsId() ,contentTaxonomyActivity.getLearningTargetsId() ,contentTaxonomyActivity.getGooruOid());
			result = getCassSession().execute(boundStatement);
			
		} catch (Exception e) {
			LOG.error("Error while retreving students class activity", e);
		}
		return result;
	}
	
	@Override
	public ResultSet getContentTaxonomyActivityDataCube(String rowKey, String columnKey){
		ResultSet result = null;
		try {
			BoundStatement boundStatement = new BoundStatement(PreparedStatmentQueries.SELECT_TAXONOMY_ACTIVITY_DATACUBE);
			boundStatement.bind(rowKey,columnKey);
			result = getCassSession().execute(boundStatement);
			
		} catch (Exception e) {
			LOG.error("Error while retreving students class activity", e);
		}
		return result;
	}
	@Override
	public long getContentTaxonomyActivityScore(String rowKey){
		ResultSet result = null;
		long questionCount = 0L;
		long score = 0L;
		try {
			BoundStatement boundStatement = new BoundStatement(PreparedStatmentQueries.SELECT_TAXONOMY_ACTIVITY_DATACUBE);
			boundStatement.bind(rowKey);
			result = getCassSession().execute(boundStatement);
			
			if (result != null) {
				for (com.datastax.driver.core.Row columns : result) {
							questionCount++;
							score += columns.getLong(Constants.SCORE);
				}
				score = questionCount > 0 ? (score/questionCount) : 0;
			}
		} catch (Exception e) {
			LOG.error("Error while retreving students class activity", e);
		}
		return score;
	}
	
	@Override
	public boolean saveTaxonomyActivityDataCube(TaxonomyActivityDataCube taxonomyActivityDataCube) {
		try {
			BoundStatement boundStatement = new BoundStatement(PreparedStatmentQueries.INSERT_TAXONOMY_ACTIVITY_DATACUBE);
			boundStatement.bind(taxonomyActivityDataCube.getRowKey(),taxonomyActivityDataCube.getLeafNode(),taxonomyActivityDataCube.getViews(),taxonomyActivityDataCube.getAttempts(),taxonomyActivityDataCube.getResourceTimespent(),taxonomyActivityDataCube.getQuestionTimespent(),taxonomyActivityDataCube.getScore());
			getCassSession().execute(boundStatement);
		} catch (Exception e) {
			LOG.error("Error while storing question grade" ,e);
			return false;
		}
		return true;
	}
	
	@Override
	public boolean saveQuestionGrade(String teacherId, String userId, String sessionId, String questionId, long score) {
		try {
			BoundStatement boundStatement = new BoundStatement(PreparedStatmentQueries.INSERT_USER_QUESTION_GRADE);
			boundStatement.bind(teacherId,userId,sessionId,questionId,score);
			getCassSession().execute(boundStatement);
		} catch (Exception e) {
			LOG.error("Error while storing question grade" ,e);
			return false;
		}
		return true;
	}

	@Override
	public ResultSet getQuestionsGradeBySessionId(String teacherId, String userId, String sessionId) {
		ResultSet result = null;
		try {
			BoundStatement boundStatement = new BoundStatement(PreparedStatmentQueries.SELECT_USER_QUESTION_GRADE_BY_SESSION);
			boundStatement.bind(teacherId,userId,sessionId);
			result = getCassSession().execute(boundStatement);
			
		} catch (Exception e) {
			LOG.error("Exception while read questions grade by session", e);
		}
		return result;
	}
	
	@Override
	public ResultSet getQuestionsGradeByQuestionId(String teacherId, String userId, String sessionId, String questionId) {
		ResultSet result = null;
		try {
			BoundStatement boundStatement = new BoundStatement(PreparedStatmentQueries.SELECT_USER_QUESTION_GRADE_BY_QUESTION);
			boundStatement.bind(teacherId,userId,sessionId,questionId);
			result = getCassSession().execute(boundStatement);
			
		} catch (Exception e) {
			LOG.error("Exception while read questions grade by session", e);
		}
		return result;
	}
	@Override
	public boolean saveQuestionGradeInSession(String sessionId, String questionId, String collectionItemId, String status, long score) {
		try {			
			BoundStatement boundStatement = new BoundStatement(PreparedStatmentQueries.UPDATE_SESSION_SCORE);
			boundStatement.bind(sessionId,questionId,collectionItemId,status,score);
			getCassSession().execute(boundStatement);
		} catch (Exception e) {
			LOG.error("Error while storing question grade in session" ,e);
			return false;
		}
		return true;
	}
}
