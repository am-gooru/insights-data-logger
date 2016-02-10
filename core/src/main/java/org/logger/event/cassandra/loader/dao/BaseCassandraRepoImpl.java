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

import org.ednovo.data.model.ClassActivityDatacube;
import org.ednovo.data.model.ContentTaxonomyActivity;
import org.ednovo.data.model.Event;
import org.ednovo.data.model.EventData;
import org.ednovo.data.model.ResourceCo;
import org.ednovo.data.model.StudentLocation;
import org.ednovo.data.model.StudentsClassActivity;
import org.ednovo.data.model.TaxonomyActivityDataCube;
import org.ednovo.data.model.UserCo;
import org.ednovo.data.model.UserSessionActivity;
import org.logger.event.cassandra.loader.ColumnFamilySet;
import org.logger.event.cassandra.loader.Constants;
import org.logger.event.cassandra.loader.LoaderConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.ExceptionCallback;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.IndexQuery;
import com.netflix.astyanax.retry.ConstantBackoff;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.util.RangeBuilder;
import com.netflix.astyanax.util.TimeUUIDUtils;

@Component
public class BaseCassandraRepoImpl extends BaseDAOCassandraImpl implements Constants {

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
	 */
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

	/**
	 * Saving multiple String columns & values for single Key.
	 * 
	 * @param cfName
	 * @param key
	 * @param columnValueList
	 */
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
	public void generateTTLColumns(String cfName, String key, String columnName, String value, int expireTime, MutationBatch m) {
		m.withRow(this.accessColumnFamily(cfName), key).putColumn(columnName, value, expireTime);
	}

	/**
	 * Delete all the data from ColumnFamily
	 * 
	 * @param cfName
	 */
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
	public String saveEvent(String cfName, String key, Event event) {

		if (event.getEventId() == null) {
			UUID eventKeyUUID = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
			key = eventKeyUUID.toString();
		} else {
			key = event.getEventId();
		}

		MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));

		m.withRow(this.accessColumnFamily(cfName), key)
				.putColumnIfNotNull(_START_TIME, event.getStartTime(), null)
				.putColumnIfNotNull(_END_TIME, event.getEndTime(), null)
				.putColumnIfNotNull(FIELDS, event.getFields(), null)
				.putColumnIfNotNull(_CONTENT_GOORU_OID, event.getContentGooruId(), null)
				.putColumnIfNotNull(_EVENT_NAME, event.getEventName(), null)
				.putColumnIfNotNull(SESSION, event.getSession().toString(), null)
				.putColumnIfNotNull(METRICS, event.getMetrics().toString(), null)
				.putColumnIfNotNull(_PAYLOAD_OBJECT, event.getPayLoadObject().toString(), null)
				.putColumnIfNotNull(USER, event.getUser().toString(), null)
				.putColumnIfNotNull(CONTEXT, event.getContext().toString(), null)
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
	public void updateTimelineObject(final String cfName, final String rowKey, final String CoulmnValue, final Event event) {

		// UUID eventColumnTimeUUID = TimeUUIDUtils.getUniqueTimeUUIDinMillis();

		MutationBatch eventTimeline = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));

		eventTimeline.withRow(this.accessColumnFamily(cfName), rowKey).putColumn(CoulmnValue, CoulmnValue, null);

		eventTimeline.withRow(this.accessColumnFamily(cfName), (rowKey + SEPERATOR + event.getEventName())).putColumn(CoulmnValue, CoulmnValue, null);

		eventTimeline.withRow(this.accessColumnFamily(cfName), (rowKey.substring(0, 8) + SEPERATOR + event.getEventName())).putColumn(CoulmnValue, CoulmnValue, null);

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
	public void saveActivity(String cfName, HashMap<String, Object> activities) {
		String rowKey = null;
		String dateId = "0";
		String columnName = null;
		String eventName = null;
		rowKey = activities.get(USER_UID) != null ? activities.get(USER_UID).toString() : null;
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
	public List<String> getParentIds(final String cfName, final String key) {

		Rows<String, String> collectionItem = null;
		List<String> classPages = new ArrayList<String>();
		String parentId = null;
		try {
			collectionItem = getKeyspace().prepareQuery(this.accessColumnFamily(cfName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5)).searchWithIndex()
					.addExpression().whereColumn(_RESOURCE_GOORU_OID).equals().value(key).execute().getResult();
		} catch (Exception e) {			
				LOG.error("Exception:",e);
		}
		if (collectionItem != null) {
			for (Row<String, String> collectionItems : collectionItem) {
				parentId = collectionItems.getColumns().getColumnByName(_COLLECTION_GOORU_OID).getStringValue();
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
	public void updateCollectionItem(String cfName, Map<String, String> eventMap) {

		MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));

		m.withRow(this.accessColumnFamily(cfName), eventMap.get(COLLECTION_ITEM_ID))
				.putColumnIfNotNull(_CONTENT_ID, eventMap.get(CONTENT_ID))
				.putColumnIfNotNull(_PARENT_CONTENT_ID, eventMap.get(PARENT_CONTENT_ID))
				.putColumnIfNotNull(_RESOURCE_GOORU_OID, eventMap.get(_CONTENT_GOORU_OID))
				.putColumnIfNotNull(_COLLECTION_GOORU_OID, eventMap.get(_PARENT_GOORU_OID))
				.putColumnIfNotNull(_ITEM_SEQUENCE, eventMap.get(ITEM_SEQUENCE))
				.putColumnIfNotNull(_ORGANIZATION_UID, eventMap.get(ORGANIZATION_UID));

	}

	/**
	 * 
	 * @param cfName
	 * @param eventMap
	 */
	public void updateClasspage(String cfName, Map<String, String> eventMap) {

		MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));

		int isGroupOwner = 0;
		int deleted = 0;
		m.withRow(this.accessColumnFamily(cfName), eventMap.get(CONTENT_GOORU_OID) + SEPERATOR + eventMap.get(GROUP_UID) + SEPERATOR + eventMap.get(GOORUID))
				.putColumnIfNotNull(_USER_GROUP_UID, eventMap.get(GROUP_UID))
				.putColumnIfNotNull(_CLASSPAGE_GOORU_OID, eventMap.get(CONTENT_GOORU_OID))
				.putColumnIfNotNull(_IS_GROUP_OWNER, isGroupOwner)
				.putColumnIfNotNull(DELETED, deleted)
				.putColumnIfNotNull(_GOORU_UID, eventMap.get(GOORUID))
				.putColumnIfNotNull(_CLASSPAGE_CODE, eventMap.get(CLASS_CODE))
				.putColumnIfNotNull(_USER_GROUP_CODE, eventMap.get(_CONTENT_GOORU_OID))
				.putColumnIfNotNull(_ORGANIZATION_UID, eventMap.get(ORGANIZATION_UID))

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
	public void updateAssessmentAnswer(String cfName, Map<String, Object> eventMap) {

		MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));
		m.withRow(this.accessColumnFamily(cfName), eventMap.get(COLLECTION_GOORU_OID) + SEPERATOR + eventMap.get("questionGooruOid") + SEPERATOR + eventMap.get("sequence"))
				.putColumnIfNotNull(_COLLECTION_GOORU_OID, eventMap.get(COLLECTION_GOORU_OID).toString())
				.putColumnIfNotNull("question_id", ((eventMap.containsKey("questionId") && eventMap.get("questionId") != null) ? Long.valueOf(eventMap.get("questionId").toString()) : null))
				.putColumnIfNotNull("answer_id", ((eventMap.containsKey("answerId") && eventMap.get("answerId") != null) ? Long.valueOf(eventMap.get("answerId").toString()) : null))
				.putColumnIfNotNull("answer_text", ((eventMap.containsKey("answerText") && eventMap.get("answerText") != null) ? eventMap.get("answerText").toString() : null))
				.putColumnIfNotNull("is_correct", ((eventMap.containsKey("isCorrect") && eventMap.get("isCorrect") != null) ? Integer.valueOf(eventMap.get("isCorrect").toString()) : null))
				.putColumnIfNotNull("type_name", ((eventMap.containsKey("typeName") && eventMap.get("typeName") != null) ? eventMap.get("typeName").toString() : null))
				.putColumnIfNotNull("answer_hint", ((eventMap.containsKey("answerHint") && eventMap.get("answerHint") != null) ? eventMap.get("answerHint").toString() : null))
				.putColumnIfNotNull("answer_explanation",
						((eventMap.containsKey("answerExplanation") && eventMap.get("answerExplanation") != null) ? eventMap.get("answerExplanation").toString() : null))
				.putColumnIfNotNull("question_gooru_oid", ((eventMap.containsKey("questionGooruOid") && eventMap.get("questionGooruOid") != null) ? eventMap.get("questionGooruOid").toString() : null))
				.putColumnIfNotNull(_QUESTION_TYPE, ((eventMap.containsKey(QUESTION_TYPE) && eventMap.get(QUESTION_TYPE) != null) ? eventMap.get(QUESTION_TYPE).toString() : null))
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
					.putColumnIfNotNull(_ESTIMATED_TIME, eventMap.get(ESTIMATED_TIME) != null ? eventMap.get(ESTIMATED_TIME).toString() : null);
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
	public void updateCollectionItemCF(String cfName, Map<String, Object> eventMap) {
		if (eventMap.containsKey(COLLECTION_ITEM_ID) && eventMap.get(COLLECTION_ITEM_ID) != null) {
			MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));
			m.withRow(this.accessColumnFamily(cfName), eventMap.get(COLLECTION_ITEM_ID).toString())
					.putColumnIfNotNull(DELETED, eventMap.get(DELETED) != null ? Integer.valueOf(eventMap.get(DELETED).toString()) : null)
					.putColumnIfNotNull(_ITEM_TYPE, (eventMap.get(COLLECTION_ITEM_TYPE) != null ? eventMap.get(ITEM_TYPE).toString() : null))
					.putColumnIfNotNull(_RESOURCE_CONTENT_ID, eventMap.get(RESOURCE_CONTENT_ID) != null ? Long.valueOf(eventMap.get(RESOURCE_CONTENT_ID).toString()) : null)
					.putColumnIfNotNull(_COLLECTION_GOORU_OID, eventMap.get(COLLECTION_GOORU_OID) != null ? eventMap.get(COLLECTION_GOORU_OID).toString() : null)
					.putColumnIfNotNull(_RESOURCE_GOORU_OID, eventMap.get(RESOURCE_GOORU_OID) != null ? eventMap.get(RESOURCE_GOORU_OID).toString() : null)
					.putColumnIfNotNull(_ITEM_SEQUENCE, eventMap.get(COLLECTION_ITEM_SEQUENCE) != null ? Integer.valueOf(eventMap.get(ITEM_SEQUENCE).toString()) : null)
					.putColumnIfNotNull(_COLLECTION_ITEM_ID, eventMap.get(COLLECTION_ITEM_ID) != null ? eventMap.get(COLLECTION_ITEM_ID).toString() : null)
					.putColumnIfNotNull(_COLLECTION_CONTENT_ID, eventMap.get(COLLECTION_CONTENT_ID) != null ? Long.valueOf(eventMap.get(COLLECTION_CONTENT_ID).toString()) : null)
					.putColumnIfNotNull(_QUESTION_TYPE, eventMap.get(COLLECTION_ITEM_QUESTION_TYPE) != null ? eventMap.get(QUESTION_TYPE).toString() : null)
					.putColumnIfNotNull(_MINIMUM_SCORE, eventMap.get(COLLECTION_ITEM_MINIMUM_SCORE) != null ? eventMap.get(MINIMUM_SCORE).toString() : null)
					.putColumnIfNotNull(NARRATION, eventMap.get(COLLECTION_ITEM_NARRATION) != null ? eventMap.get(NARRATION).toString() : null)
					.putColumnIfNotNull(_ESTIMATED_TIME, eventMap.get(COLLECTION_ITEM_ESTIMATED_TIME) != null ? eventMap.get(ESTIMATED_TIME).toString() : null)
					.putColumnIfNotNull(START, eventMap.get(COLLECTION_ITEM_START) != null ? eventMap.get(START).toString() : null)
					.putColumnIfNotNull(STOP, eventMap.get(COLLECTION_ITEM_STOP) != null ? eventMap.get(STOP).toString() : null)
					.putColumnIfNotNull(_NARRATION_TYPE, eventMap.get(COLLECTION_NARRATION_TYPE) != null ? eventMap.get(NARRATION_TYPE).toString() : null)
					.putColumnIfNotNull(_PLANNED_END_DATE, eventMap.get(COLLECTION_ITEM_PLANNED_END_DATE) != null ? new Timestamp(Long.valueOf(eventMap.get(PLANNED_END_DATE).toString())) : null)
					.putColumnIfNotNull(_ASSOCIATION_DATE, eventMap.get(COLLECTION_ITEM_ASSOCIATION_DATE) != null ? new Timestamp(Long.valueOf(eventMap.get(ASSOCIATION_DATE).toString())) : null)
					.putColumnIfNotNull(_ASSOCIATED_BY_UID, eventMap.get(COLLECTION_ITEM_ASSOCIATE_BY_UID) != null ? eventMap.get(ASSOCIATED_BY_UID).toString() : null)
					.putColumnIfNotNull(_IS_REQUIRED, eventMap.get(COLLECTION_ITEM_IS_REQUIRED) != null ? Integer.valueOf(eventMap.get(IS_REQUIRED).toString()) : null);
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
	public void updateClasspageCF(String cfName, Map<String, Object> eventMap) {
		if (eventMap.get(CLASS_ID) != null && eventMap.get(GROUP_UID) != null && eventMap.get(USER_UID) != null) {
			MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));
			m.withRow(this.accessColumnFamily(cfName), eventMap.get(CLASS_ID).toString() + SEPERATOR + eventMap.get(GROUP_UID).toString() + SEPERATOR + eventMap.get(USER_UID).toString())
					.putColumnIfNotNull(DELETED, eventMap.get(DELETED) != null ? Integer.valueOf(eventMap.get(DELETED).toString()) : 0)
					.putColumnIfNotNull(_CLASSPAGE_CONTENT_ID, eventMap.get(CONTENT_ID) != null ? Long.valueOf(eventMap.get(CONTENT_ID).toString()) : null)
					.putColumnIfNotNull(_CLASSPAGE_GOORU_OID, eventMap.get(CLASS_ID) != null ? eventMap.get(CLASS_ID).toString() : null)
					.putColumnIfNotNull(USERNAME, eventMap.get(USERNAME) != null ? eventMap.get(USERNAME).toString() : null)
					.putColumnIfNotNull(_USER_GROUP_UID, eventMap.get(GROUP_UID) != null ? eventMap.get(GROUP_UID).toString() : null)
					.putColumnIfNotNull(_ORGANIZATION_UID, eventMap.get(ORGANIZATION_UID) != null ? eventMap.get(ORGANIZATION_UID).toString() : null)
					.putColumnIfNotNull(_USER_GROUP_TYPE, eventMap.get(USER_GROUP_TYPE) != null ? eventMap.get(USER_GROUP_TYPE).toString() : null)
					.putColumnIfNotNull(_ACTIVE_FLAG, eventMap.get(ACTIVE_FLAG) != null ? Integer.valueOf(eventMap.get(ACTIVE_FLAG).toString()) : null)
					.putColumnIfNotNull(_USER_GROUP_CODE, eventMap.get(CLASS_CODE) != null ? eventMap.get(CLASS_CODE).toString() : null)
					.putColumnIfNotNull(_CLASSPAGE_CODE, eventMap.get(CLASS_CODE) != null ? eventMap.get(CLASS_CODE).toString() : null)
					.putColumnIfNotNull(_GOORU_UID, eventMap.get(USER_UID) != null ? eventMap.get(USER_UID).toString() : null)
					.putColumnIfNotNull(_IS_GROUP_OWNER, eventMap.get(IS_GROUP_OWNER) != null ? Integer.valueOf(eventMap.get(IS_GROUP_OWNER).toString()) : null);
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
	public Set<String> getAllLevelParents(String cfName, String childOid, final int depth) {
		Rows<String, String> collectionItemParents = null;
		Set<String> parentIds = new HashSet<String>();
		try {
			collectionItemParents = getKeyspace().prepareQuery(this.accessColumnFamily(cfName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5))
					.searchWithIndex().addExpression().whereColumn(_RESOURCE_GOORU_OID).equals().value(childOid.trim()).execute().getResult();

			if (collectionItemParents != null) {
				for (Row<String, String> collectionItemParent : collectionItemParents) {
					String parentId = collectionItemParent.getColumns().getColumnByName(_COLLECTION_GOORU_OID).getStringValue().trim();

					if (parentId != null && !parentId.equalsIgnoreCase(childOid)) {
						// We have a valid parent other than self.
						parentId = parentId.trim();
						parentIds.add(parentId);
						if (depth > RECURSION_MAX_DEPTH) {
							// This is safeguard and not supposed to happen in a normal nested folder condition. Identify / Fix if error is found.
							LOG.error("Max recursion depth {} exceeded", RECURSION_MAX_DEPTH);
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

	public void saveSession(String sessionActivityId,long eventTime,String status){
		final String INSERT_STATEMENT = "INSERT INTO session(session_activity_id, event_time, status) VALUES (?, ?, ?);";
		OperationResult<CqlResult<String, String>> result;
		try {
			result = getKeyspace()
			        .prepareQuery(accessColumnFamily("session"))
			        .withCql(INSERT_STATEMENT)
			        .asPreparedStatement()
			        .withStringValue(sessionActivityId)
			        .withLongValue(eventTime)
			        .withStringValue(status)
			        .execute();
		} catch (ConnectionException e) {
			LOG.error("Error:",e);
		}
	}
	
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
	public boolean saveUserSession(String sessionId,String classUid,String courseUid,String unitUid,String lessonUid,String collectionUid,String userUid,String collectionType, String eventType,long eventTime) {
		try {
			getKeyspace().prepareQuery(accessColumnFamily(ColumnFamilySet.USER_SESSIONS.getColumnFamily()))
			.withCql(INSERT_USER_SESSION)
			.asPreparedStatement()
			.withStringValue(userUid)
			.withStringValue(collectionUid)
			.withStringValue(collectionType)
			.withStringValue(classUid)
			.withStringValue(courseUid)
			.withStringValue(unitUid)
			.withStringValue(lessonUid)
			.withLongValue(eventTime)
			.withStringValue(eventType)
			.withStringValue(sessionId)
			.execute()
			;
		} catch (ConnectionException e) {
			LOG.error("Error while storing user sessions" ,e);
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
	public boolean saveUserSessionActivity(UserSessionActivity userSessionActivity) {
		try {			
			getKeyspace().prepareQuery(accessColumnFamily(ColumnFamilySet.USER_SESSION_ACTIVITY.getColumnFamily()))
			.withCql(INSERT_USER_SESSION_ACTIVITY)
			.asPreparedStatement()
			.withStringValue(userSessionActivity.getSessionId())
			.withStringValue(userSessionActivity.getGooruOid())
			.withStringValue(userSessionActivity.getCollectionItemId())
			.withStringValue(userSessionActivity.getAnswerObject())
			.withLongValue(userSessionActivity.getAttempts())
			.withStringValue(userSessionActivity.getCollectionType())
			.withStringValue(userSessionActivity.getResourceType())
			.withStringValue(userSessionActivity.getQuestionType())
			.withStringValue(userSessionActivity.getAnswerStatus())
			.withStringValue(userSessionActivity.getEventType())
			.withLongValue(userSessionActivity.getReaction())
			.withLongValue(userSessionActivity.getScore())
			.withLongValue(userSessionActivity.getTimeSpent())
			.withLongValue(userSessionActivity.getViews())
			.execute()
			;
		} catch (ConnectionException e) {
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
	public boolean saveStudentsClassActivity(StudentsClassActivity studentsClassActivity) {
		try {			
			getKeyspace().prepareQuery(accessColumnFamily(ColumnFamilySet.STUDENTS_CLASS_ACTIVITY.getColumnFamily()))
			.withCql(INSERT_STUDENTS_CLASS_ACTIVITY)
			.asPreparedStatement()
			.withStringValue(studentsClassActivity.getClassUid())
			.withStringValue(studentsClassActivity.getCourseUid())
			.withStringValue(studentsClassActivity.getUnitUid())
			.withStringValue(studentsClassActivity.getLessonUid())
			.withStringValue(studentsClassActivity.getCollectionUid())
			.withStringValue(studentsClassActivity.getUserUid())
			.withStringValue(studentsClassActivity.getCollectionType())
			.withStringValue(studentsClassActivity.getAttemptStatus())
			.withLongValue(studentsClassActivity.getScore())
			.withLongValue(studentsClassActivity.getTimeSpent())
			.withLongValue(studentsClassActivity.getViews())
			.withLongValue(studentsClassActivity.getReaction())
			.execute()
			;
		} catch (ConnectionException e) {
			LOG.error("Error while storing class activity" ,e);
			return false;
		}
		return true;
	}
	
	public boolean saveContentTaxonomyActivity(ContentTaxonomyActivity contentTaxonomyActivity) {
		try {			
			getKeyspace().prepareQuery(accessColumnFamily(ColumnFamilySet.CONTENT_TAXONOMY_ACTIVITY.getColumnFamily()))
			.withCql(INSERT_CONTENT_TAXONOMY_ACTIVITY)
			.asPreparedStatement()
			.withStringValue(contentTaxonomyActivity.getUserUid())
			.withStringValue(contentTaxonomyActivity.getSubjectId())
			.withStringValue(contentTaxonomyActivity.getCourseId())
			.withStringValue(contentTaxonomyActivity.getDomainId())
			.withStringValue(contentTaxonomyActivity.getStandardsId())
			.withStringValue(contentTaxonomyActivity.getLearningTargetsId())
			.withStringValue(contentTaxonomyActivity.getGooruOid())
			.withStringValue(contentTaxonomyActivity.getClassUid())
			.withStringValue(contentTaxonomyActivity.getResourceType())
			.withStringValue(contentTaxonomyActivity.getQuestionType())
			.withLongValue(contentTaxonomyActivity.getScore())
			.withLongValue(contentTaxonomyActivity.getTimeSpent())
			.withLongValue(contentTaxonomyActivity.getViews())
			.execute()
			;
		} catch (ConnectionException e) {
			LOG.error("Error while storing taxonomy activity" ,e);
			return false;
		}
		return true;
	}
	
	public boolean saveContentClassTaxonomyActivity(ContentTaxonomyActivity contentTaxonomyActivity) {
		try {			
			getKeyspace().prepareQuery(accessColumnFamily(ColumnFamilySet.CONTENT_CLASS_TAXONOMY_ACTIVITY.getColumnFamily()))
			.withCql(INSERT_CONTENT_TAXONOMY_ACTIVITY)
			.asPreparedStatement()
			.withStringValue(contentTaxonomyActivity.getUserUid())
			.withStringValue(contentTaxonomyActivity.getClassUid())
			.withStringValue(contentTaxonomyActivity.getSubjectId())
			.withStringValue(contentTaxonomyActivity.getCourseId())
			.withStringValue(contentTaxonomyActivity.getDomainId())
			.withStringValue(contentTaxonomyActivity.getStandardsId())
			.withStringValue(contentTaxonomyActivity.getLearningTargetsId())
			.withStringValue(contentTaxonomyActivity.getGooruOid())
			.withStringValue(contentTaxonomyActivity.getClassUid())
			.withStringValue(contentTaxonomyActivity.getResourceType())
			.withStringValue(contentTaxonomyActivity.getQuestionType())
			.withLongValue(contentTaxonomyActivity.getScore())
			.withLongValue(contentTaxonomyActivity.getTimeSpent())
			.withLongValue(contentTaxonomyActivity.getViews())
			.execute()
			;
		} catch (ConnectionException e) {
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
	public boolean saveStudentLocation(StudentLocation studentLocation) {
		try {			
			getKeyspace().prepareQuery(accessColumnFamily(ColumnFamilySet.STUDNT_LOCATION.getColumnFamily()))
			.withCql(INSERT_USER_LOCATION)
			.asPreparedStatement()
			.withStringValue(studentLocation.getUserUid())
			.withStringValue(studentLocation.getClassUid())
			.withStringValue(studentLocation.getCourseUid())
			.withStringValue(studentLocation.getUnitUid())
			.withStringValue(studentLocation.getLessonUid())
			.withStringValue(studentLocation.getCollectionUid())
			.withStringValue(studentLocation.getCollectionType())
			.withStringValue(studentLocation.getResourceUid())
			.withLongValue(studentLocation.getSessionTime())
			.execute()
			;
		} catch (ConnectionException e) {
			LOG.error("Error while storing taxonomy activity" ,e);
			return false;
		}
		return true;
	}
	
	
	public UserSessionActivity compareAndMergeUserSessionActivity(UserSessionActivity userSessionActivity) {
		try {
			Rows<String, String> result = getKeyspace().prepareQuery(accessColumnFamily(ColumnFamilySet.USER_SESSION_ACTIVITY.getColumnFamily()))
			.withCql(SELECT_USER_SESSION_ACTIVITY)
			.asPreparedStatement()
			.withStringValue(userSessionActivity.getSessionId())
			.withStringValue(userSessionActivity.getGooruOid())
			.withStringValue(userSessionActivity.getCollectionItemId())
			.execute().getResult().getRows();
			;
			if (result.size() > 0) {
				for (Row<String, String> row : result) {
					ColumnList<String> columns = row.getColumns();
					userSessionActivity.setAttempts((userSessionActivity.getAttempts()) + columns.getLongValue(ATTEMPTS, 0L));
					userSessionActivity.setTimeSpent((userSessionActivity.getTimeSpent() + columns.getLongValue(_TIME_SPENT, 0L)));
					userSessionActivity.setViews((userSessionActivity.getViews() + columns.getLongValue(VIEWS, 0L)));
				}
			}
		} catch (ConnectionException e) {
			LOG.error("Error while retreving user sessions activity" ,e);
		}
		return userSessionActivity;
	}
	
	public UserSessionActivity getUserSessionActivity(String sessionId, String gooruOid, String collectionItemId) {
		UserSessionActivity userSessionActivity = new UserSessionActivity();
		try {
			Rows<String, String> result = getKeyspace().prepareQuery(accessColumnFamily(ColumnFamilySet.USER_SESSION_ACTIVITY.getColumnFamily()))
			.withCql(SELECT_USER_SESSION_ACTIVITY)
			.asPreparedStatement()
			.withStringValue(sessionId)
			.withStringValue(gooruOid)
			.withStringValue(collectionItemId)
			.execute().getResult().getRows();
			;
			if (result.size() > 0) {
				for (Row<String, String> row : result) {
					ColumnList<String> columns = row.getColumns();
					userSessionActivity.setSessionId(columns.getStringValue(_SESSION_ID, null));
					userSessionActivity.setGooruOid(columns.getStringValue(_GOORU_OID, null));
					userSessionActivity.setCollectionItemId(columns.getStringValue(_COLLECTION_ITEM_ID, null));
					userSessionActivity.setAnswerObject(columns.getStringValue(_ANSWER_OBECT, null));
					userSessionActivity.setAnswerStatus(columns.getStringValue(_ANSWER_STATUS, null));
					userSessionActivity.setResourceType(columns.getStringValue(_RESOURCE_TYPE, null));
					userSessionActivity.setCollectionType(columns.getStringValue(_COLLECTION_TYPE, null));
					userSessionActivity.setQuestionType(columns.getStringValue(_QUESTION_TYPE, null));
					userSessionActivity.setAttempts(columns.getLongValue(ATTEMPTS, 0L));
					userSessionActivity.setEventType(columns.getStringValue(_EVENT_TYPE, NA));
					userSessionActivity.setReaction(columns.getLongValue(REACTION, 0L));
					userSessionActivity.setScore(columns.getLongValue(SCORE, 0L));
					userSessionActivity.setTimeSpent(columns.getLongValue(_TIME_SPENT, 0L));
					userSessionActivity.setViews(columns.getLongValue(VIEWS, 0L));
				}
			}
		} catch (ConnectionException e) {
			LOG.error("Error while retreving user sessions activity" ,e);
		}
		return userSessionActivity;
	}
	public StudentsClassActivity compareAndMergeStudentsClassActivity(StudentsClassActivity studentsClassActivity) {
		try {
			Rows<String, String> result = getKeyspace().prepareQuery(accessColumnFamily(ColumnFamilySet.STUDENTS_CLASS_ACTIVITY.getColumnFamily()))
			.withCql(SELECT_STUDENTS_CLASS_ACTIVITY)
			.asPreparedStatement()
			.withStringValue(studentsClassActivity.getClassUid())
			.withStringValue(studentsClassActivity.getUserUid())
			.withStringValue(studentsClassActivity.getCollectionType())
			.withStringValue(studentsClassActivity.getCourseUid())
			.withStringValue(studentsClassActivity.getUnitUid())
			.withStringValue(studentsClassActivity.getLessonUid())
			.withStringValue(studentsClassActivity.getCollectionUid())
			.execute().getResult().getRows();
			;
			if (result.size() > 0) {
				for (Row<String, String> row : result) {
					ColumnList<String> columns = row.getColumns();
					studentsClassActivity.setTimeSpent((studentsClassActivity.getTimeSpent() + columns.getLongValue(_TIME_SPENT, 0L)));
					studentsClassActivity.setViews((studentsClassActivity.getViews())+columns.getLongValue(VIEWS, 0L));
				}
			}
		} catch (ConnectionException e) {
			LOG.error("Error while retreving students class activity" ,e);
		}
		return studentsClassActivity;
	}
	public boolean updateReaction(UserSessionActivity userSessionActivity) {
		try {			
			getKeyspace().prepareQuery(accessColumnFamily(ColumnFamilySet.USER_SESSION_ACTIVITY.getColumnFamily()))
			.withCql(UPDATE_REACTION)
			.asPreparedStatement()
			.withStringValue(userSessionActivity.getSessionId())
			.withStringValue(userSessionActivity.getGooruOid())
			.withStringValue(userSessionActivity.getCollectionItemId())
			.withLongValue(userSessionActivity.getReaction())
			.execute()
			;
		} catch (ConnectionException e) {
			LOG.error("Error while storing user sessions activity" ,e);
			return false;
		}
		return true;
	}
	public boolean  hasClassActivity(StudentsClassActivity studentsClassActivity) {
		boolean hasActivity = false;
		try {
			Rows<String, String> result = getKeyspace().prepareQuery(accessColumnFamily(ColumnFamilySet.STUDENTS_CLASS_ACTIVITY.getColumnFamily()))
			.withCql(SELECT_STUDENTS_CLASS_ACTIVITY)
			.asPreparedStatement()
			.withStringValue(studentsClassActivity.getClassUid())
			.withStringValue(studentsClassActivity.getCourseUid())
			.withStringValue(studentsClassActivity.getUnitUid())
			.withStringValue(studentsClassActivity.getLessonUid())
			.withStringValue(studentsClassActivity.getCollectionUid())
			.withStringValue(studentsClassActivity.getCollectionType())
			.withStringValue(studentsClassActivity.getUserUid())
			.execute().getResult().getRows();
			;
			if(result.size() > 0){
				hasActivity = true;
			}
		} catch (ConnectionException e) {
			LOG.error("Error while retreving students class activity" ,e);
		}
		return hasActivity;
	}

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
			Rows<String, String> result = getKeyspace().prepareQuery(accessColumnFamily(ColumnFamilySet.USER_SESSION_ACTIVITY.getColumnFamily()))
			.withCql(SELECT_USER_SESSION_ACTIVITY_BY_SESSION_ID)
			.asPreparedStatement()
			.withStringValue(userSessionActivity.getSessionId())
			.execute().getResult().getRows();
			;
			if (result.size() > 0) {
				for (Row<String, String> row : result) {
					ColumnList<String> columns = row.getColumns();
						if(!gooruOid.equalsIgnoreCase(columns.getStringValue(_GOORU_OID, null)) && QUESTION.equalsIgnoreCase(columns.getStringValue("resource_type", null))){
							questionCount++;
							score += columns.getLongValue(SCORE, 0L);
						}
						if(LoaderConstants.CPV1.getName().equalsIgnoreCase(eventName) && columns.getLongValue(REACTION, 0L) > 0){
							reactionCount++;
							totalReaction += columns.getLongValue(REACTION, 0L);
						}
				}
				userSessionActivity.setScore(questionCount > 0 ? (score/questionCount) : 0);
				userSessionActivity.setReaction(reactionCount > 0 ? (totalReaction/reactionCount) : userSessionActivity.getReaction());
			}
		} catch (ConnectionException e) {
			LOG.error("Error while retreving user sessions activity" ,e);
		}
		return userSessionActivity;
	}

	public boolean saveClassActivityDataCube(ClassActivityDatacube studentsClassActivity) {
		try {			
			getKeyspace().prepareQuery(accessColumnFamily(ColumnFamilySet.CLASS_ACTIVITY_DATACUBE.getColumnFamily()))
			.withCql(INSERT_CLASS_ACTIVITY_DATACUBE)
			.asPreparedStatement()
			.withStringValue(studentsClassActivity.getRowKey())
			.withStringValue(studentsClassActivity.getLeafNode())
			.withStringValue(studentsClassActivity.getCollectionType())
			.withStringValue(studentsClassActivity.getUserUid())
			.withLongValue(studentsClassActivity.getScore())
			.withLongValue(studentsClassActivity.getTimeSpent())
			.withLongValue(studentsClassActivity.getViews())
			.withLongValue(studentsClassActivity.getReaction())
			.withLongValue(studentsClassActivity.getCompletedCount())
			.execute()
			;
		} catch (ConnectionException e) {
			LOG.error("Error while storing class activity" ,e);
			return false;
		}
		return true;
	}
	
	public ClassActivityDatacube getStudentsClassActivityDatacube(String rowKey, String userUid, String collectionType) {
		ClassActivityDatacube classActivityDatacube = new ClassActivityDatacube();
		long itemCount = 0L;
		try {
			Rows<String, String> result = getKeyspace().prepareQuery(accessColumnFamily(ColumnFamilySet.CLASS_ACTIVITY_DATACUBE.getColumnFamily())).withCql(SELECT_ALL_CLASS_ACTIVITY_DATACUBE)
					.asPreparedStatement().withStringValue(rowKey).withStringValue(collectionType).withStringValue(userUid).execute().getResult().getRows();
			;
			if (result.size() > 0) {
				long score = 0L;
				long views = 0L;
				long timeSpent = 0L;
				for (Row<String, String> row : result) {
					ColumnList<String> columns = row.getColumns();
					if(columns.getStringValue(_ATTEMPT_STATUS, COMPLETED).equals(COMPLETED)){
						itemCount++;
						score += columns.getLongValue(SCORE, 0L);
					}
					timeSpent += columns.getLongValue(_TIME_SPENT, 0L);
					views += columns.getLongValue(VIEWS, 0L);
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
	
	public Rows<String, String> getTaxonomy(String rowKey){
		Rows<String, String> result = null;
		try {
			result = getKeyspace().prepareQuery(accessColumnFamily(ColumnFamilySet.TAXONOMY_PARENT_NODE.getColumnFamily())).withCql(SELECT_TAXONOMY_PARENT_NODE)
					.asPreparedStatement().withStringValue(rowKey).execute().getResult().getRows();
			;
		} catch (Exception e) {
			LOG.error("Error while retreving txonomy tree", e);
		}
		return result;
	}
	
	public Rows<String, String> getContentTaxonomyActivity(ContentTaxonomyActivity contentTaxonomyActivity){
		Rows<String, String> result = null;
		try {
			result = getKeyspace().prepareQuery(accessColumnFamily(ColumnFamilySet.CONTENT_TAXONOMY_ACTIVITY.getColumnFamily())).withCql(SELECT_CONTENT_TAXONOMY_ACTIVITY)
					.asPreparedStatement()
					.withStringValue(contentTaxonomyActivity.getUserUid())
					.withStringValue(contentTaxonomyActivity.getResourceType())
					.withStringValue(contentTaxonomyActivity.getSubjectId())
					.withStringValue(contentTaxonomyActivity.getCourseId())
					.withStringValue(contentTaxonomyActivity.getDomainId())
					.withStringValue(contentTaxonomyActivity.getStandardsId())
					.withStringValue(contentTaxonomyActivity.getLearningTargetsId())
					.withStringValue(contentTaxonomyActivity.getGooruOid())
					.execute().getResult().getRows();
			;
		} catch (Exception e) {
			LOG.error("Error while retreving students class activity", e);
		}
		return result;
	}
	public Rows<String, String> getContentClassTaxonomyActivity(ContentTaxonomyActivity contentTaxonomyActivity){
		Rows<String, String> result = null;
		try {
			result = getKeyspace().prepareQuery(accessColumnFamily(ColumnFamilySet.CONTENT_CLASS_TAXONOMY_ACTIVITY.getColumnFamily())).withCql(SELECT_CONTENT_CLASS_TAXONOMY_ACTIVITY)
					.asPreparedStatement()
					.withStringValue(contentTaxonomyActivity.getClassUid())
					.withStringValue(contentTaxonomyActivity.getUserUid())
					.withStringValue(contentTaxonomyActivity.getResourceType())
					.withStringValue(contentTaxonomyActivity.getSubjectId())
					.withStringValue(contentTaxonomyActivity.getCourseId())
					.withStringValue(contentTaxonomyActivity.getDomainId())
					.withStringValue(contentTaxonomyActivity.getStandardsId())
					.withStringValue(contentTaxonomyActivity.getLearningTargetsId())
					.withStringValue(contentTaxonomyActivity.getGooruOid())
					.execute().getResult().getRows();
			;
		} catch (Exception e) {
			LOG.error("Error while retreving students class activity", e);
		}
		return result;
	}
	
	public Rows<String, String> getContentTaxonomyActivityDataCube(String rowKey, String columnKey){
		Rows<String, String> result = null;
		try {
			result = getKeyspace().prepareQuery(accessColumnFamily(ColumnFamilySet.TAXONOMY_ACTIVITY_DATACUBE.getColumnFamily())).withCql(SELECT_TAXONOMY_ACTIVITY_DATACUBE)
					.asPreparedStatement()
					.withStringValue(rowKey)
					.withStringValue(columnKey)
					.execute().getResult().getRows();
			;
		} catch (Exception e) {
			LOG.error("Error while retreving students class activity", e);
		}
		return result;
	}
	public long getContentTaxonomyActivityScore(String rowKey){
		Rows<String, String> result = null;
		long questionCount = 0L;
		long score = 0L;
		try {
			result = getKeyspace().prepareQuery(accessColumnFamily(ColumnFamilySet.TAXONOMY_ACTIVITY_DATACUBE.getColumnFamily())).withCql(SELECT_TAXONOMY_ACTIVITY_DATACUBE)
					.asPreparedStatement()
					.withStringValue(rowKey)
					.execute().getResult().getRows();
			;
			if (result.size() > 0) {
				for (Row<String, String> row : result) {
					ColumnList<String> columns = row.getColumns();
							questionCount++;
							score += columns.getLongValue(SCORE, 0L);
				}
				score = questionCount > 0 ? (score/questionCount) : 0;
			}
		} catch (Exception e) {
			LOG.error("Error while retreving students class activity", e);
		}
		return score;
	}
	
	public boolean saveTaxonomyActivityDataCube(TaxonomyActivityDataCube taxonomyActivityDataCube) {
		try {						
			getKeyspace().prepareQuery(accessColumnFamily(ColumnFamilySet.TAXONOMY_ACTIVITY_DATACUBE.getColumnFamily()))
			.withCql(INSERT_TAXONOMY_ACTIVITY_DATACUBE)
			.asPreparedStatement()
			.withStringValue(taxonomyActivityDataCube.getRowKey())
			.withStringValue(taxonomyActivityDataCube.getLeafNode())
			.withLongValue(taxonomyActivityDataCube.getViews())
			.withLongValue(taxonomyActivityDataCube.getAttempts())
			.withLongValue(taxonomyActivityDataCube.getResourceTimespent())
			.withLongValue(taxonomyActivityDataCube.getQuestionTimespent())
			.withLongValue(taxonomyActivityDataCube.getScore())
			.execute()
			;
		} catch (ConnectionException e) {
			LOG.error("Error while storing question grade" ,e);
			return false;
		}
		return true;
	}
	
	public boolean saveQuestionGrade(String teacherId, String userId, String sessionId, String questionId, long score) {
		try {			
			getKeyspace().prepareQuery(accessColumnFamily(ColumnFamilySet.USER_QUESTION_GRADE.getColumnFamily()))
			.withCql(INSERT_USER_QUESTION_GRADE)
			.asPreparedStatement()
			.withStringValue(teacherId)
			.withStringValue(userId)
			.withStringValue(sessionId)
			.withStringValue(questionId)
			.withLongValue(score)
			.execute()
			;
		} catch (ConnectionException e) {
			LOG.error("Error while storing question grade" ,e);
			return false;
		}
		return true;
	}

	public Rows<String, String> getQuestionsGradeBySessionId(String teacherId, String userId, String sessionId) {
		Rows<String, String> result = null;
		try {
			result = getKeyspace().prepareQuery(accessColumnFamily(ColumnFamilySet.USER_QUESTION_GRADE.getColumnFamily())).withCql(SELECT_CONTENT_TAXONOMY_ACTIVITY).asPreparedStatement()
					.withStringValue(teacherId).withStringValue(userId).withStringValue(sessionId).execute().getResult().getRows();
		} catch (Exception e) {
			LOG.error("Exception while read questions grade by session", e);
		}
		return result;
	}
	
	public Rows<String, String> getQuestionsGradeByQuestionId(String teacherId, String userId, String sessionId, String questionId) {
		Rows<String, String> result = null;
		try {
			result = getKeyspace().prepareQuery(accessColumnFamily(ColumnFamilySet.USER_QUESTION_GRADE.getColumnFamily())).withCql(SELECT_CONTENT_TAXONOMY_ACTIVITY).asPreparedStatement()
					.withStringValue(teacherId).withStringValue(userId).withStringValue(sessionId).withStringValue(questionId).execute().getResult().getRows();
		} catch (Exception e) {
			LOG.error("Exception while read questions grade by session", e);
		}
		return result;
	}
	
	public boolean saveQuestionGradeInSession(String sessionId, String questionId, String collectionItemId, long score) {
		try {			
			getKeyspace().prepareQuery(accessColumnFamily(ColumnFamilySet.USER_SESSION_ACTIVITY.getColumnFamily()))
			.withCql(UPDATE_SESSION_SCORE)
			.asPreparedStatement()
			.withStringValue(sessionId)
			.withStringValue(questionId)
			.withStringValue(collectionItemId)
			.withLongValue(score)
			.execute()
			;
		} catch (ConnectionException e) {
			LOG.error("Error while storing question grade in session" ,e);
			return false;
		}
		return true;
	}
}
