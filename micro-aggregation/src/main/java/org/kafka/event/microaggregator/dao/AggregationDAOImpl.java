package org.kafka.event.microaggregator.dao;

import java.nio.file.AccessDeniedException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.collections.TreeBag;
import org.kafka.event.microaggregator.core.CassandraConnectionProvider;
import org.kafka.event.microaggregator.core.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.netflix.astyanax.ExceptionCallback;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.AllRowsQuery;
import com.netflix.astyanax.query.IndexQuery;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.query.RowSliceQuery;
import com.netflix.astyanax.serializers.StringSerializer;

import de.congrace.exp4j.ExpressionBuilder;

public class AggregationDAOImpl extends BaseDAOCassandraImpl implements AggregationDAO, Constants {

	SimpleDateFormat format = new SimpleDateFormat(EVENT_TIMELINE_KEY_FORMAT);
	Gson gson = new Gson();

	public static final Logger logger = LoggerFactory.getLogger(AggregationDAOImpl.class);

	public AggregationDAOImpl(CassandraConnectionProvider connectionProvider) {
		super(connectionProvider);
	}

	public ColumnFamily<String, String> getColumnFamily(String columnFamilyName) {

		return new ColumnFamily<String, String>(columnFamilyName, StringSerializer.get(), StringSerializer.get());

	}

	public void startStaticAggregation(String startTime, String endTime) {

		handleAggregation(startTime, endTime);

	}

	/*
	 * @param startTime has the starting Date in the format of yyyyMMDDkkmm
	 * 
	 * @param endTime has the ending Date in the format of yyyyMMDDkkmm
	 */
	public void handleAggregation(String startTime, String endTime) {
		try {

			String lastProcessedKey = null;
			List<String> keys = new ArrayList<String>();
			List<String> column = new ArrayList<String>();
			column.add(ITEM_VALUE);
			column.add(LAST_PROCESSED_TIME);
			OperationResult<ColumnList<String>> configData = this.readRow(columnFamily.JOB_CONFIG_SETTING.columnFamily(), MINUTE_AGGREGATOR_PROCESSOR_KEY, column);
			column = new ArrayList<String>();
			column.add(ITEM_VALUE);
			List<String> prcessingKey = this.listRowColumnStringValue(configData, column);

			if (checkNull(startTime) && checkNull(endTime)) {
				try {
					Date startDate = format.parse(startTime);
					Calendar calender = Calendar.getInstance();
					calender.setTime(startDate);
					do {
						keys.add(startTime);
						calender.add(calender.MINUTE, 1);
						Date d = calender.getTime();
						startTime = format.format(d);
					} while (!startTime.equalsIgnoreCase(endTime));
					keys.add(endTime);
				} catch (ParseException e) {
					e.printStackTrace();
				}
			} else if (checkNull(startTime) && !checkNull(endTime)) {
				keys.add(startTime);
			} else if (!checkNull(startTime) && checkNull(endTime)) {
				try {
					startTime = endTime;
					Date startDate;
					startDate = format.parse(endTime);
					Calendar calender = Calendar.getInstance();
					calender.setTime(startDate);
					calender.add(calender.MINUTE, -1);
					Date endDate = calender.getTime();
					endTime = format.format(endDate);
					keys.add(endTime);
					keys.add(startTime);
				} catch (ParseException e) {
					e.printStackTrace();
				}
			} else {
				column = new ArrayList<String>();
				column.add(LAST_PROCESSED_TIME);
				List<String> processedTime = this.listRowColumnStringValue(configData, column);
				if (checkNull(processedTime)) {
					Date startDate;
					Date endDate;
					try {
						startDate = format.parse(processedTime.get(0));
						Calendar calender = Calendar.getInstance();
						endDate = calender.getTime();
						endTime = format.format(endDate);
						calender.setTime(startDate);
						do {
							calender.add(calender.MINUTE, 1);
							Date d = calender.getTime();
							startTime = format.format(d);
							keys.add(startTime);
						} while (!startTime.equalsIgnoreCase(endTime));
					} catch (ParseException e) {
						e.printStackTrace();
					}
				} else {
					Calendar calender = Calendar.getInstance();
					endTime = format.format(calender.getTime());
					calender.add(calender.MINUTE, -1);
					Date startDate = calender.getTime();
					startTime = format.format(startDate);
					keys.add(startTime);
					keys.add(endTime);
				}
			}
			List<Map<String, String>> rowData = new ArrayList<Map<String,String>>();
			// get Event Ids for every minute
			rowData = this.sortList(this.getRowsKeyColumnStringValue(this.readRows(columnFamily.EVENT_TIMELINE.columnFamily(), keys, new ArrayList<String>())),
					mapKey.KEY.mapKey(), sortType.ASC.sortType());

			// iterate for every minute
			for (Map<String, String> fetchedkey : rowData) {
				column = new ArrayList<String>();
				column.add(EVENT_NAME);
				column.add(FIELDS);

				// get raw data for processing
				List<Map<String, String>> eventData = this.getRowsColumnStringValue(
						this.readRows(columnFamily.EVENT_DETAIL.columnFamily(), convertArraytoList(String.valueOf(fetchedkey.get(mapKey.VALUE.mapKey())).split(COMMA)), column),
						new ArrayList<String>());

				// get normal Formula
				column = new ArrayList<String>();
				column.add(FORMULA);
				Map<String, Object> whereCondition = new HashMap<String, Object>();
				whereCondition.put(aggregateType.KEY.aggregateType(), aggregateType.NORMAL.aggregateType());
				List<Map<String, Object>> normalFormulaDetails = this.getRowsKeyStringValue(this.readWithIndex(columnFamily.FORMULA_DETAIL.columnFamily(), whereCondition, column),
						new ArrayList<String>());

				Set<String> dashboardKeys = listRowColumnName(readRows(columnFamily.JOB_CONFIG_SETTING.columnFamily(),convertStringtoList(convertListtoString(prcessingKey)), new ArrayList<String>()));

				dashboardKeys = formOrginalKey(dashboardKeys, eventData, format, fetchedkey.get(mapKey.KEY.mapKey()));
				if(!checkNull(dashboardKeys)){
					break;
				}
				// update live dashboard
				List<Map<String, String>> dashboardData = this.getRowsKeyLongValue(this.readRows(columnFamily.LIVE_DASHBOARD.columnFamily(),dashboardKeys, new ArrayList<String>()),
						new ArrayList<String>());

				// insert data for normal aggregation
				for (Map<String, String> countMap : dashboardData) {
					Map<String, Long> resultMap = new HashMap<String, Long>();
					for (Map<String, Object> formulaMap : normalFormulaDetails) {
						Map<String, Object> formulaDetail = new HashMap<String, Object>();
						JsonElement jsonElement = new JsonParser().parse(formulaMap.get(FORMULA).toString());
						JsonObject jsonObject = jsonElement.getAsJsonObject();
						formulaDetail = gson.fromJson(jsonObject, formulaDetail.getClass());

						resultMap = calculation(countMap, formulaDetail,fetchedkey.get(mapKey.KEY.mapKey()),eventData);
					}
					if (!checkNull(resultMap)) {
						continue;
					}

					// delete existing column since it was an dynamic column
					Set<String> columnNames = resultMap.keySet();
					deleteColumns(columnFamily.LIVE_DASHBOARD.columnFamily(), countMap.get(mapKey.KEY.mapKey()).toString(), columnNames);

					// Increment the counter column
					incrementCounterValue(columnFamily.LIVE_DASHBOARD.columnFamily(), countMap.get(mapKey.KEY.mapKey()).toString(), resultMap);
				}
				lastProcessedKey = fetchedkey.get(mapKey.KEY.mapKey());
				logger.info("processed key " + lastProcessedKey);
			}
			Map<String, String> data = new HashMap<String, String>();
			data.put(LAST_PROCESSED_TIME, lastProcessedKey);
			putStringValue(columnFamily.JOB_CONFIG_SETTING.columnFamily(), MINUTE_AGGREGATOR_PROCESSOR_KEY, data);
			logger.info("Minute Aggregator Runned Successfully");
		} catch (Exception e) {
			logger.error("Minute Runner failed due to " + e);
		}
	}

	/*
	 * @param eventData the JSON data to substitute
	 * 
	 * @param tempKey the format of key where the value is got replaced
	 */
	public Set<String> substituteKeyVariable(List<Map<String, String>> eventData, String tempKey) {

		Set<String> formedKeys= new TreeSet();
		for (Map<String, String> map : eventData) {
			String key = null;

			// Avoid empty data
			if (!checkNull(map.get(EVENT_NAME)) || !checkNull(map.get(FIELDS))) {
				continue;
			}

			Map<String, JsonElement> rawData = new HashMap<String, JsonElement>();
			String eventName = map.get(EVENT_NAME);
			JsonElement jsonElement = new JsonParser().parse(map.get(FIELDS));
			JsonObject jsonObject = jsonElement.getAsJsonObject();
			Map<String, String> rawMap = new HashMap<String, String>();
			try {
				rawMap = gson.fromJson(jsonObject, rawMap.getClass());
			} catch (Exception e) {
				logger.debug("fields is not an json Element");
				continue;
			}
			Set<String> keyData = rawMap.keySet();
			key = replaceKey(tempKey, rawMap, keyData);
			Map<String, String> contextMap = new HashMap<String, String>();
			try {
				jsonElement = new JsonParser().parse(rawMap.get(eventJSON.CONTEXT.eventJSON()));
				jsonObject = jsonElement.getAsJsonObject();
				contextMap = gson.fromJson(jsonObject, contextMap.getClass());
				contextMap.put(ORGANIZATIONUID, contextMap.get(ORGANIZATIONUID) != null && contextMap.get(ORGANIZATIONUID) != "" ? contextMap.get(ORGANIZATIONUID) : DEFAULT_ORGANIZATION_UID);
			} catch (Exception e) {
				logger.debug("Context is not an json Element");
				continue;
			}
			keyData = contextMap.keySet();
			key = replaceKey(key, contextMap, keyData);
			Map<String, String> sessionMap = new HashMap<String, String>();
			try {
				jsonElement = new JsonParser().parse(rawMap.get(eventJSON.SESSION.eventJSON()));
				jsonObject = jsonElement.getAsJsonObject();
				sessionMap = gson.fromJson(jsonObject, sessionMap.getClass());
			} catch (Exception e) {
				logger.debug("session is not an json Element");
				continue;
			}
			keyData = sessionMap.keySet();
			key = replaceKey(key, sessionMap, keyData);
			Map<String, String> userMap = new HashMap<String, String>();
			try {
				jsonElement = new JsonParser().parse(rawMap.get(eventJSON.USER.eventJSON()));
				jsonObject = jsonElement.getAsJsonObject();
				userMap = gson.fromJson(jsonObject, userMap.getClass());
			} catch (Exception e) {
				logger.debug("user is not an json Element");
				continue;
			}
			keyData = userMap.keySet();
			key = replaceKey(key, userMap, keyData);
			Map<String, String> payLoadMap = new HashMap<String, String>();
			try {
				jsonElement = new JsonParser().parse(rawMap.get(eventJSON.PAYLOADOBJECT.eventJSON()));
				jsonObject = jsonElement.getAsJsonObject();
				payLoadMap = gson.fromJson(jsonObject, payLoadMap.getClass());
			} catch (Exception e) {
				logger.debug("payLoadObject is not an json Element");
				continue;
			}
			keyData = payLoadMap.keySet();
			key = replaceKey(key, payLoadMap, keyData);
			formedKeys.addAll(convertStringtoSet(key));
			}
		return formedKeys;
	}

	/*
	 * @param columnFamilyName is name of columnFamily
	 * 
	 * @param rowKey is collection of keys
	 * 
	 * @param columnList will list column needs to be fetched
	 */
	public OperationResult<Rows<String, String>> readRows(String columnFamilyName, Collection<String> rowKey, Collection<String> columnList) {

		OperationResult<Rows<String, String>> result = null;
		RowSliceQuery<String, String> rowsResult = null;
		try {
			rowsResult = getKeyspace().prepareQuery(this.getColumnFamily(columnFamilyName)).getKeySlice(rowKey);
			if (checkNull(columnList)) {
				rowsResult.withColumnSlice(columnList);
			}
			result = rowsResult.execute();
		} catch (ConnectionException e) {
			logger.error("Exception while getting rows data of " + columnFamilyName);
			e.printStackTrace();
		}
		return result;
	}

	/*
	 * @param columnFamilyName is name of columnFamily
	 * 
	 * @param rowKey is value of key
	 * 
	 * @param columnList will list column needs to be fetched
	 */
	public OperationResult<ColumnList<String>> readRow(String columnFamilyName, String rowKey, Collection<String> columnList) {

		OperationResult<ColumnList<String>> result = null;
		RowQuery<String, String> rowsResult = null;
		try {
			rowsResult = getKeyspace().prepareQuery(this.getColumnFamily(columnFamilyName)).getKey(rowKey);

			if (checkNull(columnList)) {
				rowsResult.withColumnSlice(columnList);
			}
			result = rowsResult.execute();
		} catch (ConnectionException e) {
			logger.error("Exception while getting row data of " + columnFamilyName);
			e.printStackTrace();
		}
		return result;
	}

	/*
	 * @param columnFamilyName is name of columnFamily
	 * 
	 * @param rowKey is value of key
	 * 
	 * @param request contains data needs to be incremented in counter
	 */
	public void incrementCounterValue(String columnFamilyName, String rowKey, Map<String, Long> request) {

		if (checkNull(rowKey) && checkNull(request)) {
			MutationBatch mutationBatch = this.getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
			for (Map.Entry<String, Long> entry : request.entrySet()) {
				mutationBatch.withRow(this.getColumnFamily(columnFamilyName), rowKey).incrementCounterColumn(entry.getKey(), entry.getValue());
			}
			try {
				mutationBatch.execute();
			} catch (ConnectionException e) {
				logger.error("Exception while increment the counter in " + columnFamilyName);
				System.out.println(request + "" + e);
				e.printStackTrace();
			}
		}
	}

	/*
	 * @param columnFamilyName is name of columnFamily
	 * 
	 * @param rowKey is value of key
	 * 
	 * @param request contains data need to be inserted
	 */
	public void putLongValue(String columnFamilyName, String rowKey, Map<String, Long> request) {

		if (checkNull(rowKey) && checkNull(request)) {
			MutationBatch mutationBatch = this.getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
			for (Map.Entry<String, Long> entry : request.entrySet()) {
				mutationBatch.withRow(this.getColumnFamily(columnFamilyName), rowKey).putColumnIfNotNull(entry.getKey(), entry.getValue());
			}
			try {
				mutationBatch.execute();

			} catch (ConnectionException e) {
				logger.error("Exception while inserting the Long value in " + columnFamilyName);
				e.printStackTrace();
			}
		}
	}

	/*
	 * @param columnFamilyName is name of columnFamily
	 * 
	 * @param rowKey is value of key
	 * 
	 * @param columnList has the list of columns
	 */
	public OperationResult<ColumnList<String>> readColumns(String columnFamilyName, String rowKey, Collection<String> columnList) {

		OperationResult<ColumnList<String>> result = null;
		RowQuery<String, String> rowsResult = null;
		try {
			rowsResult = getKeyspace().prepareQuery(this.getColumnFamily(columnFamilyName)).getKey(rowKey);
			if (checkNull(columnList)) {
				rowsResult.withColumnSlice(columnList);
			}
			result = rowsResult.execute();
		} catch (ConnectionException e) {
			logger.error("Exception while getting column data of " + columnFamilyName);
			e.printStackTrace();
		}
		return result;
	}

	/*
	 * @param columnFamilyName is name of columnFamily
	 * 
	 * @param rowKey is value of key
	 * 
	 * @param columns has the set of columns needs to be deleted
	 */
	public boolean deleteColumns(String columnFamilyName, String rowKey, Set<String> columns) {

		MutationBatch mutationBatch = getKeyspace().prepareMutationBatch();
		try {
			for (String columnName : columns) {
				mutationBatch.withRow(getColumnFamily(columnFamilyName), rowKey).deleteColumn(columnName);
			}
			mutationBatch.execute();
			return true;
		} catch (ConnectionException e) {
			logger.error("Exception while deleting the column data of (columnFamily~rowKey) " + columnFamilyName + SEPERATOR + rowKey);
			e.printStackTrace();
			return false;
		}
	}

	/*
	 * @param columnFamilyName is name of columnFamily
	 * 
	 * @param rowKey is value of key
	 * 
	 * @param column has the columnName to check of its occurrences
	 */
	public boolean isColumnExists(String columnFamilyName, String rowKey, String column) {

		try {
			OperationResult<Column<String>> result = getKeyspace().prepareQuery(this.getColumnFamily(columnFamilyName)).getKey(rowKey).getColumn(column).execute();
			return checkNull(result);

		} catch (ConnectionException e) {
			logger.error("Exception while checking the column data exists of (columnFamily~rowKey) " + columnFamilyName + SEPERATOR + rowKey);
			e.printStackTrace();
		}
		return false;
	}

	/*
	 * @param columnFamilyName is name of columnFamily
	 * 
	 * @param column has the list of columnName
	 */
	public OperationResult<Rows<String, String>> readAllRows(String columnFamilyName, List<String> column) {

		Map<String, Object> resultMap = new HashMap<String, Object>();
		OperationResult<Rows<String, String>> result = null;
		AllRowsQuery<String, String> rowsResult = null;
		try {
			rowsResult = getKeyspace().prepareQuery(this.getColumnFamily(columnFamilyName)).getAllRows().setExceptionCallback(new ExceptionCallback() {

				@Override
				public boolean onException(ConnectionException arg0) {

					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {

					}
					return true;
				}
			});

			if (checkNull(column)) {
				rowsResult.withColumnSlice(column);
			}

			result = rowsResult.execute();
		} catch (ConnectionException e) {
			logger.error("Exception while Reading all the data of " + columnFamilyName);
			e.printStackTrace();
		}
		return result;
	}

	/*
	 * @param columnFamilyName is name of columnFamily
	 * 
	 * @param whereColumns contains the column value to be filtered
	 * 
	 * @param column has the list of columnName
	 */
	public OperationResult<Rows<String, String>> readWithIndex(String columnFamilyName, Map<String, Object> whereColumns, List<String> column) {

		Map<String, Object> resultMap = new HashMap<String, Object>();
		OperationResult<Rows<String, String>> result = null;
		IndexQuery<String, String> rowsResult = null;
		try {
			rowsResult = getKeyspace().prepareQuery(this.getColumnFamily(columnFamilyName)).searchWithIndex();

			if (checkNull(whereColumns)) {
				for (Map.Entry<String, Object> whereColumn : whereColumns.entrySet())
					rowsResult.addExpression().whereColumn(whereColumn.getKey()).equals().value(String.valueOf(whereColumn.getValue()));
			}
			rowsResult.autoPaginateRows(true);
			if (checkNull(column)) {
				rowsResult.withColumnSlice(column);
			}

			result = rowsResult.execute();
		} catch (ConnectionException e) {
			logger.error("Exception while reading the column with Index of(columnFamily~ColumnName) " + columnFamilyName + SEPERATOR + whereColumns.keySet());
			e.printStackTrace();
		}
		return result;
	}

	/*
	 * @param column is name of column name
	 * 
	 * @param counter is the value to be parsed in counter
	 */
	public Map<String, Long> defaultCounterValue(Collection<String> column, Long counter) {
		Map<String, Long> resultSet = new HashMap<String, Long>();
		for (String columnName : column) {
			resultSet.put(columnName, counter);
		}
		return resultSet;
	}

	/*
	 * @param columnFamilyName is name of columnFamily
	 * 
	 * @param rowKey is value of key
	 * 
	 * @param request contains column and its string value to be inserted
	 */
	public void putStringValue(String columnFamilyName, String rowKey, Map<String, String> request) {
		if (checkNull(rowKey) && checkNull(request)) {
			MutationBatch mutationBatch = this.getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
			for (Map.Entry<String, String> entry : request.entrySet()) {
				mutationBatch.withRow(this.getColumnFamily(columnFamilyName), rowKey).putColumnIfNotNull(entry.getKey(), entry.getValue());
			}
			try {
				mutationBatch.execute();
			} catch (ConnectionException e) {
				logger.error("Exception while inserting the string value to (columnFamily~rowKey) " + columnFamilyName + SEPERATOR + rowKey);
				e.printStackTrace();
			}
		}
	}

	/*
	 * @param result is the result of Cassandra Row query with list of columns
	 * 
	 * @param result is the list of columnNames needs to be filtered
	 */
	public List<String> listRowColumnStringValue(OperationResult<ColumnList<String>> result, List<String> columnNames) {

		List<String> resultList = new ArrayList<String>();
		if (checkNull(columnNames)) {
			for (Column<String> column : result.getResult()) {
				for (String columnName : columnNames) {
					if (columnName.equalsIgnoreCase(column.getName()))
						resultList.add(column.getStringValue());
					break;
				}
			}
		} else {
			for (Column<String> column : result.getResult()) {
				resultList.add(column.getStringValue());
			}
		}
		return resultList;
	}

	/*
	 * @param result is the result of Cassandra Rows query with list of rows
	 */
	public List<String> listRowsStringValue(OperationResult<Rows<String, String>> result) {

		List<String> resultList = new ArrayList<String>();
		for (Row<String, String> row : result.getResult()) {
			for (Column<String> column : row.getColumns()) {
				resultList.add(column.getStringValue());
			}
		}
		return resultList;
	}

	/*
	 * @param result is the result of Cassandra Rows query with list of rows
	 */
	public Set<String> listRowColumnName(OperationResult<Rows<String, String>> result) {

		Set<String> resultList = new TreeSet<String>();
		for (Row<String, String> row : result.getResult()) {
			for(Column<String> column : row.getColumns()){
				resultList.add(column.getName());
			}
		}
		return resultList;
	}

	/*
	 * @param result is the result of Cassandra Rows query with list of rows
	 * 
	 * @param columName is the specific column name needs to be filtered
	 */
	public List<String> listRowsColumnStringValue(OperationResult<Rows<String, String>> result, String columName) {

		List<String> resultList = new ArrayList<String>();
		if (checkNull(columName)) {
			for (Row<String, String> row : result.getResult()) {
				for (Column<String> column : row.getColumns()) {
					if (columName.equalsIgnoreCase(column.getName()))
						resultList.add(column.getStringValue());
					break;
				}
			}
		} else {
			for (Row<String, String> row : result.getResult()) {
				for (Column<String> column : row.getColumns()) {
					resultList.add(column.getStringValue());
				}
			}
		}
		return resultList;
	}

	/*
	 * @param result is the result of Cassandra Row query with list of columns
	 */
	public Map<String, String> getRowStringValue(OperationResult<ColumnList<String>> result) {

		Map<String, String> resultList = new HashMap<String, String>();
		for (Column<String> column : result.getResult()) {
			resultList.put(column.getName(), column.getStringValue());
		}
		return resultList;
	}

	/*
	 * @param result is the result of Cassandra Rows query with list of rows
	 */
	public List<Map<String, String>> getRowsKeyColumnStringValue(OperationResult<Rows<String, String>> result) {

		List<Map<String, String>> data = new ArrayList<Map<String, String>>();
		for (Row<String, String> row : result.getResult()) {
			Map<String, String> map = new HashMap<String, String>();
			StringBuffer columnValue = new StringBuffer();
			boolean hasData = false;
			for (Column<String> column : row.getColumns()) {
				hasData = true;
				if (checkNull(columnValue.toString())) {
					columnValue.append(COMMA + column.getStringValue());
				} else {
					columnValue.append(column.getStringValue());
				}

			}
			if (hasData) {
				map.put(mapKey.KEY.mapKey(), row.getKey());
				map.put(mapKey.VALUE.mapKey(), columnValue.toString());
				data.add(map);
			}
		}
		return data;
	}

	/*
	 * @param result is the result of Cassandra Rows query with list of rows
	 * 
	 * @param columNames is the list of columns needs to be filtered
	 */
	public List<Map<String, String>> getRowsKeyLongValue(OperationResult<Rows<String, String>> result, List<String> columnNames) {

		List<Map<String, String>> resultList = new ArrayList<Map<String, String>>();
		if (checkNull(columnNames)) {
			for (Row<String, String> row : result.getResult()) {
				Map<String, String> map = new HashMap<String, String>();
				for (Column<String> column : row.getColumns()) {
					for (String columnName : columnNames) {
						if (columnName.equalsIgnoreCase(column.getName()))
							map.put(column.getName(), String.valueOf(column.getLongValue()));
						break;
					}
				}
				map.put(mapKey.KEY.mapKey(), row.getKey());
				resultList.add(map);
			}
		} else {
			for (Row<String, String> row : result.getResult()) {
				Map<String, String> map = new HashMap<String, String>();
				for (Column<String> column : row.getColumns()) {
					map.put(column.getName(), String.valueOf((column.getLongValue())));
				}
				map.put(mapKey.KEY.mapKey(), row.getKey());
				resultList.add(map);
			}
		}
		return resultList;
	}

	/*
	 * @param result is the result of Cassandra Rows query with list of rows
	 * 
	 * @param columName is the list of columns needs to be filtered
	 */
	public List<Map<String, Object>> getRowsKeyStringValue(OperationResult<Rows<String, String>> result, List<String> columnNames) {

		List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
		if (checkNull(columnNames)) {
			for (Row<String, String> row : result.getResult()) {
				Map<String, Object> map = new HashMap<String, Object>();
				for (Column<String> column : row.getColumns()) {
					for (String columnName : columnNames) {
						if (columnName.equalsIgnoreCase(column.getName()))
							map.put(column.getName(), column.getStringValue());
						break;
					}
				}
				map.put(mapKey.KEY.mapKey(), row.getKey());
				resultList.add(map);
			}
		} else {
			for (Row<String, String> row : result.getResult()) {
				Map<String, Object> map = new HashMap<String, Object>();
				for (Column<String> column : row.getColumns()) {
					map.put(column.getName(), column.getStringValue());
				}
				map.put(mapKey.KEY.mapKey(), row.getKey());
				resultList.add(map);
			}

		}
		return resultList;
	}

	/*
	 * @param result is the result of Cassandra Rows query with list of rows
	 * 
	 * @param columName is the list of columns needs to be filtered
	 */
	public List<Map<String, String>> getRowsColumnStringValue(OperationResult<Rows<String, String>> result, List<String> columNames) {

		List<Map<String, String>> resultList = new ArrayList<Map<String, String>>();
		if (checkNull(columNames)) {
			for (Row<String, String> row : result.getResult()) {
				Map<String, String> rowMap = new HashMap();

				for (Column<String> column : row.getColumns()) {
					for (String columnName : columNames) {
						if (columnName.equalsIgnoreCase(column.getName()))
							rowMap.put(column.getName(), column.getStringValue());
						break;
					}
				}
				resultList.add(rowMap);
			}
		} else {
			for (Row<String, String> row : result.getResult()) {
				Map<String, String> rowMap = new HashMap();

				for (Column<String> column : row.getColumns()) {
					rowMap.put(column.getName(), column.getStringValue());
				}
				resultList.add(rowMap);
			}
		}
		return resultList;
	}

	/*
	 * @param result is the result of Cassandra Rows query with list of rows
	 */
	public List<Map<String, Object>> getStringValue(OperationResult<Rows<String, String>> result) {

		List<Map<String, Object>> dataList = new ArrayList<Map<String, Object>>();
		for (Row<String, String> row : result.getResult()) {
			Map<String, Object> resultMap = new HashMap<String, Object>();
			for (Column<String> column : row.getColumns()) {
				resultMap.put(column.getStringValue(), column.getStringValue());
			}
			dataList.add(resultMap);
		}
		return dataList;
	}

	/*
	 * @param result is the result of Cassandra Row query with list of columns
	 */
	public Map<String, Object> getLongValue(OperationResult<ColumnList<String>> result) {

		Map<String, Object> resultMap = new HashMap<String, Object>();
		for (Column<String> column : result.getResult()) {
			resultMap.put(column.getStringValue(), column.getLongValue());
		}
		return resultMap;
	}

	/*
	 * @param result is the result of Cassandra Row query with list of columns
	 * 
	 * @param columnType is the pre declaration of column Type for the column
	 */
	public Map<String, Object> getObjectvalue(OperationResult<ColumnList<String>> result, Map<String, Object> columnType) {

		Map<String, Object> resultMap = new HashMap<String, Object>();
		for (Column<String> column : result.getResult()) {
			for (Map.Entry<String, Object> entry : columnType.entrySet()) {
				if (entry.getKey().equalsIgnoreCase(column.getStringValue())) {
					if (entry.getValue() instanceof String) {
						resultMap.put(column.getName(), column.getStringValue());
					} else if (entry.getValue() instanceof Integer) {
						resultMap.put(column.getName(), column.getIntegerValue());
					} else if (entry.getValue() instanceof Long) {
						resultMap.put(column.getName(), column.getLongValue());
					} else if (entry.getValue() instanceof Date) {
						resultMap.put(column.getName(), column.getDateValue());
					}
					break;
				}
			}
		}
		return resultMap;
	}

	/*
	 * @param data is to check for empty
	 */
	public static boolean checkNull(String data) {

		if (data != null && !data.isEmpty()) {
			return true;
		}
		return false;
	}

	/*
	 * @param data is to check for empty
	 */
	public static boolean checkNull(OperationResult<?> data) {
		if (data != null) {
			return true;
		}
		return false;
	}

	/*
	 * @param data is to check for empty
	 */
	public boolean checkNull(Map<?, ?> data) {

		if (data != null && !data.isEmpty()) {
			return true;
		}
		return false;
	}

	/*
	 * @param data is to check for empty
	 */
	public boolean checkNull(Collection<?> data) {

		if (data != null && !data.isEmpty()) {
			return true;
		}
		return false;
	}

	/*
	 * @param data is to convert Array to List
	 */
	public List<String> convertArraytoList(String[] data) {
		List<String> responseData = new ArrayList<String>();
		for (String entry : data) {
			if (checkNull(entry))
				responseData.add(entry);
		}
		return responseData;
	}

	/*
	 * @param data is to convert Set to String
	 */
	public String convertSettoString(Set<String> data) {

		StringBuffer stringBuffer = new StringBuffer();
		for (String entry : data) {
			if (checkNull(stringBuffer.toString())) {
				stringBuffer.append(COMMA);
			}
			stringBuffer.append(entry);
		}
		return stringBuffer.toString();
	}
	
	/*
	 * @param data is to convert Set to String
	 */
	public Collection<String> convertSettoCollection(Set<String> data) {

		Collection<String> result = new ArrayList<String>();
		for (String entry : data) {
			result.add(entry);
		}
		return result;
	}

	/*
	 * @param data is to convert String to List
	 */
	public List<String> convertStringtoList(String data) {

		List<String> result = new ArrayList<String>();
		for (String entry : data.split(COMMA)) {
			result.add(entry);
		}
		return result;
	}

	/*
	 * @param data is to convert List to String
	 */
	public String convertListtoString(List<String> data) {

		StringBuffer stringBuffer = new StringBuffer();
		for (String entry : data) {
			if (checkNull(stringBuffer.toString())) {
				stringBuffer.append(COMMA);
			}
			stringBuffer.append(entry);
		}
		return stringBuffer.toString();
	}

	/*
	 * @param data is to convert List to Set
	 */
	public Set<String> convertListtoSet(List<String> data) {

		Set<String> result = new HashSet<String>();
		for (String entry : data) {
			result.add(entry);
		}
		return result;
	}

	/*
	 * @param data is to convert String to Set
	 */
	public Set<String> convertStringtoSet(String data) {

		Set<String> result = new HashSet<String>();
		for (String entry : data.split(COMMA)) {
			result.add(entry);
		}
		return result;
	}

	/*
	 * @param request is the set of key name
	 * 
	 * @param comparator check for the given key exists
	 */
	public Set<String> validateFormula(Set<String> request, Set<String> comparator) {
		Set<String> result = new HashSet<String>();
		for (String entry1 : request) {
			for (String entry2 : comparator) {
				if (entry1.contains(entry2))
					result.add(entry2);
				break;
			}
		}
		return result;
	}

	/*
	 * @param rowKey is the given Key
	 * 
	 * @param data is the given data map
	 * 
	 * @param keyList this the map key Name to be replaced
	 */
	public String replaceKey(String rowKey, Map<String, String> data, Set<String> keyList) {
		try {
			if (checkNull(keyList)) {
				for (String key : keyList) {
					rowKey = rowKey.replaceAll(key, String.valueOf(data.get(key)));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return rowKey;
	}

	/*
	 * @param firstKey is the prefix
	 * 
	 * @param secondKey is the suffix
	 */
	public Set<String> combineTwoKey(Set<String> firstKey, Set<String> secondKey) {

		Set<String> combinedKey = new HashSet<String>();
		for (String entry1 : firstKey) {
			combinedKey.add(entry1);
			for (String entry2 : secondKey) {
				combinedKey.add(entry2);
				combinedKey.add(entry1 + SEPERATOR + entry2);
			}
		}
		return combinedKey;
	}

	/*
	 * @param currentDate is the given date
	 * 
	 * @param format is the current format
	 * 
	 * @param firstKey has the list of target format
	 */
	public Set<String> convertDateFormat(String currentDate, SimpleDateFormat format, List<String> firstKey) throws ParseException {
		Set<String> dateSet = new HashSet<String>();
		Date date = format.parse(currentDate);
		for (String key : firstKey) {
			try {
				SimpleDateFormat keyFormat = new SimpleDateFormat(key);
				String resultKey = keyFormat.format(date);
				dateSet.add(resultKey);
			} catch (Exception e) {
				dateSet.add(key);
			}
		}
		return dateSet;
	}

	/*
	 * @param data is the given date
	 * 
	 * @param sortBy has which field needs to sort
	 * 
	 * @param sortOrder has the sort order of ASC or DESC
	 */
	public List<Map<String, String>> sortList(List<Map<String, String>> data, String sortBy, String sortOrder) {
		if (checkNull(sortBy)) {
			final String name = sortBy;
			boolean ascending = false;
			boolean descending = false;
			if (checkNull(sortOrder)) {
				if (sortOrder.equalsIgnoreCase(sortType.ASC.sortType())) {
					ascending = true;
				} else if (sortOrder.equalsIgnoreCase(sortType.DESC.sortType())) {
					descending = true;
				} else {
					ascending = true;
				}
			} else {
				ascending = true;
			}
			if (ascending) {
				Collections.sort(data, new Comparator<Map<String, String>>() {
					public int compare(final Map<String, String> m1, final Map<String, String> m2) {
						if (m1.containsKey(name) && m2.containsKey(name)) {
							if (m2.containsKey(name))
								return ((String) m1.get(name).toString().toLowerCase()).compareTo((String) m2.get(name).toString().toLowerCase());
						}
						return 1;
					}
				});
			}
			if (descending) {
				Collections.sort(data, new Comparator<Map<String, String>>() {
					public int compare(final Map<String, String> m1, final Map<String, String> m2) {

						if (m2.containsKey(name)) {
							if (m1.containsKey(name)) {
									return ((String) m2.get(name).toString().toLowerCase()).compareTo((String) m1.get(name).toString().toLowerCase());
							} else {
								return 1;
							}
						} else {
							return -1;
						}
					}
				});

			}
		}
		return data;
	}

	/*
	 * @param entry is the given data
	 * 
	 * @param jsonMap is the formula Json
	 */
	public  Map<String, Long> calculation(Map<String, String> entry, Map<String, Object> jsonMap,String currentDate,List<Map<String,String>> eventData) {
		
		String eventName =jsonMap.get("events").toString();
		List<Map<String,String>> validEventData= new ArrayList<Map<String,String>>();
		for(Map<String,String> eventMap : eventData){
			if(eventMap.get(EVENT_NAME).contains(eventName)){
			validEventData.add(eventMap);
		}
		}
		Map<String, Long> resultMap = new HashMap<String, Long>();
		if(checkNull(validEventData)){
		for (Map.Entry<String, Object> jsonEntry : jsonMap.entrySet()) {
			if(jsonEntry.getKey().equalsIgnoreCase("events")){
				continue;
			}
			JsonElement jsonElement = new JsonParser().parse(jsonEntry.getValue().toString());
			JsonObject json = new JsonObject();
			json = jsonElement.getAsJsonObject();
			String[] formulas = json.get("formulas").toString().replaceAll(DOUBLE_QUOTES, EMPTY).split(COMMA);
			String name = json.get(formulaDetail.NAME.formulaDetail()) != null ? json.get(formulaDetail.NAME.formulaDetail()).toString().replaceAll("\"", "") : jsonEntry.getKey();
			try {
				Map<String, ExpressionBuilder> aggregatedMap = new HashMap<String, ExpressionBuilder>();
				for(String formula :formulas){
					ExpressionBuilder expressionBuilder = new ExpressionBuilder(json.get(formulaDetail.FORMULA.formulaDetail()).toString().replaceAll(DOUBLE_QUOTES, EMPTY));
					Map<String,String> keyMap = new HashMap<String,String>();
					if(json.has(formula)){
						jsonElement = new JsonParser().parse(json.get(formula).toString());
						json = jsonElement.getAsJsonObject();
						name = json.get(formulaDetail.NAME.formulaDetail()) != null ? json.get(formulaDetail.NAME.formulaDetail()).toString().replaceAll("\"", "") : jsonEntry.getKey();
						name = formOrginalKey(name, validEventData, format, currentDate).toString();
						String[] columnNames = json.get(formulaDetail.REQUEST_VALUES.formulaDetail()).toString().replaceAll(DOUBLE_QUOTES, EMPTY).split(COMMA);
						Map<String,String> formulaMap = new HashMap();
						for (String columnName : columnNames) {
							String keyName = json.get(columnName).toString().replaceAll(DOUBLE_QUOTES, EMPTY);
							String keyValue = json.get(keyName).toString();
							Set<String> column = formOrginalKey(keyValue, validEventData, format, currentDate);
							keyMap.put(keyName, column.toString());
						}
						for(Map.Entry<String, String> map : keyMap.entrySet()){
						expressionBuilder.withVariable(map.getKey(),entry.get(map.getValue()) != null ? Long.valueOf(entry.get(map.getValue()).toString()) : 0L);
						}
						long calculatedData = Math.round(expressionBuilder.build().calculate());
						resultMap.put(name, calculatedData);
					}
				}
				
			} catch (Exception e) {
				resultMap.put(name, 0L);
		}
		}
		}
		return resultMap;
	}
	
	/*	C: defines => Constant
	D: defines => Date format lookup
	E: defines => eventMap param lookup*/
	public Set<String> formOrginalKey(Set<String> rowKeys,List<Map<String,String>> eventData,SimpleDateFormat currentFormat,String currentDate){
		
		Set<String> formedKeys = new TreeSet<String>();
		for(String keys : rowKeys){
			String formedKey = keys.replaceAll("C:", "");
			formedKey = formedKey.replaceAll("E:","");
			if(formedKey.contains("D:")){
				Date currentDateTime;
				try {
					currentDateTime = currentFormat.parse(currentDate);
								String[] dateFormat = formedKey.split("D:");
				dateFormat = dateFormat[1].split("~");
				SimpleDateFormat customDateFormatter = new SimpleDateFormat(dateFormat[0]);
				formedKey = formedKey.replaceAll("D:"+dateFormat[0], customDateFormatter.format(currentDateTime));
				} catch (ParseException e) {
					e.printStackTrace();
					continue;
				}
				}
			formedKeys.add(formedKey);
		}
		   return substituteKeyVariable(eventData,convertSettoString(formedKeys));
    }

		
		/*	C: defines => Constant
		D: defines => Date format lookup
		E: defines => eventMap param lookup*/
		public Set<String> formOrginalKey(String rowKeys,List<Map<String,String>> eventData,SimpleDateFormat currentFormat,String currentDate){
			
				String formedKey = rowKeys.replaceAll("C:", "");
				formedKey = formedKey.replaceAll("E:","");
				if(formedKey.contains("D:")){
					Date currentDateTime;
					try {
						currentDateTime = currentFormat.parse(currentDate);
									String[] dateFormat = formedKey.split("D:");
					dateFormat = dateFormat[1].split("~");
					SimpleDateFormat customDateFormatter = new SimpleDateFormat(dateFormat[0]);
					formedKey = formedKey.replaceAll("D:"+dateFormat[0], customDateFormatter.format(currentDateTime));
					} catch (ParseException e) {}
					}
		return substituteKeyVariable(eventData,formedKey);
	}
}
