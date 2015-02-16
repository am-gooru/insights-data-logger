package org.kafka.event.microaggregator.dao;

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

import org.kafka.event.microaggregator.core.CassandraConnectionProvider;
import org.kafka.event.microaggregator.core.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
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
			Set<String> keys = new TreeSet<String>();
			List<String> column = new ArrayList<String>();

			column.add(ITEM_VALUE);
			column.add(LAST_PROCESSED_TIME);
			column.add(formulaDetail.STATUS.formulaDetail());
			OperationResult<ColumnList<String>> configData = readRow(columnFamily.JOB_CONFIG_SETTING.columnFamily(), MINUTE_AGGREGATOR_PROCESSOR_KEY, column);

			column = new ArrayList<String>();
			column.add(formulaDetail.STATUS.formulaDetail());
			List<String> status = listRowColumnStringValue(configData, column);
			if(checkNull(status)){
				if(status.get(0).equalsIgnoreCase(formulaDetail.INPROGRESS.formulaDetail()))
					return;
			}
			Map<String, String> data = new HashMap<String, String>();
			data.put(formulaDetail.STATUS.formulaDetail(), formulaDetail.INPROGRESS.formulaDetail());
			putExpireStringValue(columnFamily.JOB_CONFIG_SETTING.columnFamily(), MINUTE_AGGREGATOR_PROCESSOR_KEY, data,120);
			column = new ArrayList<String>();
			column.add(ITEM_VALUE);
			List<String> prcessingKey = listRowColumnStringValue(configData, column);

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
					logger.error("Exception:"+e);
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
					logger.error("Exception:"+e);
				}
			} else {

				column = new ArrayList<String>();
				column.add(LAST_PROCESSED_TIME);
				List<String> processedTime = listRowColumnStringValue(configData, column);

				if (checkNull(processedTime) && checkNull(processedTime.get(0))) {
					Date startDate;
					Date endDate;
					try {
						startDate = format.parse(processedTime.get(0));
						Calendar calender = Calendar.getInstance();
						endDate = calender.getTime();
						endTime = format.format(endDate);
						calender.setTime(startDate);
						do {
							keys.add(format.format(calender.getTime()));
							calender.add(calender.MINUTE, 1);
							Date d = calender.getTime();
							startTime = format.format(d);
							keys.add(startTime);
						} while (!startTime.equalsIgnoreCase(endTime));
					} catch (ParseException e) {
						logger.error("Exception:"+e);
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
			// for every minute
			for (String key : keys) {
				List<String> rowData = new ArrayList<String>();

				// get Event Ids for every minute and sort it
				rowData = listRowColumnStringValue(readRow(columnFamily.EVENT_TIMELINE.columnFamily(), key, new ArrayList<String>()), new ArrayList<String>());

				// iterate for every event id
				for (String fetchedkey : rowData) {

					// get raw data for processisng
					column = new ArrayList<String>();
					column.add(EVENT_NAME);
					column.add(FIELDS);
					Map<String, String> eventData = getRowStringValue(readRow(columnFamily.EVENT_DETAIL.columnFamily(), fetchedkey, column));

					// Avoid empty data
					if (!checkNull(eventData.get(EVENT_NAME)) || !checkNull(eventData.get(FIELDS))) {
						continue;
					}

					// get normal Formula
					column = new ArrayList<String>();
					column.add(FORMULA);
					Map<String, String> normalFormulaDetails = getRowStringValue(readRow(columnFamily.FORMULA_DETAIL.columnFamily(),eventData.get(EVENT_NAME), column));
					
					if(!checkNull(normalFormulaDetails)){
						continue;
					}
					String configKey = getValue(convertListtoString(prcessingKey),eventData.get(EVENT_NAME));
					
					if(!checkNull(configKey)){
						continue;
					}
					
					Set<String> dashboardKeys = getRowStringValue(readRow(columnFamily.JOB_CONFIG_SETTING.columnFamily(),configKey,
							new ArrayList<String>())).keySet();

					dashboardKeys = formOrginalKey(dashboardKeys, eventData, format, key);
					if (!checkNull(dashboardKeys)) {
						continue;
					}
					// update live dashboard
					List<Map<String, String>> dashboardData = getRowsKeyLongValue(readRows(columnFamily.LIVE_DASHBOARD.columnFamily(), dashboardKeys, new ArrayList<String>()), new ArrayList<String>());

					// insert data for normal aggregation
					for (Map<String, String> countMap : dashboardData) {
						Map<String, Long> resultMap = new HashMap<String, Long>();
							try {
								JsonElement jsonElement = new JsonParser().parse(normalFormulaDetails.get(FORMULA).toString());
								JsonObject jsonObject = jsonElement.getAsJsonObject();
								resultMap = calculation(countMap, jsonObject, key, eventData);
								Map<String,Long> subEventMap = processSubEvents(countMap, jsonObject, eventData.get(EVENT_NAME));
							if(checkNull(subEventMap)){
								resultMap.putAll(subEventMap);
							}
							} catch (Exception e) {
								logger.error("unable to get formula" + e);
							}
						if (!checkNull(resultMap)) {
							continue;
						}
						// delete existing column since it was an dynamic column
						Set<String> columnNames = resultMap.keySet();
						decrementCounterValue(columnFamily.LIVE_DASHBOARD.columnFamily(),countMap.get(mapKey.KEY.mapKey()).toString(),getLongValue(readRow(columnFamily.LIVE_DASHBOARD.columnFamily(), countMap.get(mapKey.KEY.mapKey()).toString(), convertSettoList(columnNames))));
						//deleteColumns(columnFamily.LIVE_DASHBOARD.columnFamily(), countMap.get(mapKey.KEY.mapKey()).toString(), columnNames);

						// Increment the counter column
						incrementCounterValue(columnFamily.LIVE_DASHBOARD.columnFamily(), countMap.get(mapKey.KEY.mapKey()).toString(), resultMap);
					}
					lastProcessedKey = key;
					logger.info("processed key " + lastProcessedKey);
				}
			}
			if(!checkNull(lastProcessedKey)){
				lastProcessedKey = startTime;
			}
			data = new HashMap<String, String>();
			data.put(LAST_PROCESSED_TIME, lastProcessedKey);
			putStringValue(columnFamily.JOB_CONFIG_SETTING.columnFamily(), MINUTE_AGGREGATOR_PROCESSOR_KEY, data);
			data = new HashMap<String, String>();
			data.put( formulaDetail.STATUS.formulaDetail(), formulaDetail.COMPLETED.formulaDetail());
			putExpireStringValue(columnFamily.JOB_CONFIG_SETTING.columnFamily(), MINUTE_AGGREGATOR_PROCESSOR_KEY, data,120);
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
	public Set<String> substituteKeyVariable(Map<String, String> eventData, String tempKey) {

		Set<String> formedKeys = new TreeSet();
		String key = null;

		Map<String, JsonElement> rawData = new HashMap<String, JsonElement>();
		String eventName = eventData.get(EVENT_NAME);
		JsonElement jsonElement = new JsonParser().parse(eventData.get(FIELDS));
		JsonObject jsonObject = jsonElement.getAsJsonObject();
		Map<String, String> rawMap = new HashMap<String, String>();
		try {
			rawMap = gson.fromJson(jsonObject, rawMap.getClass());
		} catch (Exception e) {
			logger.debug("fields is not an json Element");
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
		}
		keyData = payLoadMap.keySet();
		key = replaceKey(key, payLoadMap, keyData);
		formedKeys.addAll(convertStringtoSet(key));
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
			rowsResult = getKeyspace().prepareQuery(getColumnFamily(columnFamilyName)).getKeySlice(rowKey);
			if (checkNull(columnList)) {
				rowsResult.withColumnSlice(columnList);
			}
			result = rowsResult.execute();
		} catch (ConnectionException e) {
			logger.error("Exception while getting rows data of " + columnFamilyName);
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
	public OperationResult<ColumnList<String>> readRow(String columnFamilyName, String rowKey, List<String> columnList) {

		OperationResult<ColumnList<String>> result = null;
		RowQuery<String, String> rowsResult = null;
		try {
			rowsResult = getKeyspace().prepareQuery(getColumnFamily(columnFamilyName)).getKey(rowKey);

			if (checkNull(columnList)) {
				rowsResult.withColumnSlice(columnList);
			}
			result = rowsResult.execute();
		} catch (ConnectionException e) {
			logger.error("Exception while getting row data of " + columnFamilyName);
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
			MutationBatch mutationBatch = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
				try {
					for (Map.Entry<String, Long> entry : request.entrySet()) {
					mutationBatch.withRow(getColumnFamily(columnFamilyName), rowKey).incrementCounterColumn(entry.getKey(), entry.getValue());
					}
					mutationBatch.execute();
				} catch (ConnectionException e) {
					logger.error("Exception while increment the counter in " + columnFamilyName);
				}
		}
	}
	
	/*
	 * @param columnFamilyName is name of columnFamily
	 * 
	 * @param rowKey is value of key
	 * 
	 * @param request contains data needs to be incremented in counter
	 */
	public void decrementCounterValue(String columnFamilyName, String rowKey, Map<String, Long> request) {

		if (checkNull(rowKey) && checkNull(request)) {
			MutationBatch mutationBatch = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
				try {
					for (Map.Entry<String, Long> entry : request.entrySet()) {
					mutationBatch.withRow(getColumnFamily(columnFamilyName), rowKey).incrementCounterColumn(entry.getKey(), -entry.getValue());
					}
					mutationBatch.execute();
				} catch (ConnectionException e) {
					logger.error("Exception while increment the counter in " + columnFamilyName);
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
			try {
				MutationBatch mutationBatch = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
				for (Map.Entry<String, Long> entry : request.entrySet()) {
					mutationBatch.withRow(getColumnFamily(columnFamilyName), rowKey).putColumnIfNotNull(entry.getKey(), entry.getValue());
				}
				mutationBatch.execute();

			} catch (ConnectionException e) {
				logger.error("Exception while inserting the Long value in " + columnFamilyName);
			}
		}
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
			} catch (ConnectionException e) {
				logger.error("Exception while deleting the column data of (columnFamily~rowKey) " + columnFamilyName + SEPERATOR + rowKey);
		}
		return true;
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
			OperationResult<Column<String>> result = getKeyspace().prepareQuery(getColumnFamily(columnFamilyName)).getKey(rowKey).getColumn(column).execute();
			return checkNull(result);

		} catch (ConnectionException e) {
			logger.error("Exception while checking the column data exists of (columnFamily~rowKey) " + columnFamilyName + SEPERATOR + rowKey);
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
			rowsResult = getKeyspace().prepareQuery(getColumnFamily(columnFamilyName)).getAllRows().setExceptionCallback(new ExceptionCallback() {

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
			rowsResult = getKeyspace().prepareQuery(getColumnFamily(columnFamilyName)).searchWithIndex();

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
			MutationBatch mutationBatch = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
				try {
					for (Map.Entry<String, String> entry : request.entrySet()) {
					mutationBatch.withRow(getColumnFamily(columnFamilyName), rowKey).putColumn(entry.getKey(), entry.getValue());
					}
					mutationBatch.execute();
				} catch (ConnectionException e) {
					logger.error("Exception while inserting the string value to (columnFamily~rowKey) " + columnFamilyName + SEPERATOR + rowKey);
				}
		}
	}
	
	/*
	 * @param columnFamilyName is name of columnFamily
	 * 
	 * @param rowKey is value of key
	 * 
	 * @param request contains column and its string value to be inserted
	 */
	public void putExpireStringValue(String columnFamilyName, String rowKey, Map<String, String> request,Integer expireSeconds) {
		if (checkNull(rowKey) && checkNull(request)) {
			MutationBatch mutationBatch = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
			for (Map.Entry<String, String> entry : request.entrySet()) {
				try {
					mutationBatch.withRow(getColumnFamily(columnFamilyName), rowKey).putColumn(entry.getKey(), entry.getValue(),expireSeconds);
					mutationBatch.execute();
				} catch (ConnectionException e) {
					logger.error("Exception while inserting the string value to (columnFamily~rowKey) " + columnFamilyName + SEPERATOR + rowKey);
					continue;
				}
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
			for (Column<String> column : row.getColumns()) {
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
				resultMap.put(column.getName(), column.getStringValue());
			}
			dataList.add(resultMap);
		}
		return dataList;
	}

	/*
	 * @param result is the result of Cassandra Row query with list of columns
	 */
	public Map<String, Long> getLongValue(OperationResult<ColumnList<String>> result) {

		Map<String, Long> resultMap = new HashMap<String, Long>();
		for (Column<String> column : result.getResult()) {
			resultMap.put(column.getName(), column.getLongValue());
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
	 * @param data is to convert Set to List
	 */
	public List<String> convertSettoList(Set<String> data) {

		List<String> dataList = new ArrayList<String>();
		for (String entry : data) {
			dataList.add(entry);
		}
		return dataList;
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
			logger.error("Exception:"+e);
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

	public String getValue(String data,String comparable){
		
		for(String name : data.split(",")){
			if(name.contains(comparable))
				return name;
		}
		return null;
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

	public Set<String> removeKey(String value,String comparable){
		
		Set<String> rowKey = new HashSet<String>();
		for(String key : value.split(",")){
			if(!comparable.equalsIgnoreCase(key)){
				rowKey.add(key);
			}
		}
		return rowKey;
	}
	/*
	 * @param entry is the given data
	 * 
	 * @param jsonMap is the formula Json
	 * 
	 * @param currentDate is the current date
	 * 
	 * @param eventData is the event detail
	 */
	public Map<String, Long> calculation(Map<String, String> entry, JsonObject jsonMap, String currentDate, Map<String, String> eventData) {

		Map<String, Long> resultMap = new HashMap<String, Long>();
		
		JsonElement jsonElement = new JsonParser().parse(jsonMap.get(formulaDetail.FORMULAS.formulaDetail()).toString());
		JsonArray formulaArray = jsonElement.getAsJsonArray();
		for (int i = 0; i < formulaArray.size(); i++) {
			try {
				JsonElement obj = formulaArray.get(i);

				JsonObject columnFormula = obj.getAsJsonObject();

				if (columnFormula.has(formulaDetail.FORMULA.formulaDetail()) && columnFormula.has(formulaDetail.STATUS.formulaDetail())
						&& formulaDetail.ACTIVE.formulaDetail().equalsIgnoreCase(columnFormula.get(formulaDetail.STATUS.formulaDetail()).getAsString())) {

					Map<String, String> variableMap = new HashMap<String, String>();
					Set<String> name = formOrginalKey(columnFormula.get(formulaDetail.NAME.formulaDetail()).getAsString(), eventData, format, currentDate);
					String columnName = convertSettoString(name);

					try {
						String[] values = columnFormula.get(formulaDetail.REQUEST_VALUES.formulaDetail()).getAsString().split(COMMA);

						for (String value : values) {
							Set<String> columnSet = formOrginalKey(columnFormula.get(value).getAsString(), eventData, format, currentDate);
							if (checkNull(columnSet))
								variableMap.put(value, convertSettoString(columnSet));
						}

						ExpressionBuilder expressionBuilder = new ExpressionBuilder(columnFormula.get(formulaDetail.FORMULA.formulaDetail()).getAsString());
						for (Map.Entry<String, String> map : variableMap.entrySet()) {
							expressionBuilder.withVariable(map.getKey(), entry.get(map.getValue()) != null ? Long.valueOf(entry.get(map.getValue()).toString()) : 0L);
						}
						long calculated = Math.round(expressionBuilder.build().calculate());
						resultMap.put(columnName, calculated);
					} catch (Exception e) {
						resultMap.put(columnName, 0L);
						logger.error("mathametical error" + e);
						continue;
					}
				}
			} catch (Exception e) {
				logger.debug("formula detail is not an json Element");
				continue;
			}
		}
		return resultMap;
	}

	public Map<String, Long> SubEventcalculation(Map<String, String> entry, JsonObject jsonMap) {

		Map<String, Long> resultMap = new HashMap<String, Long>();
		
		JsonElement jsonElement = new JsonParser().parse(jsonMap.get(formulaDetail.FORMULAS.formulaDetail()).toString());
		JsonArray formulaArray = jsonElement.getAsJsonArray();
		for (int i = 0; i < formulaArray.size(); i++) {
			try {
				JsonElement obj = formulaArray.get(i);

				JsonObject columnFormula = obj.getAsJsonObject();

				if (columnFormula.has(formulaDetail.FORMULA.formulaDetail()) && columnFormula.has(formulaDetail.STATUS.formulaDetail())
						&& formulaDetail.ACTIVE.formulaDetail().equalsIgnoreCase(columnFormula.get(formulaDetail.STATUS.formulaDetail()).getAsString())) {

					String columnName = columnFormula.get(formulaDetail.NAME.formulaDetail()).getAsString();

					try {
						String[] values = columnFormula.get(formulaDetail.REQUEST_VALUES.formulaDetail()).getAsString().split(COMMA);

						ExpressionBuilder expressionBuilder = new ExpressionBuilder(columnFormula.get(formulaDetail.FORMULA.formulaDetail()).getAsString());
						for (String value : values) {
							expressionBuilder.withVariable(value, entry.get(columnFormula.get(value).getAsString()) != null ? Long.valueOf(entry.get(columnFormula.get(value).getAsString()).toString()) : 0L);
						}
						long calculated = Math.round(expressionBuilder.build().calculate());
						resultMap.put(columnName, calculated);
					} catch (Exception e) {
						resultMap.put(columnName, 0L);
						logger.error("mathametical error" + e);
						continue;
					}
				}
			} catch (Exception e) {
				logger.debug("formula detail is not an json Element");
				continue;
			}
		}
		return resultMap;
	}
	public Map<String, Long> processSubEvents(Map<String, String> entry, JsonObject jsonMap,String eventName){
		String eventNames = jsonMap.get(formulaDetail.EVENTS.formulaDetail()).getAsString();
		Set<String> rowKey = removeKey(eventNames,eventName);
		Map<String,Long> resultData = new HashMap<String, Long>();
		if(checkNull(rowKey)){
		List<Map<String,String>> formulaDetail = getRowsColumnStringValue(readRows(columnFamily.FORMULA_DETAIL.columnFamily(),rowKey, new ArrayList<String>()), new ArrayList<String>());
		for(Map<String, String> map : formulaDetail){
		JsonElement jsonElement = new JsonParser().parse(map.get(FORMULA).toString());
		JsonObject jsonObject = jsonElement.getAsJsonObject();
		Map<String,Long> tempMap = SubEventcalculation(entry, jsonObject);
		if(checkNull(tempMap)){
		resultData.putAll(tempMap);
		}
		}
		}
		return resultData;
		}
	/*
	 * C: defines => Constant D: defines => Date format lookup E: defines =>
	 * eventMap param lookup
	 */
	public Set<String> formOrginalKey(Set<String> rowKeys, Map<String, String> eventData, SimpleDateFormat currentFormat, String currentDate) {

		Set<String> formedKeys = new TreeSet<String>();
		for (String keys : rowKeys) {
			String formedKey = keys.replaceAll(columnKey.C.columnKey(), EMPTY);
			formedKey = formedKey.replaceAll(columnKey.E.columnKey(), EMPTY);
			if (formedKey.contains(columnKey.D.columnKey())) {
				Date currentDateTime;
				try {
					currentDateTime = currentFormat.parse(currentDate);
					String[] dateFormat = formedKey.split(columnKey.D.columnKey());
					dateFormat = dateFormat[1].split(SEPERATOR);
					SimpleDateFormat customDateFormatter = new SimpleDateFormat(dateFormat[0]);
					formedKey = formedKey.replaceAll(columnKey.D.columnKey() + dateFormat[0], customDateFormatter.format(currentDateTime));
				} catch (ParseException e) {
					continue;
				}
			}
			formedKeys.add(formedKey);
		}
		return substituteKeyVariable(eventData, convertSettoString(formedKeys));
	}

	/*
	 * C: defines => Constant D: defines => Date format lookup E: defines =>
	 * eventMap param lookup
	 */
	public Set<String> formOrginalKey(String rowKeys, Map<String, String> eventData, SimpleDateFormat currentFormat, String currentDate) {

		String formedKey = rowKeys.replaceAll(columnKey.C.columnKey(), EMPTY);
		formedKey = formedKey.replaceAll(columnKey.E.columnKey(), EMPTY);
		if (columnKey.D.columnKey().contains(formedKey)) {
			Date currentDateTime;
			try {
				currentDateTime = currentFormat.parse(currentDate);
				String[] dateFormat = formedKey.split(columnKey.D.columnKey());
				dateFormat = dateFormat[1].split(SEPERATOR);
				SimpleDateFormat customDateFormatter = new SimpleDateFormat(dateFormat[0]);
				formedKey = formedKey.replaceAll(columnKey.D.columnKey() + dateFormat[0], customDateFormatter.format(currentDateTime));
			} catch (ParseException e) {
			}
		}
		return substituteKeyVariable(eventData, formedKey);
	}
}
