package org.kafka.event.microaggregator.dao;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.bcel.generic.DASTORE;
import org.kafka.event.microaggregator.core.CassandraConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import atg.taglib.json.util.JSONException;
import atg.taglib.json.util.JSONObject;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
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
import com.netflix.astyanax.query.AllRowsQuery;
import com.netflix.astyanax.query.IndexColumnExpression;
import com.netflix.astyanax.query.IndexQuery;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.query.RowSliceQuery;
import com.netflix.astyanax.serializers.StringSerializer;

import de.congrace.exp4j.Calculable;
import de.congrace.exp4j.ExpressionBuilder;
import de.congrace.exp4j.UnknownFunctionException;
import de.congrace.exp4j.UnparsableExpressionException;

public class AggregationDAOImpl extends BaseDAOCassandraImpl implements AggregationDAO {

	Gson gson = new Gson();

	public static final Logger logger = LoggerFactory.getLogger(AggregationDAOImpl.class);

	public static final String aggregatorDetail = "aggregation_detail";

	public static final String formulaDetail = "formula_detail";

	public static final String eventTimeLine = "event_timeline";

	public static final String eventDetail = "event_detail";

	public static final String liveDashboard = "live_dashboard";

	public static final String jobConfigSetting = "job_config_settings";

	public static final String separator = "~";

	public AggregationDAOImpl(CassandraConnectionProvider connectionProvider) {
		super(connectionProvider);
	}

	public ColumnFamily<String, String> getColumnFamily(String columnFamilyName) {

		return new ColumnFamily<String, String>(columnFamilyName, StringSerializer.get(), StringSerializer.get());

	}

	public void startStaticAggregation(String startTime, String endTime) {

		handleAggregation(startTime, endTime);

	}

	public void handleAggregation(String startTime, String endTime) {
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMddkkmm");
		List<String> keys = new ArrayList<String>();
		List<String> column = new ArrayList<String>();
		column.add("constant_value");
		column.add("last_processed_time");
		OperationResult<ColumnList<String>> configData = this.readRow(jobConfigSetting, "aggregation~keys", column);
		column = new ArrayList<String>();
		column.add("constant_value");
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
				keys.add(startTime);
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
			column.add("last_processed_time");
			List<String> processedTime = this.listRowColumnStringValue(configData, column);
			if (checkNull(processedTime)) {
				Calendar calender = Calendar.getInstance();
				endTime = format.format(convertListtoString(processedTime));
				keys.add(endTime);
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

		// get Event Ids for every minute
		List<Map<String, String>> rowData = this.getRowsKeyColumnStringValue(this.readRows(eventTimeLine, keys, new ArrayList<String>()));

		// iterate for every minute
		for (Map<String, String> fetchedkey : rowData) {
			column = new ArrayList<String>();
			column.add("event_name");
			column.add("fields");

			// get raw data for processing
			List<Map<String, String>> eventData = this.getRowsColumnStringValue(this.readRows(eventDetail, convertArraytoList(String.valueOf(fetchedkey.get("value")).split(",")), column),
					new ArrayList<String>());

			// get normal Formula
			column = new ArrayList<String>();
			column.add("formula");
			Map<String, Object> whereCondition = new HashMap<String, Object>();
			whereCondition.put("aggregate_type", "normal");
			List<Map<String, Object>> normalFormulaDetails = this.getRowsKeyStringValue(this.readWithIndex(formulaDetail, whereCondition, column), new ArrayList<String>());

			// fetch how many key need this aggregation
			for (String processKey : prcessingKey) {

				column = new ArrayList<String>();
				column.add("constant_value");
				column.add("item_value");
				OperationResult<ColumnList<String>> processResult = readRow(jobConfigSetting, processKey, column);
				List<String> secondKeyList = this.listRowColumnStringValue(processResult, convertStringtoList(column.get(1)));
				String tempkey = convertListtoString(secondKeyList);
				String lastProcessedKey = null;

				// iterate for every raw data key
				Set<String> secondKey = substituteKeyVariable(eventData, tempkey);
				List<String> firstKey = this.listRowColumnStringValue(processResult, convertStringtoList(column.get(0)));
				firstKey = convertStringtoList(firstKey.get(0));
				Set<String> rowKey = new HashSet<String>();
				try {
					rowKey = this.combineTwoKey(convertDateFormat(fetchedkey.get("key"), format, firstKey), secondKey);
				} catch (ParseException e) {
					e.printStackTrace();
				}

				// update live dashboard
				List<Map<String, Object>> dashboardData = this.getRowsKeyLongValue(this.readRows(liveDashboard, rowKey, new ArrayList<String>()), new ArrayList<String>());

				// insert data for normal aggregation
				for (Map<String, Object> countMap : dashboardData) {
					Map<String, Long> resultMap = new HashMap<String, Long>();
					for (Map<String, Object> formulaMap : normalFormulaDetails) {
						Map<String, String> formulaDetail = new HashMap<String, String>();
						JsonElement jsonElement = new JsonParser().parse(formulaMap.get("formula").toString());
						JsonObject jsonObject = jsonElement.getAsJsonObject();
						formulaDetail = gson.fromJson(jsonObject, formulaDetail.getClass());
						resultMap = calculation(countMap, formulaDetail);
					}
					if (!checkNull(resultMap)) {
						continue;
					}
					System.out.println(countMap.get("key").toString());
					incrementCounterValue(liveDashboard, countMap.get("key").toString(), resultMap);
					lastProcessedKey = countMap.get("key").toString();
				}
				logger.info("processed key" + lastProcessedKey);
			}
			Map<String, String> data = new HashMap<String, String>();
			data.put("last_processed_time", fetchedkey.get("key").toString());
			putStringValue(liveDashboard, "aggregation~keys", data);
		}
		logger.info("Minute Aggregator Runned Successfully");
	}

	public Set<String> substituteKeyVariable(List<Map<String, String>> eventData, String tempKey) {

		String key = null;
		for (Map<String, String> map : eventData) {

			// Avoid empty data
			if (!checkNull(map.get("event_name")) || !checkNull(map.get("fields"))) {
				continue;
			}

			Map<String, JsonElement> rawData = new HashMap<String, JsonElement>();
			String eventName = map.get("event_name");
			JsonElement jsonElement = new JsonParser().parse(map.get("fields"));
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
				jsonElement = new JsonParser().parse(rawMap.get("context"));
				jsonObject = jsonElement.getAsJsonObject();
				contextMap = gson.fromJson(jsonObject, contextMap.getClass());
				contextMap.put("organizationUId", contextMap.get("organizationUId") != null && contextMap.get("organizationUId") != "" ? contextMap.get("organizationUId")
						: "4261739e-ccae-11e1-adfb-5404a609bd14");
			} catch (Exception e) {
				logger.debug("Context is not an json Element");
			}
			keyData = contextMap.keySet();
			key = replaceKey(key, contextMap, keyData);
			Map<String, String> sessionMap = new HashMap<String, String>();
			try {
				jsonElement = new JsonParser().parse(rawMap.get("session"));
				jsonObject = jsonElement.getAsJsonObject();
				sessionMap = gson.fromJson(jsonObject, sessionMap.getClass());
			} catch (Exception e) {
				logger.debug("session is not an json Element");
			}
			keyData = sessionMap.keySet();
			key = replaceKey(key, sessionMap, keyData);
			Map<String, String> userMap = new HashMap<String, String>();
			try {
				jsonElement = new JsonParser().parse(rawMap.get("user"));
				jsonObject = jsonElement.getAsJsonObject();
				userMap = gson.fromJson(jsonObject, userMap.getClass());
			} catch (Exception e) {
				logger.debug("user is not an json Element");
			}
			keyData = userMap.keySet();
			key = replaceKey(key, userMap, keyData);
			Map<String, String> payLoadMap = new HashMap<String, String>();
			try {
				jsonElement = new JsonParser().parse(rawMap.get("payLoadObject"));
				jsonObject = jsonElement.getAsJsonObject();
				payLoadMap = gson.fromJson(jsonObject, payLoadMap.getClass());
			} catch (Exception e) {
				logger.debug("payLoadObject is not an json Element");
			}
			keyData = payLoadMap.keySet();
			key = replaceKey(key, payLoadMap, keyData);
		}
		return convertStringtoSet(key);
	}

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
			logger.error("Exception while getting row data");
			e.printStackTrace();
		}
		return result;
	}

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
			logger.error("Exception while getting row data");
			e.printStackTrace();
		}
		return result;
	}

	public Map<String, Long> defaultCounterValue(Collection<String> column, Long counter) {
		Map<String, Long> resultSet = new HashMap<String, Long>();
		for (String columnName : column) {
			resultSet.put(columnName, counter);
		}
		return resultSet;
	}

	public void putStringValue(String columnFamilyName, String rowKey, Map<String, String> request) {
		if (checkNull(rowKey) && checkNull(request)) {
			MutationBatch mutationBatch = this.getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
			for (Map.Entry<String, String> entry : request.entrySet()) {
				mutationBatch.withRow(this.getColumnFamily(columnFamilyName), rowKey).putColumnIfNotNull(entry.getKey(), entry.getValue());
			}
			try {
				mutationBatch.execute();
			} catch (ConnectionException e) {
				e.printStackTrace();
			}
		}
	}

	public void incrementCounterValue(String columnFamilyName, String rowKey, Map<String, Long> request) {
		if (checkNull(rowKey) && checkNull(request)) {
			MutationBatch mutationBatch = this.getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
			for (Map.Entry<String, Long> entry : request.entrySet()) {
				mutationBatch.withRow(this.getColumnFamily(columnFamilyName), rowKey).incrementCounterColumn(entry.getKey(), entry.getValue());
			}
			try {
				mutationBatch.execute();
			} catch (ConnectionException e) {
				e.printStackTrace();
			}
		}
	}

	public void putLongValue(String columnFamilyName, String rowKey, Map<String, Long> request) {
		if (checkNull(rowKey) && checkNull(request)) {
			MutationBatch mutationBatch = this.getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
			for (Map.Entry<String, Long> entry : request.entrySet()) {
				mutationBatch.withRow(this.getColumnFamily(columnFamilyName), rowKey).putColumnIfNotNull(entry.getKey(), entry.getValue());
			}
			try {
				mutationBatch.execute();
			} catch (ConnectionException e) {
				e.printStackTrace();
			}
		}
	}

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
			logger.error("Exception while getting column data");
			e.printStackTrace();
		}
		return result;
	}

	public Map<String, String> getRowStringValue(OperationResult<ColumnList<String>> result) {

		Map<String, String> resultList = new HashMap<String, String>();
		for (Column<String> column : result.getResult()) {
			resultList.put(column.getName(), column.getStringValue());
		}
		return resultList;
	}

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

	public List<String> listRowsStringValue(OperationResult<Rows<String, String>> result) {

		List<String> resultList = new ArrayList<String>();
		for (Row<String, String> row : result.getResult()) {
			for (Column<String> column : row.getColumns()) {
				resultList.add(column.getStringValue());
			}
		}
		return resultList;
	}

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

	public List<Map<String, String>> getRowsKeyColumnStringValue(OperationResult<Rows<String, String>> result) {

		List<Map<String, String>> data = new ArrayList<Map<String, String>>();
		for (Row<String, String> row : result.getResult()) {
			Map<String, String> map = new HashMap<String, String>();
			StringBuffer columnValue = new StringBuffer();
			boolean hasData = false;
			for (Column<String> column : row.getColumns()) {
				hasData = true;
				if (checkNull(columnValue.toString())) {
					columnValue.append("," + column.getStringValue());
				} else {
					columnValue.append(column.getStringValue());
				}

			}
			if (hasData) {
				map.put("key", row.getKey());
				map.put("value", columnValue.toString());
				data.add(map);
			}
		}
		return data;
	}

	public List<Map<String, Object>> getRowsKeyLongValue(OperationResult<Rows<String, String>> result, List<String> columnNames) {

		List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
		if (checkNull(columnNames)) {
			for (Row<String, String> row : result.getResult()) {
				Map<String, Object> map = new HashMap<String, Object>();
				for (Column<String> column : row.getColumns()) {
					for (String columnName : columnNames) {
						if (columnName.equalsIgnoreCase(column.getName()))
							map.put(column.getName(), column.getLongValue());
						break;
					}
				}
				map.put("key", row.getKey());
				resultList.add(map);
			}
		} else {
			for (Row<String, String> row : result.getResult()) {
				Map<String, Object> map = new HashMap<String, Object>();
				for (Column<String> column : row.getColumns()) {
					map.put(column.getName(), column.getLongValue());
					break;
				}
				map.put("key", row.getKey());
				resultList.add(map);
			}
		}
		return resultList;
	}

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
				map.put("key", row.getKey());
				resultList.add(map);
			}
		} else {
			for (Row<String, String> row : result.getResult()) {
				Map<String, Object> map = new HashMap<String, Object>();
				for (Column<String> column : row.getColumns()) {
					map.put(column.getName(), column.getStringValue());
				}
				map.put("key", row.getKey());
				resultList.add(map);
			}

		}
		return resultList;
	}

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

	public Map<String, Object> getLongValue(OperationResult<ColumnList<String>> result) {

		Map<String, Object> resultMap = new HashMap<String, Object>();
		for (Column<String> column : result.getResult()) {
			resultMap.put(column.getStringValue(), column.getLongValue());
		}
		return resultMap;
	}

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
			e.printStackTrace();
		}
		return result;
	}

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
			e.printStackTrace();
		}
		return result;
	}

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

	public Set<String> combineTwoKey(Set<String> firstKey, Set<String> secondKey) {

		Set<String> combinedKey = new HashSet<String>();
		for (String entry1 : firstKey) {
			combinedKey.add(entry1);
			for (String entry2 : secondKey) {
				combinedKey.add(entry2);
				combinedKey.add(entry1 + separator + entry2);
			}
		}
		return combinedKey;
	}

	public static boolean checkNull(String data) {

		if (data != null && !data.isEmpty()) {
			return true;
		}
		return false;
	}

	public boolean checkNull(Map<?, ?> data) {

		if (data != null && !data.isEmpty()) {
			return true;
		}
		return false;
	}

	public boolean checkNull(Collection<?> data) {

		if (data != null && !data.isEmpty()) {
			return true;
		}
		return false;
	}

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

	public List<String> convertArraytoList(String[] data) {
		List<String> responseData = new ArrayList<String>();
		for (String entry : data) {
			if (checkNull(entry))
				responseData.add(entry);
		}
		return responseData;
	}

	public String convertSettoString(Set<String> data) {

		StringBuffer stringBuffer = new StringBuffer();
		for (String entry : data) {
			if (!checkNull(entry)) {
				stringBuffer.append(",");
			}
			stringBuffer.append(entry);
		}
		return stringBuffer.toString();
	}

	public List<String> convertStringtoList(String data) {

		List<String> result = new ArrayList<String>();
		for (String entry : data.split(",")) {
			result.add(entry);
		}
		return result;
	}

	public String convertListtoString(List<String> data) {

		StringBuffer stringBuffer = new StringBuffer();
		for (String entry : data) {
			if (checkNull(stringBuffer.toString())) {
				stringBuffer.append(",");
			}
			stringBuffer.append(entry);
		}
		return stringBuffer.toString();
	}

	public Set<String> convertListtoSet(List<String> data) {

		Set<String> result = new HashSet<String>();
		for (String entry : data) {
			result.add(entry);
		}
		return result;
	}

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

	public Set<String> convertStringtoSet(String data) {

		Set<String> result = new HashSet<String>();
		for (String entry : data.split(",")) {
			result.add(entry);
		}
		return result;
	}

	public static Map<String, Long> calculation(Map<String, Object> entry, Map<String, String> jsonMap) {

		Map<String, Long> resultMap = new HashMap<String, Long>();
		for (Map.Entry<String, String> jsonEntry : jsonMap.entrySet()) {
			JsonElement jsonElement = new JsonParser().parse(jsonEntry.getValue());
			JsonObject json = new JsonObject();
			json = jsonElement.getAsJsonObject();
			String name = json.get("name") != null ? json.get("name").toString().replaceAll("\"", "") : jsonEntry.getKey();
			try {
				Map<String, ExpressionBuilder> aggregatedMap = new HashMap<String, ExpressionBuilder>();
				if (json.has("formula")) {
					ExpressionBuilder expressionBuilder = new ExpressionBuilder(json.get("formula").toString().replaceAll("\"", ""));
					if (json.has("requestValues")) {
						for (String request : json.get("requestValues").toString().replaceAll("\"", "").split(",")) {

							String variableName = request.replaceAll("[^a-zA-Z0-9]", "");
							expressionBuilder.withVariable(variableName, (entry.get(request) != null ? Double.parseDouble(entry.get(request).toString()) : 0L));
						}
						long calculatedData = Math.round(expressionBuilder.build().calculate());
						resultMap.put(name, calculatedData);
					}
				}
			} catch (Exception e) {
				resultMap.put(name, 0L);
			}
		}
		return resultMap;
	}
}
