package org.logger.event.cassandra.loader.dao;

import java.util.Collection;
import java.util.Map;

import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Rows;

public interface BaseCassandraRepo {

	public Column<String> readWithKeyColumn(String cfName,String key,String columnName);

	public ColumnList<String> readWithKeyColumnList(String cfName,String key,Collection<String> columnList);

	public Rows<String, String> readWithKeyListColumnList(String cfName,Collection<String> keys,Collection<String> columnList);

	public ColumnList<String> readWithKey(String cfName,String key);

	public Rows<String, String> readWithKeyList(String cfName,Collection<String> key);

	public Rows<String, String> readCommaKeyList(String cfName,String... key);

	public Rows<String, String> readIterableKeyList(String cfName,Iterable<String> keys);

	public Rows<String, String> readIndexedColumn(String cfName,String key,String columnName,String value);

	public Rows<String, String> readIndexedColumnList(String cfName,String key,Map<String,String> columnList);

	public Rows<String, String> readAllRows(String cfName);

	public void saveBulkStringList(String cfName, String key,Map<String,String> columnValueList);

	public void saveBulkLongList(String cfName, String key,Map<String,Long> columnValueList);

	public void saveStringValue(String cfName, String key,String columnName,String value);

	public void saveLongValue(String cfName, String key,String columnName,String value);

	public void generateCounter(String cfName,String key,String columnName, long value ,MutationBatch m);

	public void generateNonCounter(String cfName,String key,String columnName, String value ,MutationBatch m);

	public void generateNonCounter(String cfName,String key,String columnName, long value ,MutationBatch m);
}
