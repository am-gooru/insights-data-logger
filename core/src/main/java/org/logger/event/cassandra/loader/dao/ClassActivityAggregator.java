package org.logger.event.cassandra.loader.dao;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang.StringUtils;
import org.ednovo.data.model.StudentsClassActivity;
import org.logger.event.cassandra.loader.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClassActivityAggregator implements Runnable, Constants {

	private BaseCassandraRepoImpl baseCassandraDao;
	
	private StudentsClassActivity studentsClassActivity;
	
	private static Logger logger = LoggerFactory.getLogger(ClassActivityAggregator.class);

	public ClassActivityAggregator(StudentsClassActivity studentsClassActivity, BaseCassandraRepoImpl baseCassandraDao) {
		this. studentsClassActivity =  studentsClassActivity;
		this.baseCassandraDao = baseCassandraDao;
	}

	public void run() {
		try {
			Map<String,String> keyValues = generateRowKeyValuePair(studentsClassActivity);
			for (Map.Entry<String, String> entry : keyValues.entrySet())
			{
			    baseCassandraDao.compareAndMergeStudentsClassActivityV2(studentsClassActivity, entry.getKey(), entry.getValue());
			    baseCassandraDao.saveStudentsClassActivityV2(studentsClassActivity, entry.getKey(), entry.getValue());
			}
		} catch (Exception e) {
			logger.error("Error while rolling up data", e);
		}
		
		
	}
	private Map<String,String> generateRowKeyValuePair(StudentsClassActivity studentsClassActivity){
		Map<String, String> classActivityKeys = new HashMap<String,String>();
		classActivityKeys.put(appendTilda(studentsClassActivity.getClassUid(),studentsClassActivity.getCourseUid()), studentsClassActivity.getUnitUid());
		classActivityKeys.put(appendTilda(studentsClassActivity.getClassUid(),studentsClassActivity.getCourseUid(),studentsClassActivity.getUnitUid()), studentsClassActivity.getLessonUid());
		classActivityKeys.put(appendTilda(studentsClassActivity.getClassUid(),studentsClassActivity.getCourseUid(),studentsClassActivity.getUnitUid(),studentsClassActivity.getLessonUid()), studentsClassActivity.getCollectionUid());
		return classActivityKeys;
	}	
	private String appendTilda(String... columns) {
		StringBuilder columnKey = new StringBuilder();
		for (String column : columns) {
			if (StringUtils.isNotBlank(column)) {
				columnKey.append(columnKey.length() > 0 ? SEPERATOR : EMPTY);
				columnKey.append(column);
			}
		}
		return columnKey.toString();

	}
}
