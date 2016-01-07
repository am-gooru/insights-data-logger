package org.logger.event.cassandra.loader.dao;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.ednovo.data.model.ClassActivityV2;
import org.ednovo.data.model.StudentsClassActivity;
import org.logger.event.cassandra.loader.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;

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
			
			/**
			 * Lesson wise
			 */
			ClassActivityV2 aggregatedLessonActivity = baseCassandraDao.getStudentsClassActivityV2(appendTilda(studentsClassActivity.getClassUid(), studentsClassActivity.getCourseUid(), studentsClassActivity.getUnitUid(),studentsClassActivity.getLessonUid()), studentsClassActivity.getUserUid(), studentsClassActivity.getCollectionType());			
			aggregatedLessonActivity.setRowKey(appendTilda(studentsClassActivity.getClassUid(), studentsClassActivity.getCourseUid(), studentsClassActivity.getUnitUid()));
			aggregatedLessonActivity.setLeafNode(studentsClassActivity.getLessonUid());
			baseCassandraDao.saveStudentsClassActivityV2(aggregatedLessonActivity);
			
			/**
			 * Unit wise
			 */
			ClassActivityV2 aggregatedUnitActivity = baseCassandraDao.getStudentsClassActivityV2(appendTilda(studentsClassActivity.getClassUid(), studentsClassActivity.getCourseUid(), studentsClassActivity.getUnitUid()), studentsClassActivity.getUserUid(), studentsClassActivity.getCollectionType());			
			aggregatedUnitActivity.setRowKey(appendTilda(studentsClassActivity.getClassUid(), studentsClassActivity.getCourseUid()));
			aggregatedUnitActivity.setLeafNode(studentsClassActivity.getUnitUid());
			baseCassandraDao.saveStudentsClassActivityV2(aggregatedUnitActivity);
			
			/**
			 * Course wise
			 */
			ClassActivityV2 aggregatedCourseActivity = baseCassandraDao.getStudentsClassActivityV2(appendTilda(studentsClassActivity.getClassUid(), studentsClassActivity.getCourseUid()), studentsClassActivity.getUserUid(), studentsClassActivity.getCollectionType());			
			aggregatedCourseActivity.setRowKey(studentsClassActivity.getClassUid());
			aggregatedCourseActivity.setLeafNode(studentsClassActivity.getCourseUid());
			baseCassandraDao.saveStudentsClassActivityV2(aggregatedCourseActivity);
			
			
			
		} catch (Exception e) {
			logger.error("Error while rolling up data", e);
		}
		
		
	}
	
	private ClassActivityV2 setAggregatedActivity(Rows<String, String> result , ClassActivityV2 aggregatedActivity){
		if (result.size() > 0) {
			long score = 0L; long views = 0L ; long timeSpent = 0L; long attemptedCount = 0;
			for (Row<String, String> row : result) {
				attemptedCount++;
				ColumnList<String> columns = row.getColumns();
				score += columns.getLongValue("score", 0L);
				timeSpent += columns.getLongValue("time_spent", 0L);
				views += columns.getLongValue("views", 0L);
			}
			aggregatedActivity.setScore(score/attemptedCount);
			aggregatedActivity.setTimeSpent(timeSpent);
			aggregatedActivity.setViews(views);
		}
		return aggregatedActivity;
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
