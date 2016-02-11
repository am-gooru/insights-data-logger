package org.logger.event.cassandra.loader.dao;

import org.apache.commons.lang.StringUtils;
import org.ednovo.data.model.ClassActivityDatacube;
import org.ednovo.data.model.StudentsClassActivity;
import org.logger.event.cassandra.loader.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClassActivityDataCubeGenerator implements Runnable {

	private BaseCassandraRepoImpl baseCassandraDao;
	
	private StudentsClassActivity studentsClassActivity;
	
	private static Logger logger = LoggerFactory.getLogger(ClassActivityDataCubeGenerator.class);

	public ClassActivityDataCubeGenerator(StudentsClassActivity studentsClassActivity, BaseCassandraRepoImpl baseCassandraDao) {
		this. studentsClassActivity =  studentsClassActivity;
		this.baseCassandraDao = baseCassandraDao;
	}

	public void run() {
		try {
			
			/**
			 * Lesson wise
			 */
			ClassActivityDatacube aggregatedLessonActivity = baseCassandraDao.getStudentsClassActivityDatacube(appendTilda(studentsClassActivity.getClassUid(), studentsClassActivity.getCourseUid(), studentsClassActivity.getUnitUid(),studentsClassActivity.getLessonUid()), studentsClassActivity.getUserUid(), studentsClassActivity.getCollectionType());			
			aggregatedLessonActivity.setRowKey(appendTilda(studentsClassActivity.getClassUid(), studentsClassActivity.getCourseUid(), studentsClassActivity.getUnitUid()));
			aggregatedLessonActivity.setLeafNode(studentsClassActivity.getLessonUid());
			baseCassandraDao.saveClassActivityDataCube(aggregatedLessonActivity);
			
			/**
			 * Unit wise
			 */
			ClassActivityDatacube aggregatedUnitActivity = baseCassandraDao.getStudentsClassActivityDatacube(appendTilda(studentsClassActivity.getClassUid(), studentsClassActivity.getCourseUid(), studentsClassActivity.getUnitUid()), studentsClassActivity.getUserUid(), studentsClassActivity.getCollectionType());			
			aggregatedUnitActivity.setRowKey(appendTilda(studentsClassActivity.getClassUid(), studentsClassActivity.getCourseUid()));
			aggregatedUnitActivity.setLeafNode(studentsClassActivity.getUnitUid());
			baseCassandraDao.saveClassActivityDataCube(aggregatedUnitActivity);
			
			/**
			 * Course wise
			 */
			ClassActivityDatacube aggregatedCourseActivity = baseCassandraDao.getStudentsClassActivityDatacube(appendTilda(studentsClassActivity.getClassUid(), studentsClassActivity.getCourseUid()), studentsClassActivity.getUserUid(), studentsClassActivity.getCollectionType());			
			aggregatedCourseActivity.setRowKey(studentsClassActivity.getClassUid());
			aggregatedCourseActivity.setLeafNode(studentsClassActivity.getCourseUid());
			baseCassandraDao.saveClassActivityDataCube(aggregatedCourseActivity);
			
			
			
		} catch (Exception e) {
			logger.error("Error while rolling up data", e);
		}
		
		
	}
	
	private String appendTilda(String... columns) {
		StringBuilder columnKey = new StringBuilder();
		for (String column : columns) {
			if (StringUtils.isNotBlank(column)) {
				columnKey.append(columnKey.length() > 0 ? Constants.SEPERATOR : Constants.EMPTY);
				columnKey.append(column);
			}
		}
		return columnKey.toString();

	}
}
