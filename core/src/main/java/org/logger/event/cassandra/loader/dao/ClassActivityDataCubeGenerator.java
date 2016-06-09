package org.logger.event.cassandra.loader.dao;

import org.apache.commons.lang.StringUtils;
import org.ednovo.data.model.ClassActivityDatacube;
import org.ednovo.data.model.StudentsClassActivity;
import org.logger.event.cassandra.loader.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClassActivityDataCubeGenerator implements Runnable {

	private final BaseCassandraRepo baseCassandraRepo;

	private final StudentsClassActivity studentsClassActivity;

	private static final Logger logger = LoggerFactory.getLogger(ClassActivityDataCubeGenerator.class);

	public ClassActivityDataCubeGenerator(StudentsClassActivity studentsClassActivity, BaseCassandraRepo baseCassandraDao) {
		this. studentsClassActivity =  studentsClassActivity;
		this.baseCassandraRepo = baseCassandraDao;
	}

	public void run() {
		try {

			/**
			 * Lesson wise
			 */
			ClassActivityDatacube aggregatedLessonActivity = baseCassandraRepo.getStudentsClassActivityDatacube(appendTilda(studentsClassActivity.getClassUid(), studentsClassActivity.getCourseUid(), studentsClassActivity.getUnitUid(),studentsClassActivity.getLessonUid()), studentsClassActivity.getUserUid(), studentsClassActivity.getCollectionType());
			aggregatedLessonActivity.setRowKey(appendTilda(studentsClassActivity.getClassUid(), studentsClassActivity.getCourseUid(), studentsClassActivity.getUnitUid()));
			aggregatedLessonActivity.setLeafNode(studentsClassActivity.getLessonUid());
			baseCassandraRepo.saveClassActivityDataCube(aggregatedLessonActivity);

			/**
			 * Unit wise
			 */
			ClassActivityDatacube aggregatedUnitActivity = baseCassandraRepo.getStudentsClassActivityDatacube(appendTilda(studentsClassActivity.getClassUid(), studentsClassActivity.getCourseUid(), studentsClassActivity.getUnitUid()), studentsClassActivity.getUserUid(), studentsClassActivity.getCollectionType());
			aggregatedUnitActivity.setRowKey(appendTilda(studentsClassActivity.getClassUid(), studentsClassActivity.getCourseUid()));
			aggregatedUnitActivity.setLeafNode(studentsClassActivity.getUnitUid());
			baseCassandraRepo.saveClassActivityDataCube(aggregatedUnitActivity);

			/**
			 * Course wise
			 */
			ClassActivityDatacube aggregatedCourseActivity = baseCassandraRepo.getStudentsClassActivityDatacube(appendTilda(studentsClassActivity.getClassUid(), studentsClassActivity.getCourseUid()), studentsClassActivity.getUserUid(), studentsClassActivity.getCollectionType());
			aggregatedCourseActivity.setRowKey(studentsClassActivity.getClassUid());
			aggregatedCourseActivity.setLeafNode(studentsClassActivity.getCourseUid());
			baseCassandraRepo.saveClassActivityDataCube(aggregatedCourseActivity);



		} catch (Exception e) {
			logger.error("Error while rolling up data", e);
		}


	}

	private static String appendTilda(String... columns) {
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
