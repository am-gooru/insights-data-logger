package org.logger.event.cassandra.loader.dao;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.lang.StringUtils;
import org.ednovo.data.model.ClassActivityDatacube;
import org.ednovo.data.model.EventBuilder;
import org.ednovo.data.model.ObjectBuilder;
import org.ednovo.data.model.StudentsClassActivity;
import org.logger.event.cassandra.loader.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ResultSet;

public class ReComputationDAOImpl extends BaseDAOCassandraImpl implements ReComputationDAO {

	private static final Logger LOG = LoggerFactory.getLogger(ReComputationDAOImpl.class);

	private BaseCassandraRepo baseCassandraDao;

	private final ExecutorService service = Executors.newFixedThreadPool(10);

	public ReComputationDAOImpl() {
		baseCassandraDao = BaseCassandraRepo.instance();
	}
	
	/**
	 * ReComputations if user/teacher deletes/moves an item
	 * @param even
	 */
	@Override
	public void reComputeData(final EventBuilder event) {
		ObjectBuilder objectBuilderHandler = new ObjectBuilder(event);
		StudentsClassActivity studentsClassActivity = objectBuilderHandler.getStudentsClassActivity();
		if (StringUtils.isNotBlank(event.getClassGooruId()) && !event.getClassGooruId().equalsIgnoreCase(Constants.NA)) {
			for (String classId : event.getClassGooruId().split(Constants.COMMA)) {
				ResultSet classMembers = baseCassandraDao.getClassMembers(classId);
				generateDeleteTasks(studentsClassActivity, classMembers.one().getSet("members", String.class));
			}
		} else {
			LOG.info(event.getContentFormat() + " is not mapped with any of the classes");
		}

	}

	private void generateDeleteTasks(final StudentsClassActivity studentsClassActivity, final Set<String> studentsIds) {
		try {
			Set<Callable<String>> deleteTasks = new HashSet<Callable<String>>();
			deleteTasks.add(new Callable<String>() {
				public String call() throws Exception {
					switch (studentsClassActivity.getCollectionType()) {
					case Constants.COURSE:
						courseReCompute(studentsClassActivity, studentsIds);
						break;
					case Constants.UNIT:
						unitReCompute(studentsClassActivity, studentsIds);
						break;
					case Constants.LESSON:
						lessonReCompute(studentsClassActivity, studentsIds);
						break;
					case Constants.COLLECTION:
						collectionReCompute(studentsClassActivity, studentsIds);
						break;
					case Constants.ASSESSMENT:
						assessmentReCompute(studentsClassActivity, studentsIds);
						break;
					default:
						LOG.info("Invalid content format..");
					}
					return Constants.COMPLETED;
				}
			});

			List<Future<String>> taskStatues = service.invokeAll(deleteTasks);
			for (Future<String> taskStatus : taskStatues) {
				LOG.info(taskStatus.get());
			}
		} catch (Exception e) {
			LOG.error("Exception:", e);
		}
	}

	private void courseReCompute(final StudentsClassActivity studentsClassActivity, final Set<String> studentsIds) {
		for (final String studentId : studentsIds) {
			baseCassandraDao.deleteCourseUsage(studentsClassActivity, studentId, Constants.COLLECTION);
			baseCassandraDao.deleteCourseUsage(studentsClassActivity, studentId, Constants.ASSESSMENT);
		}
		for (String classId : studentsClassActivity.getClassUid().split(Constants.COMMA)) {
			baseCassandraDao.deleteClassActivityDataCube(appendTildaSeperator(classId, studentsClassActivity.getCourseUid()));
		}
	}

	private void unitReCompute(final StudentsClassActivity studentsClassActivity, final Set<String> studentsIds) {

		for (String classId : studentsClassActivity.getClassUid().split(Constants.COMMA)) {
			baseCassandraDao.deleteClassActivityDataCube(appendTildaSeperator(classId, studentsClassActivity.getCourseUid(), studentsClassActivity.getUnitUid()));
		}
		for (final String studentId : studentsIds) {
			baseCassandraDao.deleteUnitUsage(studentsClassActivity, studentId, Constants.COLLECTION);
			baseCassandraDao.deleteUnitUsage(studentsClassActivity, studentId, Constants.ASSESSMENT);
			ClassActivityDatacube aggregatedCourseActivityC = baseCassandraDao
					.getStudentsClassActivityDatacube(appendTildaSeperator(studentsClassActivity.getClassUid(), studentsClassActivity.getCourseUid()), studentId, Constants.COLLECTION);
			aggregatedCourseActivityC.setRowKey(studentsClassActivity.getClassUid());
			aggregatedCourseActivityC.setLeafNode(studentsClassActivity.getCourseUid());
			baseCassandraDao.saveClassActivityDataCube(aggregatedCourseActivityC);

			ClassActivityDatacube aggregatedCourseActivityA = baseCassandraDao
					.getStudentsClassActivityDatacube(appendTildaSeperator(studentsClassActivity.getClassUid(), studentsClassActivity.getCourseUid()), studentId, Constants.ASSESSMENT);
			aggregatedCourseActivityA.setRowKey(studentsClassActivity.getClassUid());
			aggregatedCourseActivityA.setLeafNode(studentsClassActivity.getCourseUid());
			baseCassandraDao.saveClassActivityDataCube(aggregatedCourseActivityA);

		}

	}

	private void lessonReCompute(final StudentsClassActivity studentsClassActivity, final Set<String> studentsIds) {

		for (String classId : studentsClassActivity.getClassUid().split(Constants.COMMA)) {
			baseCassandraDao.deleteClassActivityDataCube(appendTildaSeperator(classId, studentsClassActivity.getCourseUid(), studentsClassActivity.getUnitUid(), studentsClassActivity.getLessonUid()));
		}
		for (final String studentId : studentsIds) {
			baseCassandraDao.deleteLessonUsage(studentsClassActivity, studentId, Constants.COLLECTION);
			baseCassandraDao.deleteLessonUsage(studentsClassActivity, studentId, Constants.ASSESSMENT);

			ClassActivityDatacube aggregatedUnitActivityC = baseCassandraDao.getStudentsClassActivityDatacube(
					appendTildaSeperator(studentsClassActivity.getClassUid(), studentsClassActivity.getCourseUid(), studentsClassActivity.getUnitUid()), studentId, Constants.COLLECTION);
			aggregatedUnitActivityC.setRowKey(appendTildaSeperator(studentsClassActivity.getClassUid(), studentsClassActivity.getCourseUid()));
			aggregatedUnitActivityC.setLeafNode(studentsClassActivity.getUnitUid());
			baseCassandraDao.saveClassActivityDataCube(aggregatedUnitActivityC);

			ClassActivityDatacube aggregatedUnitActivityA = baseCassandraDao.getStudentsClassActivityDatacube(
					appendTildaSeperator(studentsClassActivity.getClassUid(), studentsClassActivity.getCourseUid(), studentsClassActivity.getUnitUid()), studentId, Constants.ASSESSMENT);
			aggregatedUnitActivityA.setRowKey(appendTildaSeperator(studentsClassActivity.getClassUid(), studentsClassActivity.getCourseUid()));
			aggregatedUnitActivityA.setLeafNode(studentsClassActivity.getUnitUid());
			baseCassandraDao.saveClassActivityDataCube(aggregatedUnitActivityA);

		}
	}

	private void collectionReCompute(final StudentsClassActivity studentsClassActivity, final Set<String> studentsIds) {
		for (String classId : studentsClassActivity.getClassUid().split(Constants.COMMA)) {
			baseCassandraDao.deleteClassActivityDataCube(appendTildaSeperator(classId, studentsClassActivity.getCourseUid(), studentsClassActivity.getUnitUid(), studentsClassActivity.getLessonUid(),
					studentsClassActivity.getCollectionUid()));

		}
		for (final String studentId : studentsIds) {
			baseCassandraDao.deleteLessonUsage(studentsClassActivity, studentId, Constants.COLLECTION);
			ClassActivityDatacube aggregatedLessonActivity = baseCassandraDao.getStudentsClassActivityDatacube(
					appendTildaSeperator(studentsClassActivity.getClassUid(), studentsClassActivity.getCourseUid(), studentsClassActivity.getUnitUid(), studentsClassActivity.getLessonUid()),
					studentId, Constants.COLLECTION);
			aggregatedLessonActivity.setRowKey(appendTildaSeperator(studentsClassActivity.getClassUid(), studentsClassActivity.getCourseUid(), studentsClassActivity.getUnitUid()));
			aggregatedLessonActivity.setLeafNode(studentsClassActivity.getLessonUid());
			baseCassandraDao.saveClassActivityDataCube(aggregatedLessonActivity);
		}

	}

	private void assessmentReCompute(final StudentsClassActivity studentsClassActivity, final Set<String> studentsIds) {
		for (String classId : studentsClassActivity.getClassUid().split(Constants.COMMA)) {
			baseCassandraDao.deleteClassActivityDataCube(appendTildaSeperator(classId, studentsClassActivity.getCourseUid(), studentsClassActivity.getUnitUid(), studentsClassActivity.getLessonUid(),
					studentsClassActivity.getCollectionUid()));
		}
		for (final String studentId : studentsIds) {
			baseCassandraDao.deleteLessonUsage(studentsClassActivity, studentId, Constants.ASSESSMENT);
			ClassActivityDatacube aggregatedLessonActivity = baseCassandraDao.getStudentsClassActivityDatacube(
					appendTildaSeperator(studentsClassActivity.getClassUid(), studentsClassActivity.getCourseUid(), studentsClassActivity.getUnitUid(), studentsClassActivity.getLessonUid()),
					studentId, Constants.ASSESSMENT);
			aggregatedLessonActivity.setRowKey(appendTildaSeperator(studentsClassActivity.getClassUid(), studentsClassActivity.getCourseUid(), studentsClassActivity.getUnitUid()));
			aggregatedLessonActivity.setLeafNode(studentsClassActivity.getLessonUid());
			baseCassandraDao.saveClassActivityDataCube(aggregatedLessonActivity);
		}
	}


}
