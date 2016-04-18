package org.logger.event.cassandra.loader;

import org.logger.event.cassandra.loader.dao.BaseDAOCassandraImpl;

import com.datastax.driver.core.PreparedStatement;

public final class PreparedQueries extends BaseDAOCassandraImpl {

	private static class PreparedQueriesHolder {
		public static final PreparedQueries INSTANCE = new PreparedQueries();
	}

	public static PreparedQueries getInstance() {
		return PreparedQueriesHolder.INSTANCE;
	}

	private final PreparedStatement INSERT_USER_SESSION = getAnalyticsCassSession().prepare(Constants.INSERT_USER_SESSION);

	private final PreparedStatement INSERT_USER_LAST_SESSION = getAnalyticsCassSession().prepare(Constants.INSERT_USER_LAST_SESSION);

	private final PreparedStatement INSERT_USER_SESSION_ACTIVITY = getAnalyticsCassSession().prepare(Constants.INSERT_USER_SESSION_ACTIVITY);

	private final PreparedStatement INSERT_STUDENTS_CLASS_ACTIVITY = getAnalyticsCassSession().prepare(Constants.INSERT_STUDENTS_CLASS_ACTIVITY);

	private final PreparedStatement INSERT_CONTENT_TAXONOMY_ACTIVITY = getAnalyticsCassSession().prepare(Constants.INSERT_CONTENT_TAXONOMY_ACTIVITY);

	private final PreparedStatement INSERT_CONTENT_CLASS_TAXONOMY_ACTIVITY = getAnalyticsCassSession().prepare(Constants.INSERT_CONTENT_CLASS_TAXONOMY_ACTIVITY);

	private final PreparedStatement INSERT_USER_LOCATION = getAnalyticsCassSession().prepare(Constants.INSERT_USER_LOCATION);

	private final PreparedStatement UPDATE_PEER_COUNT = getAnalyticsCassSession().prepare(Constants.UPDATE_PEER_COUNT);

	private final PreparedStatement SELECT_USER_SESSION_ACTIVITY = getAnalyticsCassSession().prepare(Constants.SELECT_USER_SESSION_ACTIVITY);

	private final PreparedStatement SELECT_USER_SESSION_ACTIVITY_BY_SESSION_ID = getAnalyticsCassSession().prepare(Constants.SELECT_USER_SESSION_ACTIVITY_BY_SESSION_ID);

	private final PreparedStatement SELECT_STUDENTS_CLASS_ACTIVITY = getAnalyticsCassSession().prepare(Constants.SELECT_STUDENTS_CLASS_ACTIVITY);

	private final PreparedStatement UPDATE_REACTION = getAnalyticsCassSession().prepare(Constants.UPDATE_REACTION);

	private final PreparedStatement UPDATE_SESSION_SCORE = getAnalyticsCassSession().prepare(Constants.UPDATE_SESSION_SCORE);

	private final PreparedStatement SELECT_CLASS_ACTIVITY_DATACUBE = getAnalyticsCassSession().prepare(Constants.SELECT_CLASS_ACTIVITY_DATACUBE);

	private final PreparedStatement SELECT_ALL_CLASS_ACTIVITY_DATACUBE = getAnalyticsCassSession().prepare(Constants.SELECT_ALL_CLASS_ACTIVITY_DATACUBE);

	private final PreparedStatement INSERT_CLASS_ACTIVITY_DATACUBE = getAnalyticsCassSession().prepare(Constants.INSERT_CLASS_ACTIVITY_DATACUBE);

	private final PreparedStatement SELECT_TAXONOMY_PARENT_NODE = getAnalyticsCassSession().prepare(Constants.SELECT_TAXONOMY_PARENT_NODE);

	private final PreparedStatement SELECT_CONTENT_TAXONOMY_ACTIVITY = getAnalyticsCassSession().prepare(Constants.SELECT_CONTENT_TAXONOMY_ACTIVITY);

	private final PreparedStatement SELECT_CONTENT_CLASS_TAXONOMY_ACTIVITY = getAnalyticsCassSession().prepare(Constants.SELECT_CONTENT_CLASS_TAXONOMY_ACTIVITY);

	private final PreparedStatement SELECT_TAXONOMY_ACTIVITY_DATACUBE = getAnalyticsCassSession().prepare(Constants.SELECT_TAXONOMY_ACTIVITY_DATACUBE);

	private final PreparedStatement INSERT_USER_QUESTION_GRADE = getAnalyticsCassSession().prepare(Constants.INSERT_USER_QUESTION_GRADE);

	private final PreparedStatement SELECT_USER_QUESTION_GRADE_BY_SESSION = getAnalyticsCassSession().prepare(Constants.SELECT_USER_QUESTION_GRADE_BY_SESSION);

	private final PreparedStatement SELECT_USER_QUESTION_GRADE_BY_QUESTION = getAnalyticsCassSession().prepare(Constants.SELECT_USER_QUESTION_GRADE_BY_QUESTION);

	private final PreparedStatement INSERT_TAXONOMY_ACTIVITY_DATACUBE = getAnalyticsCassSession().prepare(Constants.INSERT_TAXONOMY_ACTIVITY_DATACUBE);
	
	private final PreparedStatement INSERT_EVENTS_TIMELINE = getEventCassSession().prepare(Constants.INSERT_EVENTS_TIMELINE);
	
	private final PreparedStatement INSERT_EVENTS = getEventCassSession().prepare(Constants.INSERT_EVENTS);
	
	private final PreparedStatement UPDATE_STATISTICAL_COUNTER_DATA = getAnalyticsCassSession().prepare(Constants.UPDATE_STATISTICAL_COUNTER_DATA);
	
	private final PreparedStatement SELECT_STATISTICAL_COUNTER_DATA = getAnalyticsCassSession().prepare(Constants.SELECT_STATISTICAL_COUNTER_DATA);
	
	private final PreparedStatement UPDATE_USER_STATISTICAL_COUNTER_DATA = getAnalyticsCassSession().prepare(Constants.UPDATE_USER_STATISTICAL_COUNTER_DATA);
	
	private final PreparedStatement SELECT_USER_STATISTICAL_COUNTER_DATA = getAnalyticsCassSession().prepare(Constants.SELECT_USER_STATISTICAL_COUNTER_DATA);
	
	private final PreparedStatement SELECT_API_KEY = getAnalyticsCassSession().prepare(Constants.SELECT_API_KEY);
	
	private final PreparedStatement SELECT_CLASS_MEMBERS = getAnalyticsCassSession().prepare(Constants.SELECT_CLASS_MEMBERS);
	
	private final PreparedStatement SAVE_CLASS_MEMBERS = getAnalyticsCassSession().prepare(Constants.UPDATE_CLASS_MEMBERS);
	
	private final PreparedStatement REMOVE_CLASS_MEMBERS = getAnalyticsCassSession().prepare(Constants.REMOVE_CLASS_MEMBERS);
	
	private final PreparedStatement DELETE_COURSE_USAGE = getAnalyticsCassSession().prepare(Constants.DELETE_COURSE_USAGE);
	
	private final PreparedStatement DELETE_UNIT_USAGE = getAnalyticsCassSession().prepare(Constants.DELETE_UNIT_USAGE);
	
	private final PreparedStatement DELETE_LESSON_USAGE = getAnalyticsCassSession().prepare(Constants.DELETE_LESSON_USAGE);
	
	private final PreparedStatement DELETE_ASSESSMENT_OR_COLLECTION_USAGE = getAnalyticsCassSession().prepare(Constants.DELETE_ASSESSMENT_OR_COLLECTION_USAGE);
	
	private final PreparedStatement DELETE_DATA_CUBE_BY_ROW_KEY = getAnalyticsCassSession().prepare(Constants.DELETE_DATA_CUBE_BY_ROW_KEY);
	
	private final PreparedStatement DELETE_DATA_CUBE_BY_ROW_KEY_COLUMN = getAnalyticsCassSession().prepare(Constants.DELETE_DATA_CUBE_BY_ROW_KEY_COLUMN);
	
	private final PreparedStatement UPDATE_COLLABORATORS = getAnalyticsCassSession().prepare(Constants.UPDATE_COLLABORATORS);
	
	private final PreparedStatement UPDATE_CONTENT_CREATORS = getAnalyticsCassSession().prepare(Constants.UPDATE_CONTENT_CREATORS);
	
	private final PreparedStatement ADD_STAT_PUBLISHER_QUEUE = getEventCassSession().prepare(Constants.ADD_STAT_PUBLISHER_QUEUE);
	
	private final PreparedStatement INSERT_USER_SESSION_TAXONOMY_ACTIVITY = getAnalyticsCassSession().prepare(Constants.INSERT_USER_SESSION_TAXONOMY_ACTIVITY);
	
	private final PreparedStatement SELECT_USER_SESSION_TAXONOMY_ACTIVITY = getAnalyticsCassSession().prepare(Constants.SELECT_USER_SESSION_TAXONOMY_ACTIVITY);
	
	public PreparedStatement selectApiKey() {
		return SELECT_API_KEY;
	}
	
	public PreparedStatement updateStatisticalCounterData() {
		return UPDATE_STATISTICAL_COUNTER_DATA;
	}
	
	public PreparedStatement selectStatustucalCounterData() {
		return SELECT_STATISTICAL_COUNTER_DATA;
	}
	
	public PreparedStatement updateUserStatisticalCounterData() {
		return UPDATE_USER_STATISTICAL_COUNTER_DATA;
	}
	
	public PreparedStatement selectUserStatisticalCounterData() {
		return SELECT_USER_STATISTICAL_COUNTER_DATA;
	}
	
	public PreparedStatement insertEventsTimeline() {
		return INSERT_EVENTS_TIMELINE;
	}
	
	public PreparedStatement insertEvents() {
		return INSERT_EVENTS;
	}
	
	public PreparedStatement updateSessionScore() {
		return UPDATE_SESSION_SCORE;
	}

	public PreparedStatement insertUserSession() {
		return INSERT_USER_SESSION;
	}

	public PreparedStatement insertUserLastSession() {
		return INSERT_USER_LAST_SESSION;
	}

	public PreparedStatement insertUserSessionActivity() {
		return INSERT_USER_SESSION_ACTIVITY;
	}

	public PreparedStatement insertStudentsClassActivity() {
		return INSERT_STUDENTS_CLASS_ACTIVITY;
	}

	public PreparedStatement insertContentTaxonomyActivity() {
		return INSERT_CONTENT_TAXONOMY_ACTIVITY;
	}

	public PreparedStatement insertContentClassTaxonomyActivty() {
		return INSERT_CONTENT_CLASS_TAXONOMY_ACTIVITY;
	}

	public PreparedStatement insertUserLastLocation() {
		return INSERT_USER_LOCATION;
	}

	public PreparedStatement updatePeerCount() {
		return UPDATE_PEER_COUNT;
	}

	public PreparedStatement selectUserSessionActivity() {
		return SELECT_USER_SESSION_ACTIVITY;
	}

	public PreparedStatement selectUserSessionActivityBySessionId() {
		return SELECT_USER_SESSION_ACTIVITY_BY_SESSION_ID;
	}

	public PreparedStatement selectStudentClassActivity() {
		return SELECT_STUDENTS_CLASS_ACTIVITY;
	}

	public PreparedStatement updateReaction() {
		return UPDATE_REACTION;
	}

	public PreparedStatement selectClassActivityDataCube(PreparedStatement statement) {
		return SELECT_CLASS_ACTIVITY_DATACUBE;
	}

	public PreparedStatement selectAllClassActivityDataCube() {
		return SELECT_ALL_CLASS_ACTIVITY_DATACUBE;
	}

	public PreparedStatement insertClassActivityDataCube() {
		return INSERT_CLASS_ACTIVITY_DATACUBE;
	}

	public PreparedStatement selectTaxonomyParentNode() {
		return SELECT_TAXONOMY_PARENT_NODE;
	}

	public PreparedStatement selectContentTaxonomyActivity() {
		return SELECT_CONTENT_TAXONOMY_ACTIVITY;
	}

	public PreparedStatement selectContentClassTaxonomyActivity() {
		return SELECT_CONTENT_CLASS_TAXONOMY_ACTIVITY;
	}

	public PreparedStatement selectTaxonomyActivityDataCube() {
		return SELECT_TAXONOMY_ACTIVITY_DATACUBE;
	}

	public PreparedStatement insertUserQuestionGrade() {
		return INSERT_USER_QUESTION_GRADE;
	}

	public PreparedStatement selectUserQuestionGradeBySession() {
		return SELECT_USER_QUESTION_GRADE_BY_SESSION;
	}

	public PreparedStatement selectUserQuestionGradeByQuestion() {
		return SELECT_USER_QUESTION_GRADE_BY_QUESTION;
	}

	public PreparedStatement insertTaxonomyActivityDataCube() {
		return INSERT_TAXONOMY_ACTIVITY_DATACUBE;
	}
	
	public PreparedStatement selectClassMembers() {
		return SELECT_CLASS_MEMBERS;
	}
	
	public PreparedStatement saveClassMembers() {
		return SAVE_CLASS_MEMBERS;
	}
	
	public PreparedStatement removeClassMembers() {
		return REMOVE_CLASS_MEMBERS;
	}
	
	public PreparedStatement deleteCourseUsage() {
		return DELETE_COURSE_USAGE;
	}
	
	public PreparedStatement deleteUnitUsage() {
		return DELETE_UNIT_USAGE;
	}
	
	public PreparedStatement deleteLessonUsage() {
		return DELETE_LESSON_USAGE;
	}
	
	public PreparedStatement deleteAssessmentOrCollectionUsage() {
		return DELETE_ASSESSMENT_OR_COLLECTION_USAGE;
	}
	public PreparedStatement deleteDataCubeByRowkey() {
		return DELETE_DATA_CUBE_BY_ROW_KEY;
	}
	public PreparedStatement deleteDataCubeByRowkeyColumn() {
		return DELETE_DATA_CUBE_BY_ROW_KEY_COLUMN;
	}
	public PreparedStatement updateCollaborators() {
		return UPDATE_COLLABORATORS;
	}
	public PreparedStatement updateContentCreator() {
		return UPDATE_CONTENT_CREATORS;
	}
	public PreparedStatement addStatPublisherQueue() {
		return ADD_STAT_PUBLISHER_QUEUE;
	}
	public PreparedStatement insertUserSessionTaxonomyActivity() {
		return INSERT_USER_SESSION_TAXONOMY_ACTIVITY;
	}
	public PreparedStatement selectUserSessionTaxonomyActivity() {
		return SELECT_USER_SESSION_TAXONOMY_ACTIVITY;
	}
}
