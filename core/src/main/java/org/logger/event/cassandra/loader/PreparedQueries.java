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

	private final PreparedStatement INSERT_USER_SESSION = getCassSession().prepare(Constants.INSERT_USER_SESSION);

	private final PreparedStatement INSERT_USER_LAST_SESSION = getCassSession().prepare(Constants.INSERT_USER_LAST_SESSION);

	private final PreparedStatement INSERT_USER_SESSION_ACTIVITY = getCassSession().prepare(Constants.INSERT_USER_SESSION_ACTIVITY);

	private final PreparedStatement INSERT_STUDENTS_CLASS_ACTIVITY = getCassSession().prepare(Constants.INSERT_STUDENTS_CLASS_ACTIVITY);

	private final PreparedStatement INSERT_CONTENT_TAXONOMY_ACTIVITY = getCassSession().prepare(Constants.INSERT_CONTENT_TAXONOMY_ACTIVITY);

	private final  PreparedStatement INSERT_CONTENT_CLASS_TAXONOMY_ACTIVITY = getCassSession().prepare(Constants.INSERT_CONTENT_CLASS_TAXONOMY_ACTIVITY);

	private final PreparedStatement INSERT_USER_LOCATION = getCassSession().prepare(Constants.INSERT_USER_LOCATION);

	private final PreparedStatement UPDATE_PEER_COUNT = getCassSession().prepare(Constants.UPDATE_PEER_COUNT);

	// private final PreparedStatement UPDATE_PEER_DETAILS_ON_START = getCassSession().prepare(Constants.UPDATE_PEER_DETAILS_ON_START);

	// private final PreparedStatement UPDATE_PEER_DETAILS_ON_STOP = getCassSession().prepare(Constants.UPDATE_PEER_DETAILS_ON_STOP);

	private final PreparedStatement SELECT_USER_SESSION_ACTIVITY = getCassSession().prepare(Constants.SELECT_USER_SESSION_ACTIVITY);

	private final PreparedStatement SELECT_USER_SESSION_ACTIVITY_BY_SESSION_ID = getCassSession().prepare(Constants.SELECT_USER_SESSION_ACTIVITY_BY_SESSION_ID);

	private final PreparedStatement SELECT_STUDENTS_CLASS_ACTIVITY = getCassSession().prepare(Constants.SELECT_STUDENTS_CLASS_ACTIVITY);

	private final PreparedStatement UPDATE_REACTION = getCassSession().prepare(Constants.UPDATE_REACTION);

	private final PreparedStatement UPDATE_SESSION_SCORE = getCassSession().prepare(Constants.UPDATE_SESSION_SCORE);

	private final PreparedStatement SELECT_CLASS_ACTIVITY_DATACUBE = getCassSession().prepare(Constants.SELECT_CLASS_ACTIVITY_DATACUBE);

	private final PreparedStatement SELECT_ALL_CLASS_ACTIVITY_DATACUBE = getCassSession().prepare(Constants.SELECT_ALL_CLASS_ACTIVITY_DATACUBE);

	private final PreparedStatement INSERT_CLASS_ACTIVITY_DATACUBE = getCassSession().prepare(Constants.INSERT_CLASS_ACTIVITY_DATACUBE);

	private final PreparedStatement SELECT_TAXONOMY_PARENT_NODE = getCassSession().prepare(Constants.SELECT_TAXONOMY_PARENT_NODE);

	private final PreparedStatement SELECT_CONTENT_TAXONOMY_ACTIVITY = getCassSession().prepare(Constants.SELECT_CONTENT_TAXONOMY_ACTIVITY);

	private final PreparedStatement SELECT_CONTENT_CLASS_TAXONOMY_ACTIVITY = getCassSession().prepare(Constants.SELECT_CONTENT_CLASS_TAXONOMY_ACTIVITY);

	private final PreparedStatement SELECT_TAXONOMY_ACTIVITY_DATACUBE = getCassSession().prepare(Constants.SELECT_TAXONOMY_ACTIVITY_DATACUBE);

	private final PreparedStatement INSERT_USER_QUESTION_GRADE = getCassSession().prepare(Constants.INSERT_USER_QUESTION_GRADE);

	private final PreparedStatement SELECT_USER_QUESTION_GRADE_BY_SESSION = getCassSession().prepare(Constants.SELECT_USER_QUESTION_GRADE_BY_SESSION);

	private final PreparedStatement SELECT_USER_QUESTION_GRADE_BY_QUESTION = getCassSession().prepare(Constants.SELECT_USER_QUESTION_GRADE_BY_QUESTION);

	private final PreparedStatement INSERT_TAXONOMY_ACTIVITY_DATACUBE = getCassSession().prepare(Constants.INSERT_TAXONOMY_ACTIVITY_DATACUBE);
	
	private final PreparedStatement INSERT_EVENTS_TIMELINE = getCassSession().prepare(Constants.INSERT_EVENTS_TIMELINE);
	
	private final PreparedStatement INSERT_EVENTS = getCassSession().prepare(Constants.INSERT_EVENTS);
	
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
}
