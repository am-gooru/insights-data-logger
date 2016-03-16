package org.logger.event.cassandra.loader;

import org.logger.event.cassandra.loader.dao.BaseDAOCassandraImpl;

import com.datastax.driver.core.PreparedStatement;

public class PreparedQueries extends BaseDAOCassandraImpl {

	private static class PreparedQueriesHolder {
		public static final PreparedQueries INSTANCE = new PreparedQueries();
	}

	public static PreparedQueries getInstance() {
		return PreparedQueriesHolder.INSTANCE;
	}

	private PreparedStatement INSERT_USER_SESSION = null;

	private PreparedStatement INSERT_USER_LAST_SESSION = null;

	private PreparedStatement INSERT_USER_SESSION_ACTIVITY = null;

	private PreparedStatement INSERT_STUDENTS_CLASS_ACTIVITY = null;

	private PreparedStatement INSERT_CONTENT_TAXONOMY_ACTIVITY = null;

	private PreparedStatement INSERT_CONTENT_CLASS_TAXONOMY_ACTIVITY = null;

	private PreparedStatement INSERT_USER_LOCATION = null;

	private PreparedStatement UPDATE_PEER_COUNT = null;

	// private PreparedStatement UPDATE_PEER_DETAILS_ON_START = getCassSession().prepare(Constants.UPDATE_PEER_DETAILS_ON_START);

	// private PreparedStatement UPDATE_PEER_DETAILS_ON_STOP = getCassSession().prepare(Constants.UPDATE_PEER_DETAILS_ON_STOP);

	private PreparedStatement SELECT_USER_SESSION_ACTIVITY = null;

	private PreparedStatement SELECT_USER_SESSION_ACTIVITY_BY_SESSION_ID = null;

	private PreparedStatement SELECT_STUDENTS_CLASS_ACTIVITY = null;

	private PreparedStatement UPDATE_REACTION = null;

	private PreparedStatement UPDATE_SESSION_SCORE = null;

	private PreparedStatement SELECT_CLASS_ACTIVITY_DATACUBE = null;

	private PreparedStatement SELECT_ALL_CLASS_ACTIVITY_DATACUBE = null;

	private PreparedStatement INSERT_CLASS_ACTIVITY_DATACUBE = null;

	private PreparedStatement SELECT_TAXONOMY_PARENT_NODE = null;

	private PreparedStatement SELECT_CONTENT_TAXONOMY_ACTIVITY = null;

	private PreparedStatement SELECT_CONTENT_CLASS_TAXONOMY_ACTIVITY = null;

	private PreparedStatement SELECT_TAXONOMY_ACTIVITY_DATACUBE = null;

	private PreparedStatement INSERT_USER_QUESTION_GRADE = null;

	private PreparedStatement SELECT_USER_QUESTION_GRADE_BY_SESSION = null;

	private PreparedStatement SELECT_USER_QUESTION_GRADE_BY_QUESTION = null;

	private PreparedStatement INSERT_TAXONOMY_ACTIVITY_DATACUBE = null;

	public PreparedStatement updateSessionScore() {
		if (UPDATE_SESSION_SCORE == null) {
			UPDATE_SESSION_SCORE = getCassSession().prepare(Constants.UPDATE_SESSION_SCORE);
		}
		return UPDATE_SESSION_SCORE;
	}

	public PreparedStatement insertUserSession() {
		if (INSERT_USER_SESSION == null) {
			INSERT_USER_SESSION = getCassSession().prepare(Constants.INSERT_USER_SESSION);
		}
		return INSERT_USER_SESSION;
	}

	public PreparedStatement insertUserLastSession() {
		if (INSERT_USER_LAST_SESSION == null) {
			INSERT_USER_LAST_SESSION = getCassSession().prepare(Constants.INSERT_USER_LAST_SESSION);
		}
		return INSERT_USER_LAST_SESSION;
	}

	public PreparedStatement insertUserSessionActivity() {
		if (INSERT_USER_SESSION_ACTIVITY == null) {
			INSERT_USER_SESSION_ACTIVITY = getCassSession().prepare(Constants.INSERT_USER_SESSION_ACTIVITY);
		}
		return INSERT_USER_SESSION_ACTIVITY;
	}

	public PreparedStatement insertStudentsClassActivity() {
		if (INSERT_STUDENTS_CLASS_ACTIVITY == null) {
			INSERT_STUDENTS_CLASS_ACTIVITY = getCassSession().prepare(Constants.INSERT_STUDENTS_CLASS_ACTIVITY);
		}
		return INSERT_STUDENTS_CLASS_ACTIVITY;
	}

	public PreparedStatement insertContentTaxonomyActivity() {
		if (INSERT_CONTENT_TAXONOMY_ACTIVITY == null) {
			INSERT_CONTENT_TAXONOMY_ACTIVITY = getCassSession().prepare(Constants.INSERT_CONTENT_TAXONOMY_ACTIVITY);
		}
		return INSERT_CONTENT_TAXONOMY_ACTIVITY;
	}

	public PreparedStatement insertContentClassTaxonomyActivty() {
		if (INSERT_CONTENT_CLASS_TAXONOMY_ACTIVITY == null) {
			INSERT_CONTENT_CLASS_TAXONOMY_ACTIVITY = getCassSession().prepare(Constants.INSERT_CONTENT_CLASS_TAXONOMY_ACTIVITY);
		}
		return INSERT_CONTENT_CLASS_TAXONOMY_ACTIVITY;
	}

	public PreparedStatement insertUserLastLocation() {
		if (INSERT_USER_LOCATION == null) {
			INSERT_USER_LOCATION = getCassSession().prepare(Constants.INSERT_USER_LOCATION);
		}
		return INSERT_USER_LOCATION;
	}

	public PreparedStatement updatePeerCount() {
		if (UPDATE_PEER_COUNT == null) {
			UPDATE_PEER_COUNT = getCassSession().prepare(Constants.UPDATE_PEER_COUNT);
		}
		return UPDATE_PEER_COUNT;
	}

	public PreparedStatement selectUserSessionActivity() {
		if (SELECT_USER_SESSION_ACTIVITY == null) {
			SELECT_USER_SESSION_ACTIVITY = getCassSession().prepare(Constants.SELECT_USER_SESSION_ACTIVITY);
		}
		return SELECT_USER_SESSION_ACTIVITY;
	}

	public PreparedStatement selectUserSessionActivityBySessionId() {
		if (SELECT_USER_SESSION_ACTIVITY_BY_SESSION_ID == null) {
			return getCassSession().prepare(Constants.SELECT_USER_SESSION_ACTIVITY_BY_SESSION_ID);
		}
		return SELECT_USER_SESSION_ACTIVITY_BY_SESSION_ID;
	}

	public PreparedStatement selectStudentClassActivity() {
		if (SELECT_STUDENTS_CLASS_ACTIVITY == null) {
			return getCassSession().prepare(Constants.SELECT_STUDENTS_CLASS_ACTIVITY);
		}
		return SELECT_STUDENTS_CLASS_ACTIVITY;
	}

	public PreparedStatement updateReaction() {
		if (UPDATE_REACTION == null) {
			UPDATE_REACTION = getCassSession().prepare(Constants.UPDATE_REACTION);
		}
		return UPDATE_REACTION;
	}

	public PreparedStatement selectClassActivityDataCube(PreparedStatement statement) {
		if (SELECT_CLASS_ACTIVITY_DATACUBE == null) {
			SELECT_CLASS_ACTIVITY_DATACUBE = getCassSession().prepare(Constants.SELECT_CLASS_ACTIVITY_DATACUBE);
		}
		return SELECT_CLASS_ACTIVITY_DATACUBE;
	}

	public PreparedStatement selectAllClassActivityDataCube() {
		if (SELECT_ALL_CLASS_ACTIVITY_DATACUBE == null) {
			SELECT_ALL_CLASS_ACTIVITY_DATACUBE = getCassSession().prepare(Constants.SELECT_ALL_CLASS_ACTIVITY_DATACUBE);
		}
		return SELECT_ALL_CLASS_ACTIVITY_DATACUBE;
	}

	public PreparedStatement insertClassActivityDataCube() {
		if (INSERT_CLASS_ACTIVITY_DATACUBE == null) {
			return getCassSession().prepare(Constants.INSERT_CLASS_ACTIVITY_DATACUBE);
		}
		return INSERT_CLASS_ACTIVITY_DATACUBE;
	}

	public PreparedStatement selectTaxonomyParentNode() {
		if (SELECT_TAXONOMY_PARENT_NODE == null) {
			SELECT_TAXONOMY_PARENT_NODE = getCassSession().prepare(Constants.SELECT_TAXONOMY_PARENT_NODE);
		}
		return SELECT_TAXONOMY_PARENT_NODE;
	}

	public PreparedStatement selectContentTaxonomyActivity() {
		if (SELECT_CONTENT_TAXONOMY_ACTIVITY == null) {
			SELECT_CONTENT_TAXONOMY_ACTIVITY = getCassSession().prepare(Constants.SELECT_CONTENT_TAXONOMY_ACTIVITY);
		}
		return SELECT_CONTENT_TAXONOMY_ACTIVITY;
	}

	public PreparedStatement selectContentClassTaxonomyActivity() {
		if (SELECT_CONTENT_CLASS_TAXONOMY_ACTIVITY == null) {
			return getCassSession().prepare(Constants.SELECT_CONTENT_CLASS_TAXONOMY_ACTIVITY);
		}
		return SELECT_CONTENT_CLASS_TAXONOMY_ACTIVITY;
	}

	public PreparedStatement selectTaxonomyActivityDataCube() {
		if (SELECT_TAXONOMY_ACTIVITY_DATACUBE == null) {
			SELECT_TAXONOMY_ACTIVITY_DATACUBE = getCassSession().prepare(Constants.SELECT_TAXONOMY_ACTIVITY_DATACUBE);
		}
		return SELECT_TAXONOMY_ACTIVITY_DATACUBE;
	}

	public PreparedStatement insertUserQuestionGrade() {
		if (INSERT_USER_QUESTION_GRADE == null) {
			INSERT_USER_QUESTION_GRADE = getCassSession().prepare(Constants.INSERT_USER_QUESTION_GRADE);
		}
		return INSERT_USER_QUESTION_GRADE;
	}

	public PreparedStatement selectUserQuestionGradeBySession() {
		if (SELECT_USER_QUESTION_GRADE_BY_SESSION == null) {
			SELECT_USER_QUESTION_GRADE_BY_SESSION = getCassSession().prepare(Constants.SELECT_USER_QUESTION_GRADE_BY_SESSION);
		}
		return SELECT_USER_QUESTION_GRADE_BY_SESSION;
	}

	public PreparedStatement selectUserQuestionGradeByQuestion() {
		if (SELECT_USER_QUESTION_GRADE_BY_QUESTION == null) {
			SELECT_USER_QUESTION_GRADE_BY_QUESTION = getCassSession().prepare(Constants.SELECT_USER_QUESTION_GRADE_BY_QUESTION);
		}
		return SELECT_USER_QUESTION_GRADE_BY_QUESTION;
	}

	public PreparedStatement insertTaxonomyActivityDataCube() {
		if (INSERT_TAXONOMY_ACTIVITY_DATACUBE == null) {
			INSERT_TAXONOMY_ACTIVITY_DATACUBE = getCassSession().prepare(Constants.INSERT_TAXONOMY_ACTIVITY_DATACUBE);
		}
		return INSERT_TAXONOMY_ACTIVITY_DATACUBE;
	}
}
