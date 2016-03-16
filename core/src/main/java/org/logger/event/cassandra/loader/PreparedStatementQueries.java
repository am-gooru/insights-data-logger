package org.logger.event.cassandra.loader;

import org.logger.event.cassandra.loader.dao.BaseDAOCassandraImpl;
import org.logger.event.datasource.infra.CassandraClient;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

public final  class PreparedStatementQueries {
	
	private  final static Session SESSION = CassandraClient.getCassSession();

	public static final PreparedStatement INSERT_USER_SESSION = SESSION.prepare("INSERT INTO user_sessions(user_uid,collection_uid,collection_type,class_uid,course_uid,unit_uid,lesson_uid,event_time,event_type,session_id)VALUES(?,?,?,?,?,?,?,?,?,?);");
	
	public static final PreparedStatement INSERT_USER_LAST_SESSION = SESSION.prepare("INSERT INTO user_class_collection_last_sessions(class_uid,course_uid,unit_uid,lesson_uid,collection_uid,user_uid,session_id)VALUES(?,?,?,?,?,?,?);");
	
	public static final PreparedStatement INSERT_USER_SESSION_ACTIVITY = SESSION.prepare("INSERT INTO user_session_activity(session_id,gooru_oid,collection_item_id,answer_object,attempts,collection_type,resource_type,question_type,answer_status,event_type,parent_event_id,reaction,score,time_spent,views)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);");	
	
	public static final PreparedStatement INSERT_STUDENTS_CLASS_ACTIVITY = SESSION.prepare("INSERT INTO students_class_activity(class_uid,course_uid,unit_uid,lesson_uid,collection_uid,user_uid,collection_type,attempt_status,score,time_spent,views,reaction)VALUES(?,?,?,?,?,?,?,?,?,?,?,?);");
	
	public static final PreparedStatement INSERT_CONTENT_TAXONOMY_ACTIVITY = SESSION.prepare("INSERT INTO content_taxonomy_activity (user_uid,subject_id,course_id,domain_id,sub_domain_id,standards_id,learning_targets_id,gooru_oid,class_uid,resource_type,question_type,score,time_spent,views)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?);");	
	
	public static final PreparedStatement INSERT_CONTENT_CLASS_TAXONOMY_ACTIVITY = SESSION.prepare("INSERT INTO content_class_taxonomy_activity (user_uid,class_uid,subject_id,course_id,domain_id,sub_domain_id,standards_id,learning_targets_id,gooru_oid,class_uid,resource_type,question_type,score,time_spent,views)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);");
	
	public static final PreparedStatement INSERT_USER_LOCATION = SESSION.prepare("INSERT INTO student_location(user_uid,class_uid,course_uid,unit_uid,lesson_uid,collection_uid,collection_type,resource_uid,session_time)VALUES(?,?,?,?,?,?,?,?,?);");
	
	public static final PreparedStatement UPDATE_PEER_COUNT = SESSION.prepare("UPDATE class_activity_peer_counts SET left_peer_count=left_peer_count+? , active_peer_count=active_peer_count+? WHERE row_key = SESSION.prepare(? AND leaf_gooru_oid = SESSION.prepare(? AND collection_type = SESSION.prepare(? ;");
	
	public static final PreparedStatement UPDATE_PEER_DETAILS_ON_START = SESSION.prepare("UPDATE class_activity_peer_detail SET active_peers = SESSION.prepare(active_peers + {'"+Constants.GOORUID+"'} , left_peers = SESSION.prepare(left_peers - {'"+Constants.GOORUID+"'} WHERE row_key = SESSION.prepare(? AND leaf_gooru_oid = SESSION.prepare(? and collection_type = SESSION.prepare(?;");
	
	public static final PreparedStatement UPDATE_PEER_DETAILS_ON_STOP = SESSION.prepare("UPDATE class_activity_peer_detail SET active_peers = SESSION.prepare(active_peers - {'"+Constants.GOORUID+"'} , left_peers = SESSION.prepare(left_peers + {'"+Constants.GOORUID+"'} WHERE row_key = SESSION.prepare(? AND leaf_gooru_oid = SESSION.prepare(? and collection_type = SESSION.prepare(?;");
	
	public static final PreparedStatement SELECT_USER_SESSION_ACTIVITY = SESSION.prepare("SELECT * FROM user_session_activity WHERE session_id = SESSION.prepare(? AND gooru_oid = SESSION.prepare(? AND collection_item_id = SESSION.prepare(?;");
	
	public static final PreparedStatement SELECT_USER_SESSION_ACTIVITY_BY_SESSION_ID = SESSION.prepare("SELECT * FROM user_session_activity WHERE session_id = SESSION.prepare(?;");
	
	public static final PreparedStatement SELECT_STUDENTS_CLASS_ACTIVITY = SESSION.prepare("SELECT * FROM students_class_activity WHERE class_uid = SESSION.prepare(? AND user_uid = SESSION.prepare(? AND collection_type = SESSION.prepare(? AND course_uid = SESSION.prepare(? AND unit_uid = SESSION.prepare(? AND lesson_uid = SESSION.prepare(? AND collection_uid = SESSION.prepare(?;");
	
	public static final PreparedStatement UPDATE_REACTION = SESSION.prepare("INSERT INTO user_session_activity(session_id,gooru_oid,collection_item_id,reaction)VALUES(?,?,?,?);");
	
	public static final PreparedStatement UPDATE_SESSION_SCORE = SESSION.prepare("INSERT INTO user_session_activity(session_id,gooru_oid,collection_item_id,attempt_status,score)VALUES(?,?,?,?,?);");
	
	public static final PreparedStatement SELECT_CLASS_ACTIVITY_DATACUBE = SESSION.prepare("SELECT * FROM class_activity_datacube WHERE row_key = SESSION.prepare(? AND leaf_node = SESSION.prepare(? AND collection_type = SESSION.prepare(? AND user_uid = SESSION.prepare(?;");
	
	public static final PreparedStatement SELECT_ALL_CLASS_ACTIVITY_DATACUBE = SESSION.prepare("SELECT * FROM class_activity_datacube WHERE row_key = SESSION.prepare(? AND collection_type = SESSION.prepare(? AND user_uid = SESSION.prepare(?;");
	
	public static final PreparedStatement INSERT_CLASS_ACTIVITY_DATACUBE = SESSION.prepare("INSERT INTO class_activity_datacube(row_key,leaf_node,collection_type,user_uid,score,time_spent,views,reaction,completed_count)VALUES(?,?,?,?,?,?,?,?,?);");
	
	public static final PreparedStatement SELECT_TAXONOMY_PARENT_NODE = SESSION.prepare("SELECT * FROM taxonomy_parent_node where row_key = SESSION.prepare(?;");
	
	public static final PreparedStatement SELECT_CONTENT_TAXONOMY_ACTIVITY = SESSION.prepare("SELECT * FROM content_taxonomy_activity WHERE user_uid = SESSION.prepare(? resource_type = SESSION.prepare(? AND subject_id = SESSION.prepare(? AND course_id = SESSION.prepare(? AND domain_id = SESSION.prepare(? AND standards_id = SESSION.prepare(? AND learning_targets_id = SESSION.prepare(? AND gooru_oid = SESSION.prepare(?;");
	
	public static final PreparedStatement SELECT_CONTENT_CLASS_TAXONOMY_ACTIVITY = SESSION.prepare("SELECT * FROM content_class_taxonomy_activity WHERE class_uid = SESSION.prepare(? AND user_uid = SESSION.prepare(? AND resource_type = SESSION.prepare(? AND subject_id = SESSION.prepare(? AND course_id = SESSION.prepare(? AND domain_id = SESSION.prepare(? AND standards_id = SESSION.prepare(? AND learning_targets_id = SESSION.prepare(? AND gooru_oid = SESSION.prepare(?;");
	
	public static final PreparedStatement SELECT_TAXONOMY_ACTIVITY_DATACUBE = SESSION.prepare("SELECT * FROM taxonomy_activity_datacube WHERE row_key = SESSION.prepare(? leaf_node = SESSION.prepare(?;");
	
	public static final PreparedStatement INSERT_USER_QUESTION_GRADE = SESSION.prepare("INSERT INTO user_question_grade(teacher_id,user_id,session_id,question_id,score)VALUES(?,?,?,?,?);");
	
	public static final PreparedStatement SELECT_USER_QUESTION_GRADE_BY_SESSION = SESSION.prepare("SELECT * FROM user_question_grade WHERE teacher_id = SESSION.prepare(? AND user_id = SESSION.prepare(? AND session_id = SESSION.prepare(?;");
	
	public static final PreparedStatement SELECT_USER_QUESTION_GRADE_BY_QUESTION = SESSION.prepare("SELECT * FROM user_question_grade WHERE teacher_id = SESSION.prepare(? AND user_id = SESSION.prepare(? AND session_id = SESSION.prepare(? AND question_id = SESSION.prepare(?;");
	
	public static final PreparedStatement INSERT_TAXONOMY_ACTIVITY_DATACUBE = SESSION.prepare("INSERT INTO taxonomy_activity_datacube(row_key, leaf_node, views, attempts, resource_timespent, question_timespent, score)VALUES(?,?,?,?,?,?,?);");
}
