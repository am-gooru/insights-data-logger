package org.logger.event.cassandra.loader;

import org.logger.event.cassandra.loader.dao.BaseDAOCassandraImpl;

import com.datastax.driver.core.PreparedStatement;

public class PreparedStatementQueries extends BaseDAOCassandraImpl{
	
	public static final PreparedStatement INSERT_USER_SESSION = getCassSession().prepare("INSERT INTO user_sessions(user_uid,collection_uid,collection_type,class_uid,course_uid,unit_uid,lesson_uid,event_time,event_type,session_id)VALUES(?,?,?,?,?,?,?,?,?,?);");
	
	public static final PreparedStatement INSERT_USER_LAST_SESSION = getCassSession().prepare("INSERT INTO user_class_collection_last_sessions(class_uid,course_uid,unit_uid,lesson_uid,collection_uid,user_uid,session_id)VALUES(?,?,?,?,?,?,?);");
	
	public static final PreparedStatement INSERT_USER_SESSION_ACTIVITY = getCassSession().prepare("INSERT INTO user_session_activity(session_id,gooru_oid,collection_item_id,answer_object,attempts,collection_type,resource_type,question_type,answer_status,event_type,parent_event_id,reaction,score,time_spent,views)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);");	
	
	public static final PreparedStatement INSERT_STUDENTS_CLASS_ACTIVITY = getCassSession().prepare("INSERT INTO students_class_activity(class_uid,course_uid,unit_uid,lesson_uid,collection_uid,user_uid,collection_type,attempt_status,score,time_spent,views,reaction)VALUES(?,?,?,?,?,?,?,?,?,?,?,?);");
	
	public static final PreparedStatement INSERT_CONTENT_TAXONOMY_ACTIVITY = getCassSession().prepare("INSERT INTO content_taxonomy_activity (user_uid,subject_id,course_id,domain_id,sub_domain_id,standards_id,learning_targets_id,gooru_oid,class_uid,resource_type,question_type,score,time_spent,views)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?);");	
	
	public static final PreparedStatement INSERT_CONTENT_CLASS_TAXONOMY_ACTIVITY = getCassSession().prepare("INSERT INTO content_class_taxonomy_activity (user_uid,class_uid,subject_id,course_id,domain_id,sub_domain_id,standards_id,learning_targets_id,gooru_oid,class_uid,resource_type,question_type,score,time_spent,views)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);");
	
	public static final PreparedStatement INSERT_USER_LOCATION = getCassSession().prepare("INSERT INTO student_location(user_uid,class_uid,course_uid,unit_uid,lesson_uid,collection_uid,collection_type,resource_uid,session_time)VALUES(?,?,?,?,?,?,?,?,?);");
	
	public static final PreparedStatement UPDATE_PEER_COUNT = getCassSession().prepare("UPDATE class_activity_peer_counts SET left_peer_count=left_peer_count+? , active_peer_count=active_peer_count+? WHERE row_key = getCassSession().prepare(? AND leaf_gooru_oid = getCassSession().prepare(? AND collection_type = getCassSession().prepare(? ;");
	
	public static final PreparedStatement UPDATE_PEER_DETAILS_ON_START = getCassSession().prepare("UPDATE class_activity_peer_detail SET active_peers = getCassSession().prepare(active_peers + {'"+Constants.GOORUID+"'} , left_peers = getCassSession().prepare(left_peers - {'"+Constants.GOORUID+"'} WHERE row_key = getCassSession().prepare(? AND leaf_gooru_oid = getCassSession().prepare(? and collection_type = getCassSession().prepare(?;");
	
	public static final PreparedStatement UPDATE_PEER_DETAILS_ON_STOP = getCassSession().prepare("UPDATE class_activity_peer_detail SET active_peers = getCassSession().prepare(active_peers - {'"+Constants.GOORUID+"'} , left_peers = getCassSession().prepare(left_peers + {'"+Constants.GOORUID+"'} WHERE row_key = getCassSession().prepare(? AND leaf_gooru_oid = getCassSession().prepare(? and collection_type = getCassSession().prepare(?;");
	
	public static final PreparedStatement SELECT_USER_SESSION_ACTIVITY = getCassSession().prepare("SELECT * FROM user_session_activity WHERE session_id = getCassSession().prepare(? AND gooru_oid = getCassSession().prepare(? AND collection_item_id = getCassSession().prepare(?;");
	
	public static final PreparedStatement SELECT_USER_SESSION_ACTIVITY_BY_SESSION_ID = getCassSession().prepare("SELECT * FROM user_session_activity WHERE session_id = getCassSession().prepare(?;");
	
	public static final PreparedStatement SELECT_STUDENTS_CLASS_ACTIVITY = getCassSession().prepare("SELECT * FROM students_class_activity WHERE class_uid = getCassSession().prepare(? AND user_uid = getCassSession().prepare(? AND collection_type = getCassSession().prepare(? AND course_uid = getCassSession().prepare(? AND unit_uid = getCassSession().prepare(? AND lesson_uid = getCassSession().prepare(? AND collection_uid = getCassSession().prepare(?;");
	
	public static final PreparedStatement UPDATE_REACTION = getCassSession().prepare("INSERT INTO user_session_activity(session_id,gooru_oid,collection_item_id,reaction)VALUES(?,?,?,?);");
	
	public static final PreparedStatement UPDATE_SESSION_SCORE = getCassSession().prepare("INSERT INTO user_session_activity(session_id,gooru_oid,collection_item_id,attempt_status,score)VALUES(?,?,?,?,?);");
	
	public static final PreparedStatement SELECT_CLASS_ACTIVITY_DATACUBE = getCassSession().prepare("SELECT * FROM class_activity_datacube WHERE row_key = getCassSession().prepare(? AND leaf_node = getCassSession().prepare(? AND collection_type = getCassSession().prepare(? AND user_uid = getCassSession().prepare(?;");
	
	public static final PreparedStatement SELECT_ALL_CLASS_ACTIVITY_DATACUBE = getCassSession().prepare("SELECT * FROM class_activity_datacube WHERE row_key = getCassSession().prepare(? AND collection_type = getCassSession().prepare(? AND user_uid = getCassSession().prepare(?;");
	
	public static final PreparedStatement INSERT_CLASS_ACTIVITY_DATACUBE = getCassSession().prepare("INSERT INTO class_activity_datacube(row_key,leaf_node,collection_type,user_uid,score,time_spent,views,reaction,completed_count)VALUES(?,?,?,?,?,?,?,?,?);");
	
	public static final PreparedStatement SELECT_TAXONOMY_PARENT_NODE = getCassSession().prepare("SELECT * FROM taxonomy_parent_node where row_key = getCassSession().prepare(?;");
	
	public static final PreparedStatement SELECT_CONTENT_TAXONOMY_ACTIVITY = getCassSession().prepare("SELECT * FROM content_taxonomy_activity WHERE user_uid = getCassSession().prepare(? resource_type = getCassSession().prepare(? AND subject_id = getCassSession().prepare(? AND course_id = getCassSession().prepare(? AND domain_id = getCassSession().prepare(? AND standards_id = getCassSession().prepare(? AND learning_targets_id = getCassSession().prepare(? AND gooru_oid = getCassSession().prepare(?;");
	
	public static final PreparedStatement SELECT_CONTENT_CLASS_TAXONOMY_ACTIVITY = getCassSession().prepare("SELECT * FROM content_class_taxonomy_activity WHERE class_uid = getCassSession().prepare(? AND user_uid = getCassSession().prepare(? AND resource_type = getCassSession().prepare(? AND subject_id = getCassSession().prepare(? AND course_id = getCassSession().prepare(? AND domain_id = getCassSession().prepare(? AND standards_id = getCassSession().prepare(? AND learning_targets_id = getCassSession().prepare(? AND gooru_oid = getCassSession().prepare(?;");
	
	public static final PreparedStatement SELECT_TAXONOMY_ACTIVITY_DATACUBE = getCassSession().prepare("SELECT * FROM taxonomy_activity_datacube WHERE row_key = getCassSession().prepare(? leaf_node = getCassSession().prepare(?;");
	
	public static final PreparedStatement INSERT_USER_QUESTION_GRADE = getCassSession().prepare("INSERT INTO user_question_grade(teacher_id,user_id,session_id,question_id,score)VALUES(?,?,?,?,?);");
	
	public static final PreparedStatement SELECT_USER_QUESTION_GRADE_BY_SESSION = getCassSession().prepare("SELECT * FROM user_question_grade WHERE teacher_id = getCassSession().prepare(? AND user_id = getCassSession().prepare(? AND session_id = getCassSession().prepare(?;");
	
	public static final PreparedStatement SELECT_USER_QUESTION_GRADE_BY_QUESTION = getCassSession().prepare("SELECT * FROM user_question_grade WHERE teacher_id = getCassSession().prepare(? AND user_id = getCassSession().prepare(? AND session_id = getCassSession().prepare(? AND question_id = getCassSession().prepare(?;");
	
	public static final PreparedStatement INSERT_TAXONOMY_ACTIVITY_DATACUBE = getCassSession().prepare("INSERT INTO taxonomy_activity_datacube(row_key, leaf_node, views, attempts, resource_timespent, question_timespent, score)VALUES(?,?,?,?,?,?,?);");
}
