// You can run this command in cql client
// bin/cqlsh < {file path}/002_cluster_2_shemas.cql 
 
CREATE KEYSPACE IF NOT EXISTS event_logger_insights WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'}  AND durable_writes = true;

CREATE TABLE IF NOT EXISTS event_logger_insights.app_api_key (
    key text PRIMARY KEY,
    appName text,
    appOid text,
    endPoint text,
    pushIntervalMs text
);

INSERT INTO event_logger_insights.app_api_key(key,appName,appOid,endPoint,pushIntervalMs)VALUES('33b9ad34-1a0c-43ba-bb9c-4784abe07110','QA','QA','http://qa.gooru.org/api','1000');

INSERT INTO event_logger_insights.app_api_key(key,appName,appOid,endPoint,pushIntervalMs)VALUES('b6b82f4d-0e6e-4ad5-96d9-30849cf17727','gooru','gooru','http://www.gooru.org/api','1000');

CREATE TABLE IF NOT EXISTS event_logger_insights.class_activity_datacube (
    row_key text,
    collection_type text,
    user_uid text,
    leaf_node text,
    attempt_status text,
    completed_count bigint,
    reaction bigint,
    score bigint,
    time_spent bigint,
    views bigint,
    PRIMARY KEY (row_key, collection_type, user_uid, leaf_node)
);
 
 CREATE TABLE IF NOT EXISTS event_logger_insights.class_activity_peer_counts (
    row_key text,
    leaf_gooru_oid text,
    collection_type text,
    active_peer_count counter,
    left_peer_count counter,
    PRIMARY KEY (row_key, leaf_gooru_oid, collection_type)
);

CREATE TABLE IF NOT EXISTS event_logger_insights.class_activity_peer_detail (
    row_key text,
    leaf_gooru_oid text,
    collection_type text,
    active_peers set<text>,
    left_peers set<text>,
    PRIMARY KEY (row_key, leaf_gooru_oid, collection_type)
);

CREATE TABLE IF NOT EXISTS event_logger_insights.content_class_taxonomy_activity (
    class_uid text,
    user_uid text,
    resource_type text,
    subject_id text,
    course_id text,
    domain_id text,
    standards_id text,
    learning_targets_id text,
    gooru_oid text,
    question_type text,
    score bigint,
    time_spent bigint,
    views bigint,
    PRIMARY KEY (class_uid, user_uid, resource_type, subject_id, course_id, domain_id, standards_id, learning_targets_id, gooru_oid)
);

CREATE TABLE IF NOT EXISTS event_logger_insights.content_taxonomy_activity (
    user_uid text,
    subject_id text,
    course_id text,
    domain_id text,
    standards_id text,
    learning_targets_id text,
    gooru_oid text,
    question_type text,
    resource_type text,
    score bigint,
    time_spent bigint,
    views bigint,
    PRIMARY KEY (user_uid, subject_id, course_id, domain_id, standards_id, learning_targets_id, gooru_oid)
);

CREATE TABLE IF NOT EXISTS event_logger_insights.statistical_data (
    clustering_key text,
    metrics_name text,
    metrics_value counter,
    PRIMARY KEY (clustering_key, metrics_name)
);

CREATE TABLE IF NOT EXISTS event_logger_insights.student_location (
    class_uid text,
    user_uid text,
    collection_type text,
    collection_uid text,
    course_uid text,
    lesson_uid text,
    resource_uid text,
    session_time bigint,
    unit_uid text,
    PRIMARY KEY (class_uid, user_uid)
);

CREATE TABLE IF NOT EXISTS event_logger_insights.students_class_activity (
    class_uid text,
    user_uid text,
    collection_type text,
    course_uid text,
    unit_uid text,
    lesson_uid text,
    collection_uid text,
    attempt_status text,
    reaction bigint,
    score bigint,
    time_spent bigint,
    views bigint,
    PRIMARY KEY (class_uid, user_uid, collection_type, course_uid, unit_uid, lesson_uid, collection_uid)
);

CREATE TABLE IF NOT EXISTS event_logger_insights.taxonomy_child_count (
    row_key text PRIMARY KEY,
    child_count bigint,
    question_count bigint,
    type_name text
);

CREATE TABLE IF NOT EXISTS event_logger_insights.taxonomy_parent_node (
    row_key text PRIMARY KEY,
    course_id text,
    domain_id text,
    learning_targets_id text,
    standards_id text,
    subject_id text
);

CREATE TABLE IF NOT EXISTS event_logger_insights.user_class_collection_last_sessions (
    class_uid text,
    course_uid text,
    unit_uid text,
    lesson_uid text,
    collection_uid text,
    user_uid text,
    session_id text,
    PRIMARY KEY (class_uid, course_uid, unit_uid, lesson_uid, collection_uid, user_uid)
);

CREATE TABLE IF NOT EXISTS event_logger_insights.user_question_grade (
    teacher_id text,
    user_id text,
    session_id text,
    question_id text,
    score bigint,
    PRIMARY KEY (teacher_id, user_id, session_id, question_id)
);

CREATE TABLE IF NOT EXISTS event_logger_insights.user_session_activity (
    session_id text,
    gooru_oid text,
    collection_item_id text,
    answer_object text,
    answer_status text,
    attempts bigint,
    collection_type text,
    event_type text,
    parent_event_id text,
    question_type text,
    reaction bigint,
    resource_type text,
    score bigint,
    time_spent bigint,
    views bigint,
    PRIMARY KEY (session_id, gooru_oid, collection_item_id)
);

CREATE TABLE IF NOT EXISTS event_logger_insights.user_sessions (
    user_uid text,
    collection_uid text,
    collection_type text,
    class_uid text,
    course_uid text,
    unit_uid text,
    lesson_uid text,
    session_id text,
    event_time bigint,
    event_type text,
    PRIMARY KEY (user_uid, collection_uid, collection_type, class_uid, course_uid, unit_uid, lesson_uid, session_id)
);

CREATE TABLE IF NOT EXISTS event_logger_insights.class_content_count (
    row_key text PRIMARY KEY,
    assessment_count bigint,
    collection_count bigint,
    type_name text
);

CREATE TABLE IF NOT EXISTS event_logger_insights.class_members (
    class_id text,
    members set<text>,
    PRIMARY KEY (class_id)
);

CREATE TABLE IF NOT EXISTS event_logger_insights.content_authorized_users (
    gooru_oid text,
    creator_uid text,
    collaborators set<text>,
    PRIMARY KEY (gooru_oid)
);