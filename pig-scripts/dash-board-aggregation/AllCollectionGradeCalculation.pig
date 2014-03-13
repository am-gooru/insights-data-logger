REGISTER /usr/local/pig/piggybank.jar;

REGISTER /usr/local/pig/mysql-connector-java-5.1.17-bin.jar;

SET default_parallel 10;

SET job.name 'Aggregate Grade for Quiz';

HOURLY_EVENTS = LOAD 'cassandra://$KEYSPACE/agg_event_resource_user_hour' USING CassandraStorage() ;

HOURLY_VALUES = FOREACH HOURLY_EVENTS GENERATE date_id,event_id,user_uid,gooru_oid,parent_gooru_oid, organization_uid,attempt_first_status_count.value as attempt_first_status_count,attempt_first_status,total_occurences.value as total_views;

DUPLICATE_VALUES = DISTINCT HOURLY_VALUES;

CQRP_HOURLY_EVENTS = FILTER DUPLICATE_VALUES  BY (event_id.value == 224L AND attempt_first_status.value is not null);

FILTER_CORRECT_ANS = FILTER CQRP_HOURLY_EVENTS BY (attempt_first_status.value == 'correct');

GROUPED_HOURLY_VALUES = GROUP FILTER_CORRECT_ANS by (parent_gooru_oid);

FLATTENED_VALUES = FOREACH GROUPED_HOURLY_VALUES GENERATE group as KEY, FLATTEN(FILTER_CORRECT_ANS) ,COUNT($1) as total_correct_ans;

QUIZ_CF = LOAD 'cassandra://$KEYSPACE/question_count' USING CassandraStorage() ;

QUIZ_DATA = FOREACH QUIZ_CF GENERATE key as quiz_gooru_oid,questionCount.value as total_qn_count;

JOIN_RECORDS = JOIN FLATTENED_VALUES BY KEY.value, QUIZ_DATA BY quiz_gooru_oid;

AGGREGATED_VALUES = FOREACH JOIN_RECORDS GENERATE StringConcat('C~',parent_gooru_oid.value) as key, TOTUPLE('total_correct_ans',total_correct_ans) as total_correct_ans ,TOTUPLE('total_qn_count',QUIZ_DATA::total_qn_count) as total_qn_count;

FILETR_VALID_VALUES = FILTER AGGREGATED_VALUES BY key is not null;

STORE FILETR_VALID_VALUES INTO 'cassandra://$KEYSPACE/agg_event_collection_resource' USING CassandraStorage();
