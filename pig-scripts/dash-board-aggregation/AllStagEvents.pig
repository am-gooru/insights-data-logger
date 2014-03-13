REGISTER /usr/local/pig/piggybank.jar;

REGISTER /usr/local/pig/mysql-connector-java-5.1.17-bin.jar;

SET default_parallel 8;

SET job.name 'agg_event_resource_user_hour';

STAGE = LOAD 'cassandra://$KEYSPACE/stging_event_resource_user' USING CassandraStorage() ;

STAGE_VALUES = FOREACH STAGE GENERATE key, hour_id,date_id,event_id,user_uid,gooru_oid,parent_gooru_oid,total_timespent_ms.value as total_timespent_ms, organization_uid,event_value,resource_type,app_key,app_id,open_ended_text,attempt_first_status;

DISTINCT_STAGE_VALUES = DISTINCT STAGE_VALUES;

GROUPED_VALUES = GROUP DISTINCT_STAGE_VALUES by (hour_id,date_id,event_id,gooru_oid,parent_gooru_oid,user_uid,attempt_first_status,organization_uid,event_value,app_id);

FLATTENED_VALUES = FOREACH GROUPED_VALUES GENERATE StringConcat(group.hour_id.value,'-',group.date_id.value,'-',group.event_id.value,'-' ,(group.gooru_oid.value is null ? 'NA':group.gooru_oid.value),'-',(group.parent_gooru_oid.value is null ? 'NA':group.parent_gooru_oid.value),'-',(group.user_uid.value is null ? 'NA':group.user_uid.value),'-',(group.attempt_first_status.value is null ? 'NA':group.attempt_first_status.value),'-',(group.organization_uid.value is null ? 'NA':group.organization_uid.value),'-',(group.event_value.value is null ? 'NA':group.event_value.value),'-',(group.app_id.value is null ? 'NA':group.app_id.value)) as KEY, FLATTEN(DISTINCT_STAGE_VALUES) , COUNT($1) as total_occurences , SUM(DISTINCT_STAGE_VALUES.total_timespent_ms) as total_timespent_ms;

AGG_HOURLY_VALUES = FOREACH FLATTENED_VALUES GENERATE KEY as key,TOTUPLE('hour_id',hour_id.value) as hour_id,TOTUPLE('date_id',date_id.value) as date_id ,TOTUPLE('event_id',event_id.value) as event_id,TOTUPLE('gooru_oid',gooru_oid.value) as gooru_oid,TOTUPLE('user_uid',user_uid.value) as user_uid,TOTUPLE('event_value',event_value.value) as event_value,TOTUPLE('parent_gooru_oid',parent_gooru_oid.value) as parent_gooru_oid,TOTUPLE('total_time_spent_ms',total_timespent_ms) as total_timespent_ms,TOTUPLE('app_id', app_id.value) as app_id,TOTUPLE('app_key', app_key.value) as app_key,TOTUPLE('total_occurences', total_occurences) as total_occurences,TOTUPLE('organization_uid', organization_uid) as organization_uid,TOTUPLE('open_ended_text', open_ended_text.value) as open_ended_text;

FILETR_VALID_HOUR_VALUES = FILTER AGG_HOURLY_VALUES BY key is not null;

STORE FILETR_VALID_HOUR_VALUES INTO 'cassandra://$KEYSPACE/agg_event_resource_user_hour' USING CassandraStorage();