REGISTER /usr/local/pig/piggybank.jar;

REGISTER /usr/local/pig/mysql-connector-java-5.1.17-bin.jar;

SET default_parallel 8;

SET job.name 'Staging-Question Resource play';

STAGE = LOAD 'cassandra://$KEYSPACE/stging_event_resource_user' USING CassandraStorage() ;

STAGE_VALUES = FOREACH STAGE GENERATE key, hour_id,date_id,event_id,user_uid,gooru_oid,parent_gooru_oid,total_timespent_ms.value as total_timespent_ms, organization_uid,attempt_first_status;

DISTINCT_STAGE_VALUES = DISTINCT STAGE_VALUES;

CQRP_EVENTS = FILTER DISTINCT_STAGE_VALUES   BY (event_id.value == 224);

GROUPED_VALUES = GROUP CQRP_EVENTS by (hour_id,date_id,event_id,gooru_oid,parent_gooru_oid,user_uid,attempt_first_status,organization_uid);

FLATTENED_VALUES = FOREACH GROUPED_VALUES GENERATE StringConcat(group.hour_id.value,'-',group.date_id.value,'-',group.event_id.value,'-' ,(group.gooru_oid.value is null ? 'NA':group.gooru_oid.value),'-' ,(group.parent_gooru_oid.value is null ? 'NA':group.parent_gooru_oid.value),'-',(group.user_uid.value is null ? 'NA':group.user_uid.value),'-' ,(group.attempt_first_status.value is null ? 'NA':group.attempt_first_status.value),'-',(group.organization_uid.value is null ? 'NA':group.organization_uid.value)) as KEY, FLATTEN(CQRP_EVENTS) , COUNT($1) as attempt_first_status_count ;

AGG_HOURLY_VALUES = FOREACH FLATTENED_VALUES GENERATE KEY as key,TOTUPLE('hour_id',hour_id.value) as hour_id,TOTUPLE('date_id',date_id.value) as date_id ,TOTUPLE('event_id',event_id.value) as event_id,TOTUPLE('gooru_oid',gooru_oid.value) as gooru_oid,TOTUPLE('user_uid',user_uid.value) as user_uid,TOTUPLE('parent_gooru_oid',parent_gooru_oid.value) as parent_gooru_oid,TOTUPLE('attempt_first_status',attempt_first_status.value) as attempt_first_status,TOTUPLE('attempt_first_status_count', attempt_first_status_count) as attempt_first_status_count,TOTUPLE('organization_uid', organization_uid) as organization_uid;

FILETR_VALID_HOUR_VALUES = FILTER AGG_HOURLY_VALUES BY key is not null;

STORE FILETR_VALID_HOUR_VALUES INTO 'cassandra://$KEYSPACE/agg_event_resource_user_hour' USING CassandraStorage();
