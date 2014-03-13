REGISTER /usr/local/pig/piggybank.jar;

REGISTER /usr/local/pig/mysql-connector-java-5.1.17-bin.jar;

SET default_parallel 8;

SET job.name 'agg_event_collection_play';

HOURLY_EVENTS = LOAD 'cassandra://$KEYSPACE/agg_event_resource_user_hour' USING CassandraStorage() ;

STAGE_VALUES = FOREACH HOURLY_EVENTS GENERATE date_id,event_id,user_uid,gooru_oid,parent_gooru_oid,total_time_spent_ms.value as total_timespent_ms, organization_uid,event_value,resource_type,total_occurences.value as total_views;

DUPLICATE_STAGE_VALUES = DISTINCT STAGE_VALUES;

CP_EVENTS = FILTER DUPLICATE_STAGE_VALUES  BY (event_id.value == 7) OR (event_id.value == 56);

GROUPED_VALUES = GROUP CP_EVENTS by (gooru_oid,organization_uid);

FLATTENED_VALUES = FOREACH GROUPED_VALUES GENERATE StringConcat((group.gooru_oid.value is null ? 'NA':group.gooru_oid.value),'-',(group.organization_uid.value is null ? 'NA':group.organization_uid.value)) as KEY, FLATTEN(CP_EVENTS) , SUM(CP_EVENTS.total_views) as total_views, SUM(CP_EVENTS.total_timespent_ms) as total_timespent_ms;

AGG_HOURLY_VALUES = FOREACH FLATTENED_VALUES GENERATE StringConcat('C~',gooru_oid.value) as key,TOTUPLE('gooru_oid',gooru_oid.value) as gooru_oid,TOTUPLE(StringConcat('TS~','all'),total_timespent_ms) as total_timespent_ms,TOTUPLE(StringConcat('VC~','all'), total_views) as total_views;

FILETR_VALID_HOUR_VALUES = FILTER AGG_HOURLY_VALUES BY key is not null;

STORE FILETR_VALID_HOUR_VALUES INTO 'cassandra://$KEYSPACE/agg_event_collection_resource' USING CassandraStorage();
