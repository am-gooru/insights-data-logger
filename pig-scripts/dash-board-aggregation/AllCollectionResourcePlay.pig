REGISTER /usr/local/pig/piggybank.jar;

REGISTER /usr/local/pig/mysql-connector-java-5.1.17-bin.jar;

SET job.name 'agg_event_collection_resource_play';

HOURLY_EVENTS = LOAD 'cassandra://$KEYSPACE/agg_event_resource_user_hour' USING CassandraStorage() ;

STAGE_VALUES = FOREACH HOURLY_EVENTS GENERATE date_id,event_id,user_uid,gooru_oid,parent_gooru_oid,total_time_spent_ms.value as total_timespent_ms, organization_uid,event_value,resource_type,total_occurences.value as total_views;

DUPLICATE_STAGE_VALUES = DISTINCT STAGE_VALUES;

CRP_EVENTS = FILTER DUPLICATE_STAGE_VALUES   BY (event_id.value == 10) OR (event_id.value == 181);

GROUPED_RPLAY_VALUES = GROUP CRP_EVENTS by (parent_gooru_oid,gooru_oid,organization_uid);

FLATTENED_RPLAY_VALUES = FOREACH GROUPED_RPLAY_VALUES GENERATE StringConcat((group.parent_gooru_oid.value is null ? 'NA':group.parent_gooru_oid.value),'-',(group.organization_uid.value is null ? 'NA':group.organization_uid.value)) as KEY, FLATTEN(CRP_EVENTS) ,SUM(CRP_EVENTS.total_views) as resource_total_views, SUM(CRP_EVENTS.total_timespent_ms) as resource_total_timespent_ms;

FILTER_FLATTENED_VALUES = FILTER FLATTENED_RPLAY_VALUES BY  parent_gooru_oid.value is not null;

AGG_HOURLY_RPLAY_VALUES = FOREACH FILTER_FLATTENED_VALUES GENERATE StringConcat('R~',parent_gooru_oid.value,'~',gooru_oid.value) as key,TOTUPLE('gooru_oid',gooru_oid.value) as gooru_oid,TOTUPLE(StringConcat('CP-TS~','all'),resource_total_timespent_ms) as resource_total_timespent_ms,TOTUPLE(StringConcat('CP-VC~','all'), resource_total_views) as resource_total_views;

FILETR_VALID_RRPLAY_VALUES = FILTER AGG_HOURLY_RPLAY_VALUES BY key is not null;

STORE FILETR_VALID_RRPLAY_VALUES INTO 'cassandra://$KEYSPACE/agg_event_collection_resource' USING CassandraStorage();
