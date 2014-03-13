REGISTER /usr/local/pig/piggybank.jar;

REGISTER /usr/local/pig/mysql-connector-java-5.1.17-bin.jar;

SET default_parallel 8;

SET job.name 'agg_open_ended_text_resource';

HOURLY_EVENTS = LOAD 'cassandra://$KEYSPACE/agg_event_resource_user_hour' USING CassandraStorage() ;

STAGE_VALUES = FOREACH HOURLY_EVENTS GENERATE date_id,event_id,user_uid,gooru_oid,parent_gooru_oid,total_time_spent_ms.value as total_timespent_ms, organization_uid,event_value,resource_type,open_ended_text;

DUPLICATE_STAGE_VALUES = DISTINCT STAGE_VALUES;

OE_EVENTS = FILTER DUPLICATE_STAGE_VALUES  BY (event_id.value == 227L AND open_ended_text.value is not null);

GROUPED_VALUES = GROUP OE_EVENTS by (gooru_oid,user_uid,open_ended_text,organization_uid);

FLATTENED_VALUES = FOREACH GROUPED_VALUES GENERATE StringConcat((group.gooru_oid.value is null ? 'NA':group.gooru_oid.value),'-',(group.organization_uid.value is null ? 'NA':group.organization_uid.value)) as KEY, FLATTEN(OE_EVENTS);


AGG_HOURLY_VALUES = FOREACH FLATTENED_VALUES GENERATE StringConcat('R~',parent_gooru_oid.value,'~',gooru_oid.value) as key,TOTUPLE('gooru_oid',gooru_oid.value) as gooru_oid,TOTUPLE(StringConcat('OE','~',user_uid.value),OE_EVENTS::open_ended_text) as open_ended_text;

FILETR_VALID_HOUR_VALUES = FILTER AGG_HOURLY_VALUES BY key is not null;

STORE FILETR_VALID_HOUR_VALUES INTO 'cassandra://$KEYSPACE/agg_event_collection_resource' USING CassandraStorage();
