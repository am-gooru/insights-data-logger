REGISTER /opt/pig/piggybank.jar;

REGISTER /opt/pig/mysql-connector-java-5.1.17-bin.jar;

SET default_parallel 8;

SET job.name 'geo_location';

EVENT = LOAD 'cassandra://event_logger_insights/event_detail' USING CassandraStorage() ;

EVENT_VALUES = FOREACH EVENT GENERATE gooru_uid.value as gooru_uid, country.value as country;

FILTER_EVENT_VALUES = FILTER EVENT_VALUES by gooru_uid != 'ANONYMOUS' AND (gooru_uid is not null) AND (country != '' or country is not null);

GROUP_EVENT_VALUES = GROUP FILTER_EVENT_VALUES by (gooru_uid, country);

EVENT_VALUES1 = FOREACH GROUP_EVENT_VALUES GENERATE group.gooru_uid as gooru_uid, group.country as country, 
COUNT(FILTER_EVENT_VALUES.country) as cnt;

EVENT_VALUES2 = GROUP EVENT_VALUES1 BY (gooru_uid, country) ;

EVENT_VALUES3 = FOREACH EVENT_VALUES2 GENERATE group.gooru_uid, group.country, MAX(EVENT_VALUES1.cnt);

EVENT_VALUES4 = FOREACH EVENT_VALUES3 GENERATE TOTUPLE('gooru_uid', gooru_uid) as key, TOTUPLE('gooru_uid', gooru_uid) as gooru_uid, TOTUPLE('country', country) as country;

STORE EVENT_VALUES4 INTO 'cassandra://event_logger_insights/country' USING CassandraStorage();

