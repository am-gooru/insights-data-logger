REGISTER /opt/pig/piggybank.jar;

REGISTER /opt/pig/mysql-connector-java-5.1.17-bin.jar;

SET default_parallel 8;

SET job.name 'geo_location';

EVENT = LOAD 'cassandra://event_logger_insights/event_detail' USING CassandraStorage() ;

EVENT_VALUES = FOREACH EVENT GENERATE gooru_uid.value as gooru_uid, city.value as city;

FILTER_EVENT_VALUES = FILTER EVENT_VALUES by gooru_uid != 'ANONYMOUS' AND (gooru_uid is not null) AND (city != '' or city is not null);

GROUP_EVENT_VALUES = GROUP FILTER_EVENT_VALUES by (gooru_uid, city);

EVENT_VALUES1 = FOREACH GROUP_EVENT_VALUES GENERATE group.gooru_uid as gooru_uid, group.city as city, COUNT(FILTER_EVENT_VALUES.city) as cnt;

EVENT_VALUES2 = GROUP EVENT_VALUES1 BY (gooru_uid, city) ;

EVENT_VALUES3 = FOREACH EVENT_VALUES2 GENERATE group.gooru_uid, group.city, MAX(EVENT_VALUES1.cnt);

EVENT_VALUES4 = FOREACH EVENT_VALUES3 GENERATE TOTUPLE('gooru_uid', gooru_uid) as key, TOTUPLE('gooru_uid', gooru_uid) as gooru_uid, TOTUPLE('city', city) as city;

STORE EVENT_VALUES4 INTO 'cassandra://event_logger_insights/city' USING CassandraStorage();

