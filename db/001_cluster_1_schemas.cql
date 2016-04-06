// You can run this command in cql client
// bin/cqlsh < {file path}/001_cluster_1_shemas.cql 

CREATE KEYSPACE IF NOT EXISTS event_logger_insights WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'}  AND durable_writes = true;

CREATE TABLE IF NOT EXISTS event_logger_insights.events_timeline (
    event_time text,
    event_id text,
    PRIMARY KEY (event_time, event_id)
);

CREATE TABLE IF NOT EXISTS event_logger_insights.events (
    event_id text PRIMARY KEY,
    fields text
);

CREATE TABLE IF NOT EXISTS event_logger_insights.stat_publisher_queue (
    metrics_name text,
    gooru_oid text,
    event_time bigint,
    PRIMARY KEY (metrics_name, gooru_oid)
)