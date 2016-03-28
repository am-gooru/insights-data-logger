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