alter table event_detail add city text;
alter table event_detail add state text;
alter table event_detail add country text;

CREATE INDEX city ON event_detail ("city");
CREATE INDEX state ON event_detail ("state");
CREATE INDEX country ON event_detail ("country");

