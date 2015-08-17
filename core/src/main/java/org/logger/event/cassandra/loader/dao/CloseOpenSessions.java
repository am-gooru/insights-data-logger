package org.logger.event.cassandra.loader.dao;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.ednovo.data.model.Event;
import org.json.JSONObject;
import org.logger.event.cassandra.loader.ColumnFamily;
import org.logger.event.cassandra.loader.Constants;
import org.logger.event.cassandra.loader.LoaderConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;

public class CloseOpenSessions implements Runnable, Constants {

	private String gooruUId;

	private BaseCassandraRepoImpl baseCassandraDao;

	private Gson gson = new Gson();

	private DefaultHttpClient httpClient;

	private String restPoint = null;

	private String apiKey = null;
	
	private static Logger logger = LoggerFactory.getLogger(CloseOpenSessions.class);

	public CloseOpenSessions(String gooruUId, BaseCassandraRepoImpl baseCassandraDao) {
		this.gooruUId = gooruUId;
		this.baseCassandraDao = baseCassandraDao;
		this.restPoint = baseCassandraDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), LoaderConstants.ENV_END_POINT.getName(), DEFAULT_COLUMN, 0).getStringValue();
		this.apiKey = baseCassandraDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), _API_KEY, DEFAULT_COLUMN, 0).getStringValue();
	}

	public void run() {
		try {
			ColumnList<String> sessions = baseCassandraDao.readWithKey(ColumnFamily.SESSIONS.getColumnFamily(), (gooruUId + SEPERATOR + SESSIONS), 0);
			for (Column<String> session : sessions) {
				if (session.getStringValue() != null & session.getStringValue().equalsIgnoreCase(START)) {
					ColumnList<String> sessionInfo = baseCassandraDao.readWithKey(ColumnFamily.SESSION_ACTIVITY.getColumnFamily(), session.getName(), 0);
					if (sessionInfo != null) {
						long endTime = sessionInfo.getLongValue(_END_TIME, 0L);
						long totalTimeSpent = (sessionInfo.getLongValue(_END_TIME, 0L) - sessionInfo.getLongValue(_START_TIME, 0L));
						ColumnList<String> eventDetail = baseCassandraDao.readWithKey(ColumnFamily.EVENTDETAIL.getColumnFamily(), sessionInfo.getStringValue(_EVENT_ID, null), 0);
						if (eventDetail != null) {
							baseCassandraDao.saveStringValue(ColumnFamily.SESSIONS.getColumnFamily(), (gooruUId + SEPERATOR + SESSIONS), session.getName(), STOP, 172800);
							String eventField = eventDetail.getStringValue(FIELDS, null);
							JSONObject eventJson = new JSONObject(eventField);
							Event event = gson.fromJson(eventField, Event.class);
							event.setEndTime(endTime);
							JSONObject metrics = new JSONObject(event.getMetrics());
							metrics.put(TOTALTIMEINMS, totalTimeSpent);
							metrics.put(VIEWS_COUNT, 1L);
							JSONObject context = new JSONObject(event.getContext());
							context.put(TYPE, STOP);
							context.put(LOGGED_BY, SYSTEM);
							eventJson.put(METRICS, metrics.toString());
							eventJson.put(CONTEXT, context.toString());
							StringEntity eventEntity = new StringEntity("[" + eventJson + "]");
							HttpPost postRequest = new HttpPost(restPoint+LOGGING_URL+apiKey);
							postRequest.setEntity(eventEntity);
							postRequest.setHeader(CONTENT_TYPE,CONTENT_TYPE_VALUES);
							httpClient = new DefaultHttpClient();
							HttpResponse response = httpClient.execute(postRequest);
							logger.info("Status : {} ", response.getStatusLine().getStatusCode());
							logger.info("System logged Event : {} ", eventJson);
						}
					}
				}
			}
		} catch (Exception e) {
			logger.error("Error while closing events", e);
		}
	}

}
