package org.logger.event.cassandra.loader.dao;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.ednovo.data.model.Event;
import org.json.JSONObject;
import org.logger.event.cassandra.loader.ColumnFamilySet;
import org.logger.event.cassandra.loader.Constants;
import org.logger.event.cassandra.loader.LoaderConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;

public class CloseOpenSessions implements Runnable {

	private String gooruUId;
	
	private String sessionId;
	
	private BaseCassandraRepo baseCassandraDao;

	private Gson gson = new Gson();

	private DefaultHttpClient httpClient;

	private String restPoint = null;

	private String apiKey = null;
	
	private static Logger logger = LoggerFactory.getLogger(CloseOpenSessions.class);

	public CloseOpenSessions(String gooruUId, String sessionId, BaseCassandraRepo baseCassandraDao) {
		this.gooruUId = gooruUId;
		this.sessionId = sessionId;
		this.baseCassandraDao = baseCassandraDao;
		this.restPoint = baseCassandraDao.readWithKeyColumn(ColumnFamilySet.CONFIGSETTINGS.getColumnFamily(), LoaderConstants.ENV_END_POINT.getName(), Constants.DEFAULT_COLUMN).getStringValue();
		this.apiKey = baseCassandraDao.readWithKeyColumn(ColumnFamilySet.CONFIGSETTINGS.getColumnFamily(), Constants._API_KEY, Constants.DEFAULT_COLUMN).getStringValue();
	}

	public void run() {
		try {
			ColumnList<String> sessions = baseCassandraDao.readWithKey(ColumnFamilySet.SESSIONS.getColumnFamily(), (gooruUId + Constants.SEPERATOR + Constants.SESSIONS));
			for (Column<String> session : sessions) {
				if (session.getStringValue() != null && !sessionId.equalsIgnoreCase(session.getName()) && session.getStringValue().equalsIgnoreCase(Constants.START)) {
					ColumnList<String> sessionInfo = baseCassandraDao.readWithKey(ColumnFamilySet.SESSION_ACTIVITY.getColumnFamily(), session.getName());
					if (sessionInfo != null) {
						logger.info("Closing session : {}",session.getName());
						long endTime = sessionInfo.getLongValue(Constants._END_TIME, 0L);
						long totalTimeSpent = (sessionInfo.getLongValue(Constants._END_TIME, 0L) - sessionInfo.getLongValue(Constants._START_TIME, 0L));
						ColumnList<String> eventDetail = baseCassandraDao.readWithKey(ColumnFamilySet.EVENTDETAIL.getColumnFamily(), sessionInfo.getStringValue(Constants._EVENT_ID, null));
						if (eventDetail != null) {
							baseCassandraDao.saveStringValue(ColumnFamilySet.SESSIONS.getColumnFamily(), (gooruUId + Constants.SEPERATOR + Constants.SESSIONS), session.getName(), Constants.STOP, 1);
							String eventField = eventDetail.getStringValue(Constants.FIELDS, null);
							JSONObject eventJson = new JSONObject(eventField);
							Event event = gson.fromJson(eventField, Event.class);
							event.setEndTime(endTime);
							JSONObject metrics = new JSONObject(event.getMetrics());
							metrics.put(Constants.TOTALTIMEINMS, totalTimeSpent);
							metrics.put(Constants.VIEWS_COUNT, 1L);
							JSONObject context = new JSONObject(event.getContext());
							context.put(Constants.TYPE, Constants.STOP);
							context.put(Constants.LOGGED_BY, Constants.SYSTEM);
							eventJson.put(Constants.METRICS, metrics.toString());
							eventJson.put(Constants.CONTEXT, context.toString());
							StringEntity eventEntity = new StringEntity("[" + eventJson + "]");
							HttpPost postRequest = new HttpPost(restPoint+Constants.LOGGING_URL+apiKey);
							postRequest.setEntity(eventEntity);
							postRequest.setHeader(Constants.CONTENT_TYPE,Constants.CONTENT_TYPE_VALUES);
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
