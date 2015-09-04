package org.logger.event.cassandra.loader.dao;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpParams;
import org.logger.event.cassandra.loader.ColumnFamily;
import org.logger.event.cassandra.loader.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.model.ColumnList;


public class LTIServiceHandler implements Constants, Runnable{
	
	private static Logger logger = LoggerFactory.getLogger(LTIServiceHandler.class);
	
	private static String VALID_FIELDS = LTI_SERVICE_ID+COMMA+GOORUID+COMMA+CONTENT_GOORU_OID;
	
	private static BaseCassandraRepoImpl baseDao;
	
	private String gooruOId;
	
	private String gooruUId;
	
	private String sessionToken;
	
	private static DefaultHttpClient httpClient;
	
	private static HttpPost postRequest;
	
	private static final String SERVICE_ID = "serviceId"; 
	
	private static final String LTI_END_POINT = "lti.end.point"; 
	
	public LTIServiceHandler() {}
	
	public LTIServiceHandler(BaseCassandraRepoImpl baseDao) {
		LTIServiceHandler.baseDao = baseDao;
		httpClient = new DefaultHttpClient();
		String url = baseDao.readWithKeyColumn(ColumnFamily.CONFIGSETTINGS.getColumnFamily(), LTI_END_POINT, DEFAULT_COLUMN, 0).getStringValue();
		postRequest = new HttpPost(url);
	}
	
	public void ltiEventProcess(String eventName, Map<String, Object> eventMap) {
		
		String serviceId = eventMap.get(LTI_SERVICE_ID) != null ? eventMap.get(LTI_SERVICE_ID).toString() : null;	
		
		this.gooruUId = eventMap.get(GOORUID) != null ? eventMap.get(GOORUID).toString() : null;
		
		this.gooruOId = eventMap.get(CONTENT_GOORU_OID) != null ? eventMap.get(CONTENT_GOORU_OID).toString() : null;
		
		if(serviceId == null || gooruUId == null || gooruOId == null) {
			logger.error(buildString(VALID_FIELDS, " should not be null for ", eventName));
			return;
		}
		baseDao.saveStringValue(ColumnFamily.LTI_ACTIVITY.getColumnFamily(), buildString(gooruOId, SEPERATOR, gooruUId), serviceId, INPROGRESS, 604800);
	}

	public LTIServiceHandler(String sessionToken, String gooruOId, String gooruUId) {
		this.gooruUId = gooruUId;
		this.gooruOId = gooruOId;
		this.sessionToken = sessionToken;
	}
	
	@Override
	public void run() {
		
		ColumnList<String> ltiColumns = baseDao.readWithKey(ColumnFamily.LTI_ACTIVITY.getColumnFamily(), buildString(gooruOId, SEPERATOR, gooruUId), 0);
		if(ltiColumns == null) {
			return;
		}
		ColumnList<String> sessionColumn = baseDao.readWithKey(ColumnFamily.SESSIONS.getColumnFamily(), buildString(RS, SEPERATOR, gooruOId, SEPERATOR, gooruUId), 0);
		if(sessionColumn != null) {
			String sessionId = sessionColumn.getStringValue(_SESSION_ID, EMPTY);	
			sessionColumn = baseDao.readWithKey(ColumnFamily.SESSION_ACTIVITY.getColumnFamily(), sessionId, 0);
		}
		long score = sessionColumn != null ? sessionColumn.getLongValue(buildString(gooruOId, SEPERATOR, SCORE), 0L) : 0;
		Map<String, Long> serviceBasedScore = new HashMap<String, Long>();
		for(int columnCount = ltiColumns.size()-1; columnCount == 0; columnCount--) {
			String status = ltiColumns.getColumnByIndex(columnCount).getStringValue();
			if(status.equals(INPROGRESS)) {
				String serviceId = ltiColumns.getColumnByIndex(columnCount).getName();
				if(serviceBasedScore.isEmpty()) {
					serviceBasedScore.put(serviceId, score);
				} else {
					serviceBasedScore.put(serviceId, 0L);
				}
			}
		}
		for(Entry<String, Long> entry : serviceBasedScore.entrySet()) {
			executeAPI(sessionToken, gooruOId, entry.getKey(), entry.getValue());
		}
	}
	
	private void executeAPI(String sessionToken, String gooruOId, String serviceId, Long score) {
		HttpParams params = new BasicHttpParams();
		params.setParameter(SESSION_TOKEN, sessionToken);
		params.setParameter(GOORU_OID, gooruOId);
		params.setParameter(SERVICE_ID, serviceId);
		params.setParameter(SCORE, score);
		postRequest.setParams(params);
		try {
			HttpResponse response = httpClient.execute(postRequest);
			if(response.getStatusLine().getStatusCode() != 200) {
				throw new IOException();
			}
		} catch (IOException e) {
			logger.error(buildString("unable to Execute LTI API(",sessionToken,gooruOId,serviceId,score,")"),e);
		}
	}
	
	private String buildString(Object... text) {
		
		StringBuffer keyBuffer = new StringBuffer();
		for(int objectCount = 0; objectCount < text.length; objectCount++) {
			if(text[objectCount] != null) {
				keyBuffer.append(text[objectCount]);
			}
		}
		return keyBuffer.toString();
	}
}
