package org.logger.event.cassandra.loader.dao;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.logger.event.cassandra.loader.ColumnFamilySet;
import org.logger.event.cassandra.loader.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.model.ColumnList;


public class LTIServiceHandler implements Runnable{
	
	private static final Logger LOG = LoggerFactory.getLogger(LTIServiceHandler.class);
	
	private static final String VALID_FIELDS = Constants.LTI_SERVICE_ID+Constants.COMMA+Constants.GOORUID+Constants.COMMA+Constants.CONTENT_GOORU_OID;
	
	private static BaseCassandraRepo baseDao;
	
	private String gooruOId;
	
	private String gooruUId;
	
	private String sessionToken;
	
	private static DefaultHttpClient httpClient;
	
	private static HttpPost postRequest;
	
	private static URIBuilder builder;
	
	private static final String SERVICE_ID = "serviceId"; 
	
	private static final String LTI_END_POINT = "lti.end.point"; 
	
	public LTIServiceHandler() {}
	
	public LTIServiceHandler(BaseCassandraRepo baseDao) {
		LTIServiceHandler.baseDao = baseDao;
		httpClient = new DefaultHttpClient();
		String url = baseDao.readWithKeyColumn(ColumnFamilySet.CONFIGSETTINGS.getColumnFamily(), LTI_END_POINT, Constants.DEFAULT_COLUMN).getStringValue();
		postRequest = new HttpPost();
		try {
			builder = new URIBuilder();
			builder.setPath(url);
		} catch (Exception e) {
			LOG.error("ERROR while building a LTI API path", e);
		}
	}
	
	public void ltiEventProcess(String eventName, Map<String, Object> eventMap) {
		
		String serviceId = eventMap.get(Constants.LTI_SERVICE_ID) != null ? eventMap.get(Constants.LTI_SERVICE_ID).toString() : null;	
		
		this.gooruUId = eventMap.get(Constants.GOORUID) != null ? eventMap.get(Constants.GOORUID).toString() : null;
		
		this.gooruOId = eventMap.get(Constants.CONTENT_GOORU_OID) != null ? eventMap.get(Constants.CONTENT_GOORU_OID).toString() : null;
		
		if(serviceId == null || gooruUId == null || gooruOId == null) {
			LOG.error(buildString(VALID_FIELDS, " should not be null for ", eventName));
			return;
		}
		baseDao.saveStringValue(ColumnFamilySet.LTI_ACTIVITY.getColumnFamily(), buildString(gooruOId, Constants.SEPERATOR, gooruUId), serviceId, Constants.INPROGRESS, 604800);
	}

	public LTIServiceHandler(String sessionToken, String gooruOId, String gooruUId) {
		this.gooruUId = gooruUId;
		this.gooruOId = gooruOId;
		this.sessionToken = sessionToken;
	}
	
	@Override
	public void run() {
		ColumnList<String> ltiColumns = baseDao.readWithKey(ColumnFamilySet.LTI_ACTIVITY.getColumnFamily(), buildString(gooruOId, Constants.SEPERATOR, gooruUId));
		if(ltiColumns == null) {
			return;
		}
		ColumnList<String> sessionColumn = baseDao.readWithKey(ColumnFamilySet.SESSIONS.getColumnFamily(), buildString(Constants.RS, Constants.SEPERATOR, gooruOId, Constants.SEPERATOR, gooruUId));
		String sessionId = sessionColumn != null ? sessionColumn.getStringValue(Constants._SESSION_ID, null) : null;	
		if(sessionId == null) {
			return;
		}
		sessionColumn = baseDao.readWithKey(ColumnFamilySet.SESSION_ACTIVITY.getColumnFamily(), sessionId);
		long score = sessionColumn != null ? sessionColumn.getLongValue(buildString(gooruOId, Constants.SEPERATOR, Constants._SCORE_IN_PERCENTAGE), 0L) : 0;
		Map<String, Long> serviceBasedScore = new HashMap<String, Long>();
		for(int columnCount = ltiColumns.size()-1; columnCount >= 0; columnCount--) {
			String status = ltiColumns.getColumnByIndex(columnCount).getStringValue();
			if(status.equals(Constants.INPROGRESS)) {
				String serviceId = ltiColumns.getColumnByIndex(columnCount).getName();
				if(serviceBasedScore.isEmpty()) {
					serviceBasedScore.put(serviceId, score);
				} else {
					serviceBasedScore.put(serviceId, 0L);
				}
			}
		}
		for(Entry<String, Long> entry : serviceBasedScore.entrySet()) {
			if(executeAPI(sessionToken, gooruOId, entry.getKey(), entry.getValue())) {
				baseDao.saveStringValue(ColumnFamilySet.LTI_ACTIVITY.getColumnFamily(), buildString(gooruOId, Constants.SEPERATOR, gooruUId), entry.getKey(), Constants.COMPLETED);
			}
		}
	}
	
	private boolean executeAPI(String sessionToken, String gooruOId, String serviceId, Long score) {
		builder.setParameter(Constants.SESSION_TOKEN, sessionToken);
		builder.setParameter(Constants.GOORU_OID, gooruOId);
		builder.setParameter(SERVICE_ID, serviceId);
		builder.setParameter(Constants.SCORE, String.valueOf(score));
		boolean status = false;
		try {
			postRequest.setURI(builder.build());
			HttpResponse response = httpClient.execute(postRequest);
			EntityUtils.toString(response.getEntity());
			if(response.getStatusLine().getStatusCode() != 200) {
				throw new IOException();
			}
			status = true;
		} catch (Exception e) {
			LOG.error(buildString("unable to Execute LTI API(",sessionToken,Constants.SEPERATOR,gooruOId,Constants.SEPERATOR,serviceId,Constants.SEPERATOR,score,")"),e);
		}
		return status;
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
