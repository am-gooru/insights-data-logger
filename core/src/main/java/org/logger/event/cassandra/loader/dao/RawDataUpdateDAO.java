package org.logger.event.cassandra.loader.dao;

import java.util.Map;

import org.ednovo.data.model.ResourceCo;
import org.ednovo.data.model.UserCo;

public interface RawDataUpdateDAO {
	
	ResourceCo processResource(Map<String, Object> eventMap, ResourceCo resourceCo);
	ResourceCo processCollection(Map<String, Object> eventMap, ResourceCo resourceCo);
	UserCo processUser(Map<String, Object> eventMap, UserCo userCo);
	void updateCollectionItemTable(Map<String, Object> eventMap, Map<String, Object> collectionItemMap);
	void updateClasspage(Map<String, Object> dataMap, Map<String, Object> classpageMap);
	void updateCollectionTable(Map<String, Object> eventMap, Map<String, Object> collectionMap);
	void updateAssessmentAnswer(Map<String, Object> eventMap, Map<String, Object> assessmentAnswerMap);

}
