package org.logger.event.cassandra.loader.dao;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.ednovo.data.model.ClassActivityDatacube;
import org.ednovo.data.model.ContentTaxonomyActivity;
import org.ednovo.data.model.EventBuilder;
import org.ednovo.data.model.EventData;
import org.ednovo.data.model.ResourceCo;
import org.ednovo.data.model.StudentLocation;
import org.ednovo.data.model.StudentsClassActivity;
import org.ednovo.data.model.TaxonomyActivityDataCube;
import org.ednovo.data.model.UserCo;
import org.ednovo.data.model.UserSessionActivity;

import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Rows;

public interface BaseCassandraRepo {

	static BaseCassandraRepo instance() {
		return new BaseCassandraRepoImpl();

	}

	Column<String> readWithKeyColumn(String cfName, String key, String columnName);

	boolean checkColumnExist(String cfName, String key, String columnName);

	ColumnList<String> readWithKeyColumnList(String cfName, String key, Collection<String> columnList);

	Rows<String, String> readWithKeyListColumnList(String cfName, Collection<String> keys, Collection<String> columnList);

	ColumnList<String> readWithKey(String cfName, String key);

	Rows<String, String> readWithKeyList(String cfName, Collection<String> key);

	Rows<String, String> readCommaKeyList(String cfName, String[] key);

	Rows<String, String> readIterableKeyList(String cfName, Iterable<String> keys);

	Rows<String, String> readIndexedColumn(String cfName, String columnName, String value);

	Rows<String, String> readIndexedColumn(String cfName, String columnName, long value);

	Rows<String, String> readIndexedColumnLastNrows(String cfName, String columnName, String value, Integer rowsToRead);

	ColumnList<String> readKeyLastNColumns(String cfName, String key, Integer columnsToRead);

	long getCount(String cfName, String key);

	Rows<String, String> readIndexedColumnList(String cfName, Map<String, String> columnList);

	boolean isRowKeyExists(String cfName, String key);

	boolean isValueExists(String cfName, Map<String, Object> columns);

	Collection<String> getKey(String cfName, Map<String, Object> columns);

	ColumnList<String> readColumnsWithPrefix(String cfName, String rowKey, String startColumnNamePrefix, String endColumnNamePrefix, Integer rowsToRead);

	Rows<String, String> readAllRows(String cfName);

	void saveBulkStringList(String cfName, String key, Map<String, String> columnValueList);

	void saveBulkLongList(String cfName, String key, Map<String, Long> columnValueList);

	void saveBulkList(String cfName, String key, Map<String, Object> columnValueList);

	void saveStringValue(String cfName, String key, String columnName, String value);

	void saveStringValue(String cfName, String key, String columnName, String value, int expireTime);

	void saveLongValue(String cfName, String key, String columnName, long value, int expireTime);

	void saveLongValue(String cfName, String key, String columnName, long value);

	void saveValue(String cfName, String key, String columnName, Object value);

	void increamentCounter(String cfName, String key, String columnName, long value);

	void generateCounter(String cfName, String key, String columnName, long value, MutationBatch m);

	void generateNonCounter(String columnName, Object value, ColumnListMutation<String> m);

	void generateNonCounter(String cfName, String key, String columnName, String value, MutationBatch m);

	void generateNonCounter(String cfName, String key, String columnName, long value, MutationBatch m);

	void generateNonCounter(String cfName, String key, String columnName, Integer value, MutationBatch m);

	void generateNonCounter(String cfName, String key, String columnName, Boolean value, MutationBatch m);

	void generateTTLColumns(String cfName, String key, String columnName, long value, int expireTime, MutationBatch m);

	void generateTTLColumns(String cfName, String key, String columnName, String value, int expireTime, MutationBatch m);

	void deleteAll(String cfName);

	void deleteRowKey(String cfName, String key);

	void deleteColumn(String cfName, String key, String columnName);

	ColumnFamily<String, String> accessColumnFamily(String columnFamilyName);

	String saveEvent(String cfName, EventData eventData);

	String saveEvent(String cfName, String key, EventBuilder event);

	void updateTimelineObject(String cfName, String rowKey, String CoulmnValue, EventBuilder event);

	void updateTimeline(String cfName, EventData eventData, String rowKey);

	void saveActivity(String cfName, HashMap<String, Object> activities);

	Map<String, Object> isEventIdExists(String cfName, String userUid, String eventId);

	List<String> getParentIds(String cfName, String key);

	void updateCollectionItem(String cfName, Map<String, String> eventMap);

	void updateClasspage(String cfName, Map<String, String> eventMap);

	boolean getClassPageOwnerInfo(String cfName, String key, String classPageGooruOid);

	boolean isUserPartOfClass(String cfName, String key, String classPageGooruOid);

	void updateResourceEntity(ResourceCo resourceco);

	void updateUserEntity(UserCo userCo);

	void updateAssessmentAnswer(String cfName, Map<String, Object> eventMap);

	void updateCollection(String cfName, Map<String, Object> eventMap);

	void updateCollectionItemCF(String cfName, Map<String, Object> eventMap);

	void updateClasspageCF(String cfName, Map<String, Object> eventMap);

	Set<String> getAllLevelParents(String cfName, String childOid, int depth);

	void saveSession(String sessionActivityId, long eventTime, String status);

	String getParentId(String cfName, String key);

	boolean saveUserSession(String sessionId, String classUid, String courseUid, String unitUid, String lessonUid, String collectionUid, String userUid, String collectionType, String eventType,
			long eventTime);

	boolean saveUserSessionActivity(UserSessionActivity userSessionActivity);

	boolean saveStudentsClassActivity(StudentsClassActivity studentsClassActivity);

	boolean saveContentTaxonomyActivity(ContentTaxonomyActivity contentTaxonomyActivity);

	boolean saveContentClassTaxonomyActivity(ContentTaxonomyActivity contentTaxonomyActivity);

	boolean saveStudentLocation(StudentLocation studentLocation);

	UserSessionActivity compareAndMergeUserSessionActivity(UserSessionActivity userSessionActivity);

	UserSessionActivity getUserSessionActivity(String sessionId, String gooruOid, String collectionItemId);

	StudentsClassActivity compareAndMergeStudentsClassActivity(StudentsClassActivity studentsClassActivity);

	boolean updateReaction(UserSessionActivity userSessionActivity);

	boolean hasClassActivity(StudentsClassActivity studentsClassActivity);

	UserSessionActivity getSessionScore(UserSessionActivity userSessionActivity, String eventName);

	boolean saveClassActivityDataCube(ClassActivityDatacube studentsClassActivity);

	ClassActivityDatacube getStudentsClassActivityDatacube(String rowKey, String userUid, String collectionType);

	Rows<String, String> getTaxonomy(String rowKey);

	Rows<String, String> getContentTaxonomyActivity(ContentTaxonomyActivity contentTaxonomyActivity);

	Rows<String, String> getContentClassTaxonomyActivity(ContentTaxonomyActivity contentTaxonomyActivity);

	Rows<String, String> getContentTaxonomyActivityDataCube(String rowKey, String columnKey);

	long getContentTaxonomyActivityScore(String rowKey);

	boolean saveTaxonomyActivityDataCube(TaxonomyActivityDataCube taxonomyActivityDataCube);

	boolean saveQuestionGrade(String teacherId, String userId, String sessionId, String questionId, long score);

	Rows<String, String> getQuestionsGradeBySessionId(String teacherId, String userId, String sessionId);

	Rows<String, String> getQuestionsGradeByQuestionId(String teacherId, String userId, String sessionId, String questionId);

	boolean saveQuestionGradeInSession(String sessionId, String questionId, String collectionItemId, String status, long score);

	Rows<String, String> readAllRows(String cfName, CallBackRows allRows);

}
