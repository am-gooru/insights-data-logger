/*******************************************************************************
 * DataUtils.java
 * insights-event-logger
 * Created by Gooru on 2014
 * Copyright (c) 2014 Gooru. All rights reserved.
 * http://www.goorulearning.org/
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 ******************************************************************************/
package org.logger.event.cassandra.loader;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

public final class DataUtils {

	private static final Map<String, String> formatedEventNameMap;

	private static final Map<String, String> formatedAnswerSeq;

	private static final Map<String, Long> formatedReaction;

	private static final Map<String, String> collectionItemKeys;

	private static final Map<String, String> collectionKeys;

	private static final Map<String, String> collectionItemTableKeys;

	private static final Map<String, String> classpageTableKeyMap;

	private static final Map<String, String> collectionItemCFKeys;

	private static final Map<String, String> collectionCFKeys;

	private static final Map<String, String> classpageCFDataTypeMap;

	private static final Map<String, String> collectionCFDataTypeMap;

	private static final Map<String, String> collectionItemCFDataTypeMap;

	private static final Map<String, Long> resourceFormatIdMap;

	static {
		formatedEventNameMap = new HashMap<>();
		formatedEventNameMap.put("collection-search", "search_performed");
		formatedEventNameMap.put("resource-search", "search_performed");
		formatedEventNameMap.put("quiz-search", "search_performed");
		formatedEventNameMap.put("scollection-search", "search_performed");
		formatedEventNameMap.put("resource-add", "resources uploaded");
		formatedEventNameMap.put("scollection-item-add", "resources uploaded");
		formatedEventNameMap.put("scollection-copy", "collection customized");
		formatedEventNameMap.put("collection-copy", "collection customized");
		formatedEventNameMap.put("collection-play", "collections viewed");
		formatedEventNameMap.put("collection-play-dots", "collections viewed");
		formatedEventNameMap.put("resource-preview", "resources viewed");
		formatedEventNameMap.put("resource-play-dots", "resources viewed");
		formatedEventNameMap.put("sessionExpired", "session-expired");
		formatedEventNameMap.put("collection-create", "created_collections");
		formatedEventNameMap.put("scollection-create", "created_collections");
	}

	static {
		formatedAnswerSeq = new HashMap<>();
		formatedAnswerSeq.put("0", "skipped");
		formatedAnswerSeq.put("1", "A");
		formatedAnswerSeq.put("2", "B");
		formatedAnswerSeq.put("3", "C");
		formatedAnswerSeq.put("4", "D");
		formatedAnswerSeq.put("5", "E");
		formatedAnswerSeq.put("6", "F");
	}

	static {
		formatedReaction = new HashMap<>();
		formatedReaction.put("i-need-help", 1L);
		formatedReaction.put("i-donot-understand", 2L);
		formatedReaction.put("meh", 3L);
		formatedReaction.put("i-can-understand", 4L);
		formatedReaction.put("i-can-explain", 5L);
	}

	static {
		collectionItemTableKeys = new HashMap<>();
		collectionItemTableKeys.put("collectionContentId", "collectionContentId");
		collectionItemTableKeys.put("collectionGooruOid", "collectionGooruOid");
		collectionItemTableKeys.put("resourceGooruOid", "resourceGooruOid");
		collectionItemTableKeys.put("resourceContentId", "resourceContentId");
		collectionItemTableKeys.put("deleted", "deleted");
		collectionItemTableKeys.put("questionType", "questionType");
		collectionItemTableKeys.put("performanceTasks", "performanceTasks");
		collectionItemTableKeys.put("minimumScore", "minimumScore");
		collectionItemTableKeys.put("narration", "narration");
		collectionItemTableKeys.put("estimatedTime", "estimatedTime");
		collectionItemTableKeys.put("start", "start");
		collectionItemTableKeys.put("stop", "stop");
		collectionItemTableKeys.put("narrationType", "narrationType");
		collectionItemTableKeys.put("plannedEndDate", "plannedEndDate");
		collectionItemTableKeys.put("associationDate", "associationDate");
		collectionItemTableKeys.put("associatedByUid", "associatedByUid");
		collectionItemTableKeys.put("isRequired", "isRequired");
	}

	static {
		collectionItemCFKeys = new HashMap<>();
		collectionItemCFKeys.put("deleted", "deleted");
		collectionItemCFKeys.put("item_type", "itemType");
		collectionItemCFKeys.put("resource_content_id", "resourceContentId");
		collectionItemCFKeys.put("collection_content_id", "collectionContentId");
		collectionItemCFKeys.put("collection_gooru_oid", "collectionGooruOid");
		collectionItemCFKeys.put("resource_gooru_oid", "resourceGooruOid");
		collectionItemCFKeys.put("item_sequence", "itemSequence");
		collectionItemCFKeys.put("collection_item_id", "collectionItemId");
		collectionItemCFKeys.put("question_type", "questionType");
		collectionItemCFKeys.put("minimum_score", "minimumScore");
		collectionItemCFKeys.put("narration", "narration");
		collectionItemCFKeys.put("estimated_time", "estimatedTime");
		collectionItemCFKeys.put("start", "start");
		collectionItemCFKeys.put("stop", "stop");
		collectionItemCFKeys.put("narration_type", "narrationType");
		collectionItemCFKeys.put("planned_end_date", "plannedEndDate");
		collectionItemCFKeys.put("association_date", "associationDate");
		collectionItemCFKeys.put("associated_by_uid", "associatedByUid");
		collectionItemCFKeys.put("is_required", "isRequired");
		collectionItemCFKeys.put("rKey", "rKey");
	}

	static {
		collectionCFKeys = new HashMap<>();
		collectionCFKeys.put("gooru_oid", "gooruOid");
		collectionCFKeys.put("content_id", "contentId");
		collectionCFKeys.put("collection_type", "collectionType");
		collectionCFKeys.put("grade", "grade");
		collectionCFKeys.put("goals", "goals");
		collectionCFKeys.put("ideas", "ideas");
		collectionCFKeys.put("performance_tasks", "performanceTasks");
		collectionCFKeys.put("language", "language");
		collectionCFKeys.put("key_points", "keyPoints");
		collectionCFKeys.put("notes", "notes");
		collectionCFKeys.put("language_objective", "languageObjective");
		collectionCFKeys.put("network", "network");
		collectionCFKeys.put("mail_notification", "mailNotification");
		collectionCFKeys.put("build_type_id", "buildTypeId");
		collectionCFKeys.put("narration_link", "narrationLink");
		collectionCFKeys.put("estimated_time", "estimatedTime");
		collectionCFKeys.put("rKey", "rKey");

	}
	static {
		classpageTableKeyMap = new HashMap<>();
		classpageTableKeyMap.put("deleted", "deleted");
		classpageTableKeyMap.put("classpage_content_id", "contentId");
		classpageTableKeyMap.put("classpage_gooru_oid", "classId");
		classpageTableKeyMap.put("username", "username");
		classpageTableKeyMap.put("user_group_uid", "groupUId");
		classpageTableKeyMap.put("organization_uid", "organizationUId");
		classpageTableKeyMap.put("user_group_type", "userGroupType");
		classpageTableKeyMap.put("active_flag", "activeFlag");
		classpageTableKeyMap.put("user_group_code", "classCode");
		classpageTableKeyMap.put("classpage_code", "classCode");
		classpageTableKeyMap.put("gooru_uid", "userUid");
		classpageTableKeyMap.put("is_group_owner", "isGroupOwner");
		classpageTableKeyMap.put("rKey", "rKey");
	}

	static {
		collectionKeys = new HashMap<>();
		collectionKeys.put("collectionContentId", "collectionContentId");
		collectionKeys.put("gooruOid", "collectionGooruOid");
		collectionKeys.put("collectionType", "collectionType");
		collectionKeys.put("grade", "collectionGrade");
		collectionKeys.put("goals", "collectionGoals");
		collectionKeys.put("ideas", "ideas");
		collectionKeys.put("performanceTasks", "performanceTasks");
		collectionKeys.put("language", "language");
		collectionKeys.put("keyPoints", "keyPoints");
		collectionKeys.put("notes", "notes");
		collectionKeys.put("languageObjective", "languageObjective");
		collectionKeys.put("network", "network");
		collectionKeys.put("mailNotification", "mailNotification");
		collectionKeys.put("buildTypeId", "buildTypeId");
		collectionKeys.put("narrationLink", "narrationLink");
		collectionKeys.put("estimatedTime", "estimatedTime");
	}
	static {
		collectionItemKeys = new HashMap<>();
		collectionItemKeys.put("contentGooruId", "resourceGooruOid");
		collectionItemKeys.put("parentGooruId", "collectionGooruOid");
		collectionItemKeys.put("parentContentId", "collectionContentId");
		collectionItemKeys.put("contentId", "resourceContentId");
		collectionItemKeys.put("collectionItemId", "collectionItemId");
		collectionItemKeys.put("itemType", "itemType");
		collectionItemKeys.put("itemSequence", "itemSequence");
		collectionItemKeys.put("gooruUId", "associatedByUid");
		collectionItemKeys.put("associationDate", "associationDate");
		collectionItemKeys.put("typeName", "questionType");

	}
	static {
		classpageCFDataTypeMap = new HashMap<>();
		classpageCFDataTypeMap.put("deleted", "Integer");
		classpageCFDataTypeMap.put("classpage_content_id", "Long");
		classpageCFDataTypeMap.put("classpage_gooru_oid", "String");
		classpageCFDataTypeMap.put("username", "String");
		classpageCFDataTypeMap.put("user_group_uid", "String");
		classpageCFDataTypeMap.put("organization_uid", "String");
		classpageCFDataTypeMap.put("user_group_type", "String");
		classpageCFDataTypeMap.put("active_flag", "Integer");
		classpageCFDataTypeMap.put("user_group_code", "String");
		classpageCFDataTypeMap.put("classpage_code", "String");
		classpageCFDataTypeMap.put("gooru_uid", "String");
		classpageCFDataTypeMap.put("is_group_owner", "Integer");
		classpageCFDataTypeMap.put("rKey", "String");
	}
	static {
		collectionCFDataTypeMap = new HashMap<>();
		collectionCFDataTypeMap.put("gooru_oid", "String");
		collectionCFDataTypeMap.put("content_id", "Long");
		collectionCFDataTypeMap.put("collection_type", "String");
		collectionCFDataTypeMap.put("grade", "String");
		collectionCFDataTypeMap.put("goals", "String");
		collectionCFDataTypeMap.put("ideas", "String");
		collectionCFDataTypeMap.put("performance_tasks", "String");
		collectionCFDataTypeMap.put("language", "String");
		collectionCFDataTypeMap.put("key_points", "String");
		collectionCFDataTypeMap.put("notes", "String");
		collectionCFDataTypeMap.put("language_objective", "String");
		collectionCFDataTypeMap.put("network", "String");
		collectionCFDataTypeMap.put("mail_notification", "Boolean");
		collectionCFDataTypeMap.put("build_type_id", "Long");
		collectionCFDataTypeMap.put("narration_link", "String");
		collectionCFDataTypeMap.put("estimated_time", "String");
		collectionCFDataTypeMap.put("rKey", "String");
	}
	static {
		collectionItemCFDataTypeMap = new HashMap<>();
		collectionItemCFDataTypeMap.put("deleted", "Integer");
		collectionItemCFDataTypeMap.put("item_type", "String");
		collectionItemCFDataTypeMap.put("resource_content_id", "Long");
		collectionItemCFDataTypeMap.put("collection_content_id", "Long");
		collectionItemCFDataTypeMap.put("collection_gooru_oid", "String");
		collectionItemCFDataTypeMap.put("resource_gooru_oid", "String");
		collectionItemCFDataTypeMap.put("item_sequence", "Integer");
		collectionItemCFDataTypeMap.put("collection_item_id", "String");
		collectionItemCFDataTypeMap.put("question_type", "String");
		collectionItemCFDataTypeMap.put("minimum_score", "String");
		collectionItemCFDataTypeMap.put("narration", "String");
		collectionItemCFDataTypeMap.put("estimated_time", "String");
		collectionItemCFDataTypeMap.put("start", "String");
		collectionItemCFDataTypeMap.put("stop", "String");
		collectionItemCFDataTypeMap.put("narration_type", "String");
		collectionItemCFDataTypeMap.put("planned_end_date", "Timestamp");
		collectionItemCFDataTypeMap.put("association_date", "Timestamp");
		collectionItemCFDataTypeMap.put("associated_by_uid", "String");
		collectionItemCFDataTypeMap.put("is_required", "Integer");
		collectionItemCFDataTypeMap.put("rKey", "String");
	}

	static {
		resourceFormatIdMap = new HashMap<>();
		resourceFormatIdMap.put("animation/swf",103L);
		resourceFormatIdMap.put("animation/kmz",103L);
		resourceFormatIdMap.put("application",123L);
		resourceFormatIdMap.put("assessment-exam",130L);
		resourceFormatIdMap.put("assessment-question",104L);
		resourceFormatIdMap.put("assessment-quiz",106L);
		resourceFormatIdMap.put("assignment", 132L);
		resourceFormatIdMap.put("classpage",131L);
		resourceFormatIdMap.put("exam/pdf",102L);
		resourceFormatIdMap.put("folder",133L);
		resourceFormatIdMap.put("gooru/classbook",124L);
		resourceFormatIdMap.put("gooru/classplan",123L);
		resourceFormatIdMap.put("gooru/notebook",126L);
		resourceFormatIdMap.put("gooru/studyshelf",125L);
		resourceFormatIdMap.put("handouts", 106L);
		resourceFormatIdMap.put("image/png",105L);
		resourceFormatIdMap.put("ppt/pptx", 105L);
		resourceFormatIdMap.put("qb/question",128L);
		resourceFormatIdMap.put("qb/response",127L);
		resourceFormatIdMap.put("question",104L);
		resourceFormatIdMap.put("resource/url",102L);
		resourceFormatIdMap.put("scollection",122L);
		resourceFormatIdMap.put("shelf",122L);
		resourceFormatIdMap.put("textbook/scribd",106L);
		resourceFormatIdMap.put("video/youtube",100L);
		resourceFormatIdMap.put("vimeo/video",100L);
		resourceFormatIdMap.put("quiz",122L);
	}

	private DataUtils() {
		throw new AssertionError();
	}

	public static String makeCombinedEventName(String eventName) {
		return StringUtils.defaultIfEmpty(formatedEventNameMap.get(eventName), eventName);
	}

	public static Long formatReactionString(String key) {
		return formatedReaction.get(key) == null ? 0L : formatedReaction.get(key);
	}

	public static String makeCombinedAnswerSeq(int sequence) {
		return StringUtils.defaultIfEmpty(formatedAnswerSeq.get(String.valueOf(sequence)), String.valueOf(sequence));
	}

	public static Long getResourceFormatId(String key) {
		return resourceFormatIdMap.containsKey(key) ? resourceFormatIdMap.get(key) : null ;
	}

}
