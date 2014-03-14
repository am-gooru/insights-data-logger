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

public class DataUtils {

	private static Map<String, String> formatedEventNameMap;

	private static Map<String, String> formatedAnswerSeq;
	
	private static Map<String, Long> formatedReaction;
	
	static {
    	formatedEventNameMap = new HashMap<String, String>();
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
		formatedAnswerSeq = new HashMap<String, String>();
		formatedAnswerSeq.put("0", "Skipped");
		formatedAnswerSeq.put("1", "A");
		formatedAnswerSeq.put("2", "B");
		formatedAnswerSeq.put("3", "C");
		formatedAnswerSeq.put("4", "D");
		formatedAnswerSeq.put("5", "E");
		formatedAnswerSeq.put("6", "F");
	}
	
	static {
		formatedReaction = new HashMap<String, Long>();
		formatedReaction.put("i-need-help", 1L);
		formatedReaction.put("i-donot-understand", 2L);
		formatedReaction.put("meh", 3L);
		formatedReaction.put("i-can-understand", 4L);
		formatedReaction.put("i-can-explain", 5L);
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
	

}
