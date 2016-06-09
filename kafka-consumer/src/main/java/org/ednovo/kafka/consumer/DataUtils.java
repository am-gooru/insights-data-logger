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
package org.ednovo.kafka.consumer;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

public final class DataUtils {

	private static final Pattern collectionLoadPattern = Pattern
			.compile("^(/collection/)([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})(.json)$");
	private static final Pattern collectionPlayPattern = Pattern
			.compile("^(/collection/)([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})(/play.json)$");
	private static final Pattern quizAttemptStartPattern = Pattern
			.compile("^(/assessment/)([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})(/attempt/attempt.json)$");
	private static final Map<String, String> formatedEventNameMap;

	static {
    	formatedEventNameMap = new HashMap<>();
		formatedEventNameMap.put("checkContentAccess", "check-content-access");
		formatedEventNameMap.put("checkSession", "check-session");
		formatedEventNameMap.put("collection play", "collection-play");
		formatedEventNameMap.put("collectionplay", "collection-play");
		formatedEventNameMap.put("loadPage", "load-page");
		formatedEventNameMap.put("login.SSO", "login-SSO");
		formatedEventNameMap.put("profileStatus", "profile-status");
		formatedEventNameMap.put("quiz play", "quiz-play");
		formatedEventNameMap.put("quizplay", "quiz-play");
		formatedEventNameMap.put("quiz preview", "quiz-preview");
		formatedEventNameMap.put("resource preview", "resource-preview");
		formatedEventNameMap.put("resourcepreview", "resource-preview");
		formatedEventNameMap.put("sessionExpired", "session-expired");
		formatedEventNameMap.put("showRating", "show-rating");
		formatedEventNameMap.put("resourceCollectionList", "resource-collection-list");
	}

	private DataUtils() {
		throw new AssertionError();
	}

	public static String makeEventNameConsistent(String eventName) {
		return StringUtils.defaultIfEmpty(formatedEventNameMap.get(eventName), eventName);
	}

	public static Map<String, String> guessEventName(String context) {
		Map<String, String> guessedAttributes = new HashMap<>();
		Matcher matcher = collectionLoadPattern.matcher(context);
		if (matcher.matches()) {
			String gooruOid = matcher.group(2);
			guessedAttributes.put("parent_gooru_id", gooruOid);
			guessedAttributes.put("content_gooru_id", gooruOid);
			guessedAttributes.put("event_name", "collection-load");
		} else {
			matcher = collectionPlayPattern.matcher(context);
			if (matcher.matches()) {
				String gooruOid = matcher.group(2);
				guessedAttributes.put("parent_gooru_id", gooruOid);
				guessedAttributes.put("content_gooru_id", gooruOid);
				guessedAttributes.put("event_name", "collection-load");
			}
		}
		matcher = quizAttemptStartPattern.matcher(context);
		if (matcher.matches()) {
			String gooruOid = matcher.group(2);
			guessedAttributes.put("parent_gooru_id", gooruOid);
			guessedAttributes.put("content_gooru_id", gooruOid);
			guessedAttributes.put("event_name", "quiz-attempt-start");
		}
		return guessedAttributes;
	}
}
