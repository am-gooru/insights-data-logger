/*******************************************************************************
 * ServerValidationUtils.java
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
package org.logger.event.web.utils;

import java.util.Date;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.ednovo.data.model.Event;
import org.json.JSONException;
import org.json.JSONObject;
import org.logger.event.cassandra.loader.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.validation.Errors;

public class ServerValidationUtils implements Constants {
	
	protected final static Logger logger = LoggerFactory.getLogger(ServerValidationUtils.class);

	public static void rejectIfNullOrEmpty(Errors errors, String data, String field, String errorMsg) {
		if (data == null || StringUtils.isBlank(data)) {
			errors.rejectValue(field, errorMsg);
		}
	}
	public static void rejectIfAnyException(Errors errors, String errorCode, Exception exception) {
			errors.rejectValue(errorCode, exception.toString());
	}
	public static void rejectIfNull(Errors errors, Object data, String field, String errorMsg) {
		if (data == null) {
			errors.rejectValue(field, errorMsg);
		}
	}

	public static void rejectIfNull(Errors errors, Object data, String field, String errorCode, String errorMsg) {
		if (data == null) {
			errors.rejectValue(field, errorCode, errorMsg);
		}
	}
	
	public static void rejectIfAlReadyExist(Errors errors, Object data,  String errorCode, String errorMsg) {
		if (data != null) {
			errors.reject(errorCode, errorMsg);
		}
	}
	
	public static void rejectIfNullOrEmpty(Errors errors, Set<?> data, String field, String errorMsg) {
		if (data == null || data.size() == 0) {
			errors.rejectValue(field, errorMsg);
		}
	}

	public static void rejectIfNullOrEmpty(Errors errors, String data, String field, String errorCode, String errorMsg) {
		if (data == null || StringUtils.isBlank(data)) {
			errors.rejectValue(field, errorCode, errorMsg);
		}
	}

	public static void rejectIfInvalidType(Errors errors, String data, String field, String errorCode, String errorMsg, Map<String, String> typeParam) {
		if (!typeParam.containsKey(data)) {
			errors.rejectValue(field, errorCode, errorMsg);
		}
	}
	
	public static void rejectIfInvalidDate(Errors errors, Date data, String field, String errorCode, String errorMsg) {
		Date date =new Date();
		if (data.compareTo(date) <= 0) {
			errors.rejectValue(field, errorCode, errorMsg);
		}
	}

	public static void rejectIfInvalid(Errors errors, Double data, String field, String errorCode, String errorMsg, Map<Double, String> ratingScore) {
		if (!ratingScore.containsKey(data)) {
			errors.rejectValue(field, errorCode, errorMsg);
		}
	}
	
	public static void logErrorIfZeroLongValue(Boolean isValidEvent, Long data, String field, String errorCode, String eventJson, String errorMsg) {
		if (isValidEvent) {
			if (data == null || data == 0L) {
				isValidEvent = false;
				logger.error(errorMsg + " ErrorCode :" + errorCode + " FieldName :" + field + " : " + eventJson);
			}
		}
	}
	
	public static void logErrorIfNullOrEmpty(Boolean isValidEvent, String data, String field, String errorCode, String eventJson, String errorMsg) {
		if (isValidEvent) {
			if (data == null || StringUtils.isBlank(data) || data.equalsIgnoreCase("null")) {
				isValidEvent = false;
				logger.error(errorMsg + " ErrorCode :" + errorCode + " FieldName :" + field + " : " + eventJson);
			}
		}
	}
	
	public static void logErrorIfNullOrEmpty(Boolean isValidEvent, JSONObject data, String field, String errorCode, String eventJson, String errorMsg) {
		if (isValidEvent && data == null) {
				isValidEvent = false;
				logger.error(errorMsg + " ErrorCode :" + errorCode + " FieldName :" + field + " : " + eventJson);
		}
	}
	
	public static void logErrorIfNull(Boolean isValidEvent, Object data, String field, String errorMsg) {
		if (isValidEvent) {
			if (data == null) {
				isValidEvent = false;
				logger.error(errorMsg + " : FieldName :" + field + " : ");
			}
		}
	}

	public static void deepEventCheck(Boolean isValidEvent, Event event,
			String field) {
		if (isValidEvent
				&& event.getEventName().matches(SESSION_ACTIVITY_EVENTS)) {
			try {
				JSONObject session = event.getSession();
				if (!session.has(SESSION_ID)
						|| (session.has(SESSION_ID) && (session
								.isNull(SESSION_ID) || (session.get(SESSION_ID) != null && session
								.getString(SESSION_ID).equalsIgnoreCase("null"))))) {
					isValidEvent = false;
					logger.error(RAW_EVENT_NULL_EXCEPTION + SESSION_ID + " : "
							+ field);
				}

			} catch (JSONException e) {
				isValidEvent = false;
				logger.error(RAW_EVENT_JSON_EXCEPTION + SESSION + " : " + field);
			}
			try {
				JSONObject context = event.getContext();
				if (!context.has(CONTENT_GOORU_OID)
						|| (context.has(CONTENT_GOORU_OID) && (context
								.isNull(CONTENT_GOORU_OID) || (context
								.get(CONTENT_GOORU_OID) != null && context
								.getString(CONTENT_GOORU_OID).equalsIgnoreCase(
										"null"))))) {
					isValidEvent = false;
					logger.error(RAW_EVENT_NULL_EXCEPTION + CONTENT_GOORU_OID
							+ " : " + field);
				}
				if ((context.has(CLASS_GOORU_OID) && (context
						.isNull(CLASS_GOORU_OID) || (context
						.get(CLASS_GOORU_OID) != null && !context.getString(
						CLASS_GOORU_OID).equalsIgnoreCase("null"))))) {
					// Log If class Id is available and any one of the CUL
					// ids are missing
					if ((!context.has(COURSE_GOORU_OID) || (context
							.has(COURSE_GOORU_OID) && (context
							.isNull(COURSE_GOORU_OID) || (context
							.get(COURSE_GOORU_OID) != null && context
							.getString(COURSE_GOORU_OID).equalsIgnoreCase(
									"null")))))
							|| (!context.has(UNIT_GOORU_OID) || (context
									.has(UNIT_GOORU_OID) && (context
									.isNull(UNIT_GOORU_OID) || (context
									.get(UNIT_GOORU_OID) != null && context
									.getString(UNIT_GOORU_OID)
									.equalsIgnoreCase("null")))))
							|| (!context.has(LESSON_GOORU_OID) || (context
									.has(LESSON_GOORU_OID) && (context
									.isNull(LESSON_GOORU_OID) || (context
									.get(LESSON_GOORU_OID) != null && context
									.getString(LESSON_GOORU_OID)
									.equalsIgnoreCase("null")))))) {
						isValidEvent = false;
						logger.error(RAW_EVENT_NULL_EXCEPTION + CUL_IDS_MISSING
								+ " : " + field);
					}
				}
			} catch (JSONException e) {
				isValidEvent = false;
				logger.error(RAW_EVENT_JSON_EXCEPTION + CONTEXT + " : " + field);
			}
		}
	}

}
