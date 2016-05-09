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
import org.ednovo.data.model.EventBuilder;
import org.json.JSONException;
import org.json.JSONObject;
import org.logger.event.cassandra.loader.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.validation.Errors;

public class ServerValidationUtils {
	
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

	public static void deepEventCheck(Boolean isValidEvent, EventBuilder event,
			String field) {
		if (isValidEvent
				&& event.getEventName().matches(Constants.SESSION_ACTIVITY_EVENTS)) {
			try {
				JSONObject session = event.getSession();
				if (!session.has(Constants.SESSION_ID)
						|| (session.has(Constants.SESSION_ID) && (session
								.isNull(Constants.SESSION_ID) || (session.get(Constants.SESSION_ID) != null && session
								.getString(Constants.SESSION_ID).equalsIgnoreCase("null"))))) {
					isValidEvent = false;
					logger.error(Constants.RAW_EVENT_NULL_EXCEPTION + Constants.SESSION_ID + " : "
							+ field);
				}

			} catch (JSONException e) {
				isValidEvent = false;
				logger.error(Constants.RAW_EVENT_JSON_EXCEPTION + Constants.SESSION + " : " + field);
			}
			try {
				JSONObject context = event.getContext();
				if (!context.has(Constants.CONTENT_GOORU_OID)
						|| (context.has(Constants.CONTENT_GOORU_OID) && (context
								.isNull(Constants.CONTENT_GOORU_OID) || (context
								.get(Constants.CONTENT_GOORU_OID) != null && context
								.getString(Constants.CONTENT_GOORU_OID).equalsIgnoreCase(
										"null"))))) {
					isValidEvent = false;
					logger.error(Constants.RAW_EVENT_NULL_EXCEPTION + Constants.CONTENT_GOORU_OID
							+ " : " + field);
				}
				if(event.getGooruUUID().equalsIgnoreCase(Constants.ANONYMOUS)){
					isValidEvent = false;
					logger.error(Constants.ANONYMOUS + " is not valid user event"
							+ " : " + field);
				}
				if ((context.has(Constants.CLASS_GOORU_OID) && (!context
						.isNull(Constants.CLASS_GOORU_OID) || (context
						.get(Constants.CLASS_GOORU_OID) != null && !context.getString(
						Constants.CLASS_GOORU_OID).equalsIgnoreCase("null"))))) {
					// Log If class Id is available and any one of the CUL
					// ids are missing
					if ((!context.has(Constants.COURSE_GOORU_OID) || (context
							.has(Constants.COURSE_GOORU_OID) && (context
							.isNull(Constants.COURSE_GOORU_OID) || (context
							.get(Constants.COURSE_GOORU_OID) != null && context
							.getString(Constants.COURSE_GOORU_OID).equalsIgnoreCase(
									"null")))))
							|| (!context.has(Constants.UNIT_GOORU_OID) || (context
									.has(Constants.UNIT_GOORU_OID) && (context
									.isNull(Constants.UNIT_GOORU_OID) || (context
									.get(Constants.UNIT_GOORU_OID) != null && context
									.getString(Constants.UNIT_GOORU_OID)
									.equalsIgnoreCase("null")))))
							|| (!context.has(Constants.LESSON_GOORU_OID) || (context
									.has(Constants.LESSON_GOORU_OID) && (context
									.isNull(Constants.LESSON_GOORU_OID) || (context
									.get(Constants.LESSON_GOORU_OID) != null && context
									.getString(Constants.LESSON_GOORU_OID)
									.equalsIgnoreCase("null")))))) {
						isValidEvent = false;
						logger.error(Constants.RAW_EVENT_NULL_EXCEPTION + Constants.CUL_IDS_MISSING
								+ " : " + field);
					}
				}
			} catch (JSONException e) {
				isValidEvent = false;
				logger.error(Constants.RAW_EVENT_JSON_EXCEPTION + Constants.CONTEXT + " : " + field);
			}
		}
	}

}
