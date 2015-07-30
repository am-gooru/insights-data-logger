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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.validation.Errors;

public class ServerValidationUtils {
	
	protected final static Logger logger = LoggerFactory.getLogger(ServerValidationUtils.class);

	public static void rejectIfNullOrEmpty(Errors errors, String data, String field, String errorMsg) {
		if (data == null || data.equals("")) {
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
		if (data == null || data.equals("")) {
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
			if (data == null || data.equals("") || data.equalsIgnoreCase("null")) {
				isValidEvent = false;
				logger.error(errorMsg + " ErrorCode :" + errorCode + " FieldName :" + field + " : " + eventJson);
			}
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
}
