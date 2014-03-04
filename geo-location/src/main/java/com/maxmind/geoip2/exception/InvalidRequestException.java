/*******************************************************************************
 * InvalidRequestException.java
 * geo-location
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
package com.maxmind.geoip2.exception;

import java.net.URL;

/**
 * This class represents a non-specific error returned by MaxMind's GeoIP2 web
 * service. This occurs when the web service is up and responding to requests,
 * but the request sent was invalid in some way.
 */
final public class InvalidRequestException extends GeoIp2Exception {
    private static final long serialVersionUID = 8662062420258379643L;
    private final String code;
    private final URL url;

    /**
     * @param message
     *            A message explaining the cause of the error.
     * @param code
     *            The error code returned by the web service.
     * @param url
     *            The URL queried.
     */
    public InvalidRequestException(String message, String code, URL url) {
        super(message);
        this.url = url;
        this.code = code;
    }

    /**
     * @param message
     *            A message explaining the cause of the error.
     * @param code
     *            The error code returned by the web service.
     * @param httpStatus
     *            The HTTP status of the response.
     * @param url
     *            The URL queried.
     * @param e
     *            The cause of the exception.
     */
    public InvalidRequestException(String message, String code, int httpStatus,
            URL url, Throwable e) {
        super(message, e);
        this.code = code;
        this.url = url;
    }

    /**
     * @return The error code returned by the MaxMind web service.
     */
    public String getCode() {
        return this.code;
    }

    /**
     * @return the URL queried.
     */
    public URL getUrl() {
        return this.url;
    }
}
