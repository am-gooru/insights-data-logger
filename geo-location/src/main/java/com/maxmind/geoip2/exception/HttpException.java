/*******************************************************************************
 * Copyright 2014 Ednovo d/b/a Gooru. All rights reserved.
 * http://www.goorulearning.org/
 *   
 *   HttpException.java
 *   event-api-stable-1.2
 *   
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *  
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *  
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 ******************************************************************************/
/**
 * This class provides a model for the data returned by the GeoIP2 Omni end
 * point.
 *
 * The only difference between the City, City/ISP/Org, and Omni model classes is
 * which fields in each record may be populated.
 *
 * @see <a href="http://dev.maxmind.com/geoip/geoip2/web-services">GeoIP2 Web
 *      Services</a>
 */
package com.maxmind.geoip2.exception;

import java.io.IOException;
import java.net.URL;

/**
 * This class represents an HTTP transport error. This is not an error returned
 * by the web service itself. As such, it is a IOException instead of a
 * GeoIp2Exception.
 */
final public class HttpException extends IOException {
    private static final long serialVersionUID = -8301101841509056974L;
    private final int httpStatus;
    private final URL url;

    /**
     * @param message
     *            A message describing the reason why the exception was thrown.
     * @param httpStatus
     *            The HTTP status of the response that caused the exception.
     * @param url
     *            The URL queried.
     */
    public HttpException(String message, int httpStatus, URL url) {
        super(message);
        this.httpStatus = httpStatus;
        this.url = url;
    }

    /**
     * @param message
     *            A message describing the reason why the exception was thrown.
     * @param httpStatus
     *            The HTTP status of the response that caused the exception.
     * @param url
     *            The URL queried.
     * @param cause
     *            The cause of the exception.
     */
    public HttpException(String message, int httpStatus, URL url,
            Throwable cause) {
        super(message, cause);
        this.httpStatus = httpStatus;
        this.url = url;
    }

    /**
     * @return the HTTP status of the query that caused the exception.
     */
    public int getHttpStatus() {
        return this.httpStatus;
    }

    /**
     * @return the URL queried.
     */
    public URL getUrl() {
        return this.url;
    }
}
