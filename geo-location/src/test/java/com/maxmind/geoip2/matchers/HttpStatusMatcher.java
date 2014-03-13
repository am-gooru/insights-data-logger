/*******************************************************************************
 * Copyright 2014 Ednovo d/b/a Gooru. All rights reserved.
 * http://www.goorulearning.org/
 *   
 *   HttpStatusMatcher.java
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
package com.maxmind.geoip2.matchers;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

import com.maxmind.geoip2.exception.HttpException;

public class HttpStatusMatcher extends TypeSafeMatcher<HttpException> {

    private int foundStatusCode;
    private final int expectedStatusCode;

    public static HttpStatusMatcher hasStatus(int item) {
        return new HttpStatusMatcher(item);
    }

    private HttpStatusMatcher(int expectedStatusCode) {
        this.expectedStatusCode = expectedStatusCode;
    }

    @Override
    protected boolean matchesSafely(final HttpException exception) {
        this.foundStatusCode = exception.getHttpStatus();
        return this.foundStatusCode == this.expectedStatusCode;
    }

    @Override
    public void describeTo(Description description) {
        description.appendValue(String.valueOf(this.foundStatusCode))
                .appendText(" was not found instead of ")
                .appendValue(String.valueOf(this.expectedStatusCode));
    }
}
