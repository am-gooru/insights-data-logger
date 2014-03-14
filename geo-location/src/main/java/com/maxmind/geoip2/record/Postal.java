/*******************************************************************************
 * Postal.java
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
/**
 *
 */
package com.maxmind.geoip2.record;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Contains data for the postal record associated with an IP address.
 *
 * This record is returned by all the end points except the Country end point.
 */
final public class Postal {
    @JsonProperty
    private String code;

    @JsonProperty
    private Integer confidence;

    public Postal() {
    }

    /**
     * @return The postal code of the location. Postal codes are not available
     *         for all countries. In some countries, this will only contain part
     *         of the postal code. This attribute is returned by all end points
     *         except the Country end point.
     */
    public String getCode() {
        return this.code;
    }

    /**
     * @return A value from 0-100 indicating MaxMind's confidence that the
     *         postal code is correct. This attribute is only available from the
     *         Omni end point.
     */
    public Integer getConfidence() {
        return this.confidence;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return this.code != null ? this.code : "";
    }
}
