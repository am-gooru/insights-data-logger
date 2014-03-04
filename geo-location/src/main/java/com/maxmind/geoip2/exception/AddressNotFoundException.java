/*******************************************************************************
 * AddressNotFoundException.java
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

/**
 * This exception is thrown when the IP address is not found in the database.
 * This generally means that the address was a private or reserved address.
 */
final public class AddressNotFoundException extends GeoIp2Exception {

    private static final long serialVersionUID = -639962574626980783L;

    /**
     * @param message
     *            A message explaining the cause of the error.
     */
    public AddressNotFoundException(String message) {
        super(message);
    }

    /**
     * @param message
     *            A message explaining the cause of the error.
     * @param e
     *            The cause of the exception.
     */
    public AddressNotFoundException(String message, Throwable e) {
        super(message, e);
    }
}
