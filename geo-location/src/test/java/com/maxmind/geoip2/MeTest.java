/*******************************************************************************
 * Copyright 2014 Ednovo d/b/a Gooru. All rights reserved.
 * http://www.goorulearning.org/
 *   
 *   MeTest.java
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
package com.maxmind.geoip2;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import com.google.api.client.http.HttpTransport;
import com.maxmind.geoip2.exception.GeoIp2Exception;

public class MeTest {
    private WebServiceClient client;

    @Before
    public void createClient() {
        HttpTransport transport = new TestTransport();

        this.client = new WebServiceClient.Builder(42, "abcdef123456")
                .testTransport(transport).build();

    }

    @Test
    public void omni() throws IOException, GeoIp2Exception {
        assertEquals(this.client.omni().getTraits().getIpAddress(),
                "24.24.24.24");
    }

    @Test
    public void cityIspOrg() throws IOException, GeoIp2Exception {
        assertEquals(this.client.cityIspOrg().getTraits().getIpAddress(),
                "24.24.24.24");
    }

    @Test
    public void city() throws IOException, GeoIp2Exception {
        assertEquals(this.client.city().getTraits().getIpAddress(),
                "24.24.24.24");
    }

    @Test
    public void country() throws IOException, GeoIp2Exception {
        assertEquals(this.client.country().getTraits().getIpAddress(),
                "24.24.24.24");
    }
}
