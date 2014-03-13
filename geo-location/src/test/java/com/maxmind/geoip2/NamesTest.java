/*******************************************************************************
 * Copyright 2014 Ednovo d/b/a Gooru. All rights reserved.
 * http://www.goorulearning.org/
 *   
 *   NamesTest.java
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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;

import org.junit.Test;

import com.google.api.client.http.HttpTransport;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityIspOrgResponse;

public class NamesTest {
    HttpTransport transport = new TestTransport();

    @Test
    public void testNames() throws IOException, GeoIp2Exception {
        WebServiceClient client = new WebServiceClient.Builder(42,
                "abcdef123456").testTransport(this.transport)
                .locales(Arrays.asList("zh-CN", "ru")).build();

        CityIspOrgResponse cio = client.cityIspOrg(InetAddress
                .getByName("1.1.1.2"));
        assertEquals("country.getContinent().getName() does not return 北美洲",
                "北美洲", cio.getContinent().getName());
        assertEquals("country.getCountry().getName() does not return 美国", "美国",
                cio.getCountry().getName());
        assertEquals("toString() returns getName()",
                cio.getCountry().getName(), cio.getCountry().getName());
    }

    @Test
    public void russianFallback() throws IOException, GeoIp2Exception {
        WebServiceClient client = new WebServiceClient.Builder(42,
                "abcdef123456").testTransport(this.transport)
                .locales(Arrays.asList("as", "ru")).build();

        CityIspOrgResponse cio = client.cityIspOrg(InetAddress
                .getByName("1.1.1.2"));
        assertEquals(
                "country.getCountry().getName() does not return объединяет государства",
                "объединяет государства", cio.getCountry().getName());

    }

    @Test
    public void testFallback() throws IOException, GeoIp2Exception {
        WebServiceClient client = new WebServiceClient.Builder(42,
                "abcdef123456").testTransport(this.transport)
                .locales(Arrays.asList("pt", "en", "zh-CN")).build();
        CityIspOrgResponse cio = client.cityIspOrg(InetAddress
                .getByName("1.1.1.2"));
        assertEquals("en is returned when pt is missing", cio.getContinent()
                .getName(), "North America");

    }

    @Test
    public void noFallback() throws IOException, GeoIp2Exception {
        WebServiceClient client = new WebServiceClient.Builder(42,
                "abcdef123456").testTransport(this.transport)
                .locales(Arrays.asList("pt", "es", "af")).build();
        CityIspOrgResponse cio = client.cityIspOrg(InetAddress
                .getByName("1.1.1.2"));

        assertNull("null is returned when locale is not available", cio
                .getContinent().getName());
    }

    @Test
    public void noLocale() throws IOException, GeoIp2Exception {
        WebServiceClient client = new WebServiceClient.Builder(42,
                "abcdef123456").testTransport(this.transport).build();
        CityIspOrgResponse cio = client.cityIspOrg(InetAddress
                .getByName("1.1.1.2"));
        assertEquals("en is returned when no locales are specified", cio
                .getContinent().getName(), "North America");

    }

    @Test
    public void testMissing() throws IOException, GeoIp2Exception {
        WebServiceClient client = new WebServiceClient.Builder(42,
                "abcdef123456").testTransport(this.transport)
                .locales(Arrays.asList("en")).build();

        CityIspOrgResponse cio = client.cityIspOrg(InetAddress
                .getByName("1.1.1.2"));
        assertNotNull(cio.getCity());
        assertNull("null is returned when names object is missing", cio
                .getCity().getName());
    }
}
