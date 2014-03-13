/*******************************************************************************
 * Copyright 2014 Ednovo d/b/a Gooru. All rights reserved.
 * http://www.goorulearning.org/
 *   
 *   CountryTest.java
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
import java.net.InetAddress;

import org.junit.Before;
import org.junit.Test;

import com.google.api.client.http.HttpTransport;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CountryResponse;

public class CountryTest {
    private CountryResponse country;

    @Before
    public void setUp() throws IOException, GeoIp2Exception {
        HttpTransport transport = new TestTransport();
        WebServiceClient client = new WebServiceClient.Builder(42,
                "abcdef123456").testTransport(transport).build();

        this.country = client.country(InetAddress.getByName("1.1.1.3"));
    }

    @SuppressWarnings("boxing")
    @Test
    public void testContinent() {
        assertEquals("country.getContinent().getCode() does not return NA",
                "NA", this.country.getContinent().getCode());
        assertEquals(
                "country.getContinent().getGeoNameId() does not return 42", 42,
                (int) this.country.getContinent().getGeoNameId());
        assertEquals(
                "country.getContinent().getName() does not return North America",
                "North America", this.country.getContinent().getName());
    }

    @SuppressWarnings("boxing")
    @Test
    public void testCountry() {

        assertEquals("country.getCountry().getCode() does not return US", "US",
                this.country.getCountry().getIsoCode());
        assertEquals("country.getCountry().getGeoNameId() does not return 1",
                1, (int) this.country.getCountry().getGeoNameId());
        assertEquals("country.getCountry().getConfidence() does not return 56",
                new Integer(56), this.country.getCountry().getConfidence());
        assertEquals(
                "country.getCountry().getName(\"en\") does not return United States",
                "United States", this.country.getCountry().getName());
    }

    @SuppressWarnings("boxing")
    @Test
    public void testRegisteredCountry() {
        assertEquals(
                "country.getRegisteredCountry().getIsoCode() does not return CA",
                "CA", this.country.getRegisteredCountry().getIsoCode());
        assertEquals(
                "country.getRegisteredCountry().getGeoNameId() does not return 2",
                2, (int) this.country.getRegisteredCountry().getGeoNameId());
        assertEquals(
                "country.getRegisteredCountry().getName(\"en\") does not return United States",
                "Canada", this.country.getRegisteredCountry().getName());
    }

    @SuppressWarnings("boxing")
    @Test
    public void testRepresentedCountry() {
        assertEquals(
                "country.getRepresentedCountry().getCode() does not return GA",
                "GB", this.country.getRepresentedCountry().getIsoCode());
        assertEquals(
                "country.getRepresentedCountry().getGeoNameId() does not return 4",
                4, (int) this.country.getRepresentedCountry().getGeoNameId());
        assertEquals(
                "country.getRepresentedCountry().getName(\"en\") does not return United Kingdom",
                "United Kingdom", this.country.getRepresentedCountry()
                        .getName());
        assertEquals(
                "country.getRepresentedCountry().getType() does not return military",
                "military", this.country.getRepresentedCountry().getType());
    }

    @Test
    public void testTraits() {

        assertEquals(
                "country.getTraits().getIpAddress does not return 1.2.3.4",
                "1.2.3.4", this.country.getTraits().getIpAddress());

    }

}
