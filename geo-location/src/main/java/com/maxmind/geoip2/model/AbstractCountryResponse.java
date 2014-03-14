/*******************************************************************************
 * AbstractCountryResponse.java
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
 * This class provides a model for the data returned by the GeoIP2 Omni end
 * point.
 *
 * The only difference between the City, City/ISP/Org, and Omni model classes is
 * which fields in each record may be populated.
 *
 * @see <a href="http://dev.maxmind.com/geoip/geoip2/web-services">GeoIP2 Web
 *      Services</a>
 */
package com.maxmind.geoip2.model;

import java.io.IOException;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.maxmind.geoip2.record.Continent;
import com.maxmind.geoip2.record.Country;
import com.maxmind.geoip2.record.MaxMind;
import com.maxmind.geoip2.record.RepresentedCountry;
import com.maxmind.geoip2.record.Traits;

abstract class AbstractCountryResponse {
    @JsonProperty
    private Continent continent = new Continent();

    @JsonProperty
    private Country country = new Country();

    @JsonProperty("registered_country")
    private Country registeredCountry = new Country();

    @JsonProperty("maxmind")
    private MaxMind maxmind = new MaxMind();

    @JsonProperty("represented_country")
    private RepresentedCountry representedCountry = new RepresentedCountry();

    @JsonProperty
    private Traits traits = new Traits();

    /**
     * @return Continent record for the requested IP address.
     */
    public Continent getContinent() {
        return this.continent;
    }

    /**
     * @return Country record for the requested IP address. This object
     *         represents the country where MaxMind believes the end user is
     *         located.
     */
    public Country getCountry() {
        return this.country;
    }

    /**
     * @return MaxMind record containing data related to your account.
     */
    @JsonProperty("maxmind")
    public MaxMind getMaxMind() {
        return this.maxmind;
    }

    /**
     * @return Registered country record for the requested IP address. This
     *         record represents the country where the ISP has registered a
     *         given IP block and may differ from the user's country.
     */
    public Country getRegisteredCountry() {
        return this.registeredCountry;
    }

    /**
     * @return Represented country record for the requested IP address. The
     *         represented country is used for things like military bases or
     *         embassies. It is only present when the represented country
     *         differs from the country.
     */
    public RepresentedCountry getRepresentedCountry() {
        return this.representedCountry;
    }

    /**
     * @return Record for the traits of the requested IP address.
     */
    public Traits getTraits() {
        return this.traits;
    }

    /**
     * @return JSON representation of this object. The structure is the same as
     *         the JSON provided by the GeoIP2 web service.
     * @throws IOException
     *             if there is an error serializing the object to JSON.
     */
    public String toJson() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(Include.NON_NULL);
        mapper.setSerializationInclusion(Include.NON_EMPTY);
        return mapper.writeValueAsString(this);
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "Country ["
                + (this.getContinent() != null ? "getContinent()="
                        + this.getContinent() + ", " : "")
                + (this.getCountry() != null ? "getCountry()="
                        + this.getCountry() + ", " : "")
                + (this.getRegisteredCountry() != null ? "getRegisteredCountry()="
                        + this.getRegisteredCountry() + ", "
                        : "")
                + (this.getRepresentedCountry() != null ? "getRepresentedCountry()="
                        + this.getRepresentedCountry() + ", "
                        : "")
                + (this.getTraits() != null ? "getTraits()=" + this.getTraits()
                        : "") + "]";
    }
}
