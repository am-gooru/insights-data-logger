/*******************************************************************************
 * Copyright 2014 Ednovo d/b/a Gooru. All rights reserved.
 * http://www.goorulearning.org/
 *   
 *   DatabaseReader.java
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

package com.maxmind.geoip2;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.maxmind.db.Reader;
import com.maxmind.db.Reader.FileMode;
import com.maxmind.geoip2.exception.AddressNotFoundException;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityIspOrgResponse;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.model.CountryResponse;
import com.maxmind.geoip2.model.OmniResponse;

/**
 * Instances of this class provide a reader for the GeoIP2 database format. IP
 * addresses can be looked up using the <code>get</code> method.
 */
public class DatabaseReader implements GeoIp2Provider, Closeable {

    private final Reader reader;

    private final ObjectMapper om;

    DatabaseReader(Builder builder) throws IOException {
        if (builder.stream != null) {
            this.reader = new Reader(builder.stream);
        } else if (builder.database != null) {
            this.reader = new Reader(builder.database, builder.mode);
        } else {
            // This should never happen. If it does, review the Builder class
            // constructors for errors.
            throw new IllegalArgumentException(
                    "Unsupported Builder configuration: expected either File or URL");
        }
        this.om = new ObjectMapper();
        this.om.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
                false);
        InjectableValues inject = new InjectableValues.Std().addValue(
                "locales", builder.locales);
        this.om.setInjectableValues(inject);
    }

    /**
     * Constructs a Builder for the DatabaseReader. The file passed to it must
     * be a valid GeoIP2 database file.
     *
     * <code>Builder</code> creates instances of
     * <code>DatabaseReader</client> from values set by the methods.
     *
     * Only the values set in the <code>Builder</code> constructor are
     * required.
     */
    public final static class Builder {
        final File database;
        final InputStream stream;

        List<String> locales = Arrays.asList("en");
        FileMode mode = FileMode.MEMORY_MAPPED;

        /**
         * @param stream
         *            the stream containing the GeoIP2 database to use.
         */
        public Builder(InputStream stream) {
            this.stream = stream;
            this.database = null;
        }

        /**
         * @param database
         *            the GeoIP2 database file to use.
         */
        public Builder(File database) {
            this.database = database;
            this.stream = null;
        }

        /**
         * @param val
         *            List of locale codes to use in name property from most
         *            preferred to least preferred.
         */
        public Builder locales(List<String> val) {
            this.locales = val;
            return this;
        }

        /**
         * @param val
         *            The file mode used to open the GeoIP2 database
         * @throws java.lang.IllegalArgumentException
         *             if you initialized the Builder with a URL, which uses
         *             {@link FileMode#MEMORY}, but you provided a different
         *             FileMode to this method.
         * */
        public Builder fileMode(FileMode val) {
            if (this.stream != null && !FileMode.MEMORY.equals(val)) {
                throw new IllegalArgumentException(
                        "Only FileMode.MEMORY is supported when using an InputStream.");
            }
            this.mode = val;
            return this;
        }

        /**
         * @return an instance of <code>DatabaseReader</code> created from the
         *         fields set on this builder.
         * @throws IOException
         */
        public DatabaseReader build() throws IOException {
            return new DatabaseReader(this);
        }
    }

    /**
     * @param ipAddress
     *            IPv4 or IPv6 address to lookup.
     * @return A <T> object with the data for the IP address
     * @throws IOException
     *             if there is an error opening or reading from the file.
     * @throws AddressNotFoundException
     *             if the IP address is not in our database
     */
    private <T> T get(InetAddress ipAddress, Class<T> cls) throws IOException,
            AddressNotFoundException {
        ObjectNode node = (ObjectNode) this.reader.get(ipAddress);

        // We throw the same exception as the web service when an IP is not in
        // the database
        if (node == null) {
            throw new AddressNotFoundException("The address "
                    + ipAddress.getHostAddress() + " is not in the database.");
        }

        if (!node.has("traits")) {
            node.put("traits", this.om.createObjectNode());
        }
        ObjectNode traits = (ObjectNode) node.get("traits");
        traits.put("ip_address", ipAddress.getHostAddress());

        // The cast and the Omni.class are sort of ugly. There might be a
        // better way
        return this.om.treeToValue(node, cls);
    }

    /**
     * Closes the GeoIP2 database and returns resources to the system.
     *
     * @throws IOException
     *             if an I/O error occurs.
     */
    @Override
    public void close() throws IOException {
        this.reader.close();
    }

    @Override
    public CountryResponse country(InetAddress ipAddress) throws IOException,
            GeoIp2Exception {
        return this.get(ipAddress, CountryResponse.class);
    }

    @Override
    public CityResponse city(InetAddress ipAddress) throws IOException,
            GeoIp2Exception {
        return this.get(ipAddress, CityResponse.class);
    }

    @Override
    public CityIspOrgResponse cityIspOrg(InetAddress ipAddress)
            throws IOException, GeoIp2Exception {
        return this.get(ipAddress, CityIspOrgResponse.class);
    }

    @Override
    public OmniResponse omni(InetAddress ipAddress) throws IOException,
            GeoIp2Exception {
        return this.get(ipAddress, OmniResponse.class);
    }
}
