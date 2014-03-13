/*******************************************************************************
 * Copyright 2014 Ednovo d/b/a Gooru. All rights reserved.
 * http://www.goorulearning.org/
 *   
 *   GeoLocation.java
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
package org.ednovo.data.geo.location;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;

//import org.logger.event.cassandra.loader.CassandraDataLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.DatabaseReader;


public class GeoLocation  {

	private static String fileNameCity = "/home/gooruapp/event-logger-stable-1.1/loader/GeoLite2-City.mmdb";
    private static final Logger logger = LoggerFactory.getLogger(GeoLocation.class);

    /**
     * @return a string country database name from the
     *         field set on the environment.
     */
    
    public String getFileNamemmdb(){
		String currPath = "";
		currPath = System.getenv("GEO_LOCATION_DB_FILE");
		logger.info("GEO_LOCATION_DB_FILE = {}", currPath);
		return (currPath);
    }
    
    /**
     * @param ip
     *            IP address to lookup.
     * @return A string with the city name for the IP address
     * @throws IOException
     *             if there is an error opening or reading from the file.
     * @throws GeoIp2Exception
     *             if the IP address is not in country database
     */
	public String getGeoCityByIP (String ip) throws IOException, GeoIp2Exception {
		ip = ip.trim();
		String City = null;
		File database = new File(getFileNamemmdb());
		DatabaseReader reader = new DatabaseReader.Builder(database).build();
		
    	try {
			CityResponse response = reader.city(InetAddress.getByName(ip));
			City = response.getCity().getName();
		} catch (IOException e) {
			reader.close();
			return (City);
		} catch (GeoIp2Exception e) {
			reader.close();
			return (City);
		}
	
		reader.close();
		return (City);
	}

    /**
     * @param ip
     *            IP address to lookup.
     * @return a string with the region name for the IP address
     * @throws IOException
     *             if there is an error opening or reading from the file.
     * @throws GeoIp2Exception
     *             if the IP address is not in country database
     */
	public String getGeoRegionByIP (String ip) throws IOException, GeoIp2Exception {
		ip = ip.trim();
		String Region = null;
		File database = new File(getFileNamemmdb());
		DatabaseReader reader = new DatabaseReader.Builder(database).build();
    	try {
			CityResponse response = reader.city(InetAddress.getByName(ip));
			Region = response.getMostSpecificSubdivision().getName();
		} catch (IOException e) {
			reader.close();
			return (Region);
		} catch (GeoIp2Exception e) {
			reader.close();
			return (Region);
		}
		reader.close();
		return (Region);
	}
	
    /**
     * @param ip
     *            IP address to lookup.
     * @return A string with the country name for the IP address
     * @throws IOException
     *             if there is an error opening or reading from the file.
     * @throws GeoIp2Exception
     *             if the IP address is not in country database
     */
	public String getGeoCountryByIP (String ip) throws IOException, GeoIp2Exception {
		ip = ip.trim();
		String Country = null;
		File database = new File(getFileNamemmdb());
		DatabaseReader reader = new DatabaseReader.Builder(database).build();
    	try {
			CityResponse response = reader.city(InetAddress.getByName(ip));
			Country = response.getCountry().getName();
		} catch (IOException e) {
			reader.close();
			return (Country);
		} catch (GeoIp2Exception e) {
			reader.close();
			return (Country);
		}
		reader.close();
		return (Country);
	}

}
