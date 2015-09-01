/*******************************************************************************
 * GeoLocation.java
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
package org.ednovo.data.geo.location;

import java.io.File;
import java.net.InetAddress;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.model.CityResponse;

public class GeoLocation  {

	private static File database;
	private DatabaseReader reader;
	private static final Logger logger = LoggerFactory.getLogger(GeoLocation.class);
	
	private static GeoLocation geoLocation = null;
	
	private GeoLocation() {	}
    
	public static GeoLocation getInstance() {
		if(geoLocation == null) {
			geoLocation = new GeoLocation();
		}
		return geoLocation;
	}
	/**
     * @return a string country database file path from the
     *         field set on the environment.
     * @exception will throw if the database file is not set in Environment. 
     */
    private String getFileNamemmdb(){
		String currPath = System.getenv("GEO_LOCATION_DB_FILE");
		if(StringUtils.isBlank(currPath)) {
			throw new NullPointerException("GeoLocation Database FileName is not specified!");
		}
		return currPath;
    }
    
    /**
     * @return GeoDatabase as a file
     */
    private File getGeoDatabase() {
    	if(database == null) {
    		database = new File(getFileNamemmdb());
    	}
    	return database;  
    }
    
    /**
     * 
     * @return a DatabaseReader with Geo database
     * @throws Exception if there is an error occur while opening or reading file and load it into reader.
     */
    private DatabaseReader getDatabaseReader() throws Exception {
    	if(reader == null) {
    		reader = new DatabaseReader.Builder(getGeoDatabase()).build();
    	}
    	return reader;
    }
    /**
     * @param ip
     *            IP address to lookup.
     * @return A string with the city name for the IP address. Default value is null.
     */
	public String getGeoCityByIP (String ip) {
		String geoCity = null;
    	try {
			CityResponse response = getDatabaseReader().city(InetAddress.getByName(ip.trim()));
			geoCity = response.getCity().getName();
		} catch (Exception e) {
			logger.error("Error while getting Geo City with ip. {}, {}", ip, e);
		} finally {
			closeDatabaseReader();
		}
		return geoCity;
	}

    /**
     * @param ip
     *            IP address to lookup.
     * @return a string with the region name for the IP address. Default value is null.
     */
	public String getGeoRegionByIP (String ip) {
		String geoRegion = null;
    	try {
			CityResponse response = getDatabaseReader().city(InetAddress.getByName(ip.trim()));
			geoRegion = response.getMostSpecificSubdivision().getName();
		} catch (Exception e) {
			logger.error("Error while getting Geo Region with ip {}, {}" , ip, e);
		} finally {
			closeDatabaseReader();
		}
		return geoRegion;
	}
	
    /**
     * @param ip
     *            IP address to lookup.
     * @return A string with the country name for the IP address. Default value is null.
     */
	public String getGeoCountryByIP (String ip) {
		String geoCountry = null;
    	try {
			CityResponse response = getDatabaseReader().city(InetAddress.getByName(ip.trim()));
			geoCountry = response.getCountry().getName();
		} catch (Exception e) {
			logger.error("Error while getting Geo Country with ip {}, {}" , ip, e);
		} finally {
			closeDatabaseReader();
		}
		return geoCountry;
	}

	/**
	 * 
	 * @param ip
	 * 			IP address to lookup.
	 * @return
	 * 			CityResponse from Geo Database for the IP address.
	 */
	public CityResponse getGeoResponse(String ip) {
		CityResponse response = null;
		try {
			response = getDatabaseReader().city(InetAddress.getByName(ip.trim()));
		} catch (Exception e) {
			logger.error("Error while getting Geo Response with ip {}, {}" , ip, e);
		} finally {
			closeDatabaseReader();
		}
		return response;
	}
	
	private void closeDatabaseReader() {
		try {
			reader.close();
		} catch (Exception e) {
			logger.error("Error while closing the Database Reader connection.", e);
		}
	}
}
