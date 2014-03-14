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

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;

import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.DatabaseReader;


public class GeoLocation  {

	private static String fileNameCity = "/var/www/donedivineorder/tmp/GeoLite2-City.mmdb";

	public String getGeoCityByIP (String ip) throws IOException, GeoIp2Exception {
		String City;
		File database = new File(fileNameCity);
		DatabaseReader reader = new DatabaseReader.Builder(database).build();
		
		CityResponse response = reader.city(InetAddress.getByName(ip));

		City = response.getCity().getName();
		reader.close();
		
		if (City == null) City = "";
		return (City);
	}

	public String getGeoRegionByIP (String ip) throws IOException, GeoIp2Exception {
		String Region;
		File database = new File(fileNameCity);
		DatabaseReader reader = new DatabaseReader.Builder(database).build();
		
		CityResponse response = reader.city(InetAddress.getByName(ip));

		Region = response.getMostSpecificSubdivision().getName();
		reader.close();
		
		if (Region == null) Region = "";
		return (Region);
	}
	
	public String getGeoCountryByIP (String ip) throws IOException, GeoIp2Exception {
		String Country;
		File database = new File(fileNameCity);
		DatabaseReader reader = new DatabaseReader.Builder(database).build();
		
		CityResponse response = reader.city(InetAddress.getByName(ip));

		Country = response.getCountry().getName();
		reader.close();
		
		return (Country);
		
	}

}
