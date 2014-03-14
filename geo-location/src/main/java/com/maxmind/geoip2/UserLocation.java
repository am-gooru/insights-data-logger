/*******************************************************************************
 * UserLocation.java
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

import java.io.IOException;

import org.ednovo.data.geo.location.GeoLocation;
import com.maxmind.geoip2.exception.GeoIp2Exception;


public class UserLocation {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws GeoIp2Exception 
	 * @throws IOException1 
	 */
	public static void main(String[] args) throws IOException, GeoIp2Exception {
		// TODO Auto-generated method stub

		GeoLocation geo = new GeoLocation();
		String ip = "178.79.161.206";
		
		ip = "192.241.223.155";
		
		System.out.println (geo.getGeoCityByIP (ip));
		System.out.println (geo.getGeoRegionByIP (ip));
		System.out.println (geo.getGeoCountryByIP (ip));
	}
	
	
}
