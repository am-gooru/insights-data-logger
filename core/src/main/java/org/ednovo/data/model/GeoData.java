package org.ednovo.data.model;

import java.io.Serializable;

public class GeoData implements Serializable {

	/**
	 * @author daniel
	 */
	private static final long serialVersionUID = 1L;

	private String country;
	private String state;
	private String city;
	private Double latitude;
	private Double longitude;
	
	public String getCountry() {
		return country;
	}
	public void setCountry(String country) {
		this.country = country;
	}
	public String getState() {
		return state;
	}
	public void setState(String state) {
		this.state = state;
	}
	public String getCity() {
		return city;
	}
	public void setCity(String city) {
		this.city = city;
	}
	public Double getLatitude() {
		return latitude;
	}
	public void setLatitude(Double latitude) {
		this.latitude = latitude;
	}
	public Double getLongitude() {
		return longitude;
	}
	public void setLongitude(Double longitude) {
		this.longitude = longitude;
	}
	
}
