package com.sb.miniproject.rest_api;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlType;

/**
 * POJO for data
 * @author Prateek Sheel
 *
 */
@XmlType(propOrder = { "year", "season", "avgTemp", "dataPoints", "stations"})
public class Data implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private String year;
	private String season;
	private String avgTemp;
	private String dataPoints;
	private String stations;
		
	public Data() {
		super();
	}

	public Data(String year, String season, String avgTemp, String dataPoints, String stations) {
		super();
		this.year = year;
		this.season = season;
		this.avgTemp = avgTemp;
		this.dataPoints = dataPoints;
		this.stations = stations;
	}

	public String getYear() {
		return year;
	}

	public void setYear(String year) {
		this.year = year;
	}

	public String getSeason() {
		return season;
	}

	public void setSeason(String season) {
		this.season = season;
	}

	public String getAvgTemp() {
		return avgTemp;
	}

	public void setAvgTemp(String avgTemp) {
		this.avgTemp = avgTemp;
	}

	public String getDataPoints() {
		return dataPoints;
	}

	public void setDataPoints(String dataPoints) {
		this.dataPoints = dataPoints;
	}

	public String getStations() {
		return stations;
	}

	public void setStations(String stations) {
		this.stations = stations;
	}
	
}
