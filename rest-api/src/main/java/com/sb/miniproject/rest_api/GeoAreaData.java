package com.sb.miniproject.rest_api;

import java.io.Serializable;
import java.util.List;

import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

/**
 * Business Object for data of a rectangular area over a range of years
 * @author Prateek Sheel
 *
 */
@XmlRootElement(name = "geoAreaData")
@XmlType(propOrder = { "lattitude1", "longitude1", "lattitude2", "longitude2"
		, "yearFrom", "yearTo", "averageTemp" ,"dataList"})
public class GeoAreaData implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4962277833615961276L;
	
	private String lattitude1;
	private String longitude1;
	private String lattitude2;
	private String longitude2;
	private String yearFrom;
	private String yearTo;
	private String averageTemp;
	
	@XmlElementWrapper(name="dataList")
	private List<Data> dataList;
	
	public GeoAreaData() {
		super();
	}

	public GeoAreaData(String lattitude1, String longitude1, String lattitude2, String longitude2, String yearFrom,
			String yearTo, String averageTemp, List<Data> dataList) {
		super();
		this.lattitude1 = lattitude1;
		this.longitude1 = longitude1;
		this.lattitude2 = lattitude2;
		this.longitude2 = longitude2;
		this.yearFrom = yearFrom;
		this.yearTo = yearTo;
		this.averageTemp = averageTemp;
		this.dataList = dataList;
	}

	public String getLattitude1() {
		return lattitude1;
	}

	public void setLattitude1(String lattitude1) {
		this.lattitude1 = lattitude1;
	}

	public String getLongitude1() {
		return longitude1;
	}

	public void setLongitude1(String longitude1) {
		this.longitude1 = longitude1;
	}

	public String getLattitude2() {
		return lattitude2;
	}

	public void setLattitude2(String lattitude2) {
		this.lattitude2 = lattitude2;
	}

	public String getLongitude2() {
		return longitude2;
	}

	public void setLongitude2(String longitude2) {
		this.longitude2 = longitude2;
	}

	public String getYearFrom() {
		return yearFrom;
	}

	public void setYearFrom(String yearFrom) {
		this.yearFrom = yearFrom;
	}

	public String getYearTo() {
		return yearTo;
	}

	public void setYearTo(String yearTo) {
		this.yearTo = yearTo;
	}

	public List<Data> getDataList() {
		return dataList;
	}

	public void setDataList(List<Data> dataList) {
		this.dataList = dataList;
	}

	public String getAverageTemp() {
		return averageTemp;
	}

	public void setAverageTemp(String averageTemp) {
		this.averageTemp = averageTemp;
	}
	
}
