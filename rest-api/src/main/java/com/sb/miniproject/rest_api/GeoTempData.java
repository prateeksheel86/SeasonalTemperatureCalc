package com.sb.miniproject.rest_api;

import java.util.List;

import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

/**
 * Container for REST API Data
 * @author Prateek Sheel
 *
 */
@XmlRootElement(name = "geoTempData")
@XmlType(propOrder = { "lattitude", "longitude", "dataList"})
public class GeoTempData {
	
	private String lattitude;
	private String longitude;

	@XmlElementWrapper(name="dataList")
	private List<Data> dataList;

	public GeoTempData(String lattitude, String longitude, List<Data> dataList) {
		super();
		this.lattitude = lattitude;
		this.longitude = longitude;
		this.dataList = dataList;
	}

	public GeoTempData() {
		super();
		// TODO Auto-generated constructor stub
	}

	public String getLattitude() {
		return lattitude;
	}

	public void setLattitude(String lattitude) {
		this.lattitude = lattitude;
	}

	public String getLongitude() {
		return longitude;
	}

	public void setLongitude(String longitude) {
		this.longitude = longitude;
	}

	public List<Data> getDataList() {
		return dataList;
	}

	public void setDataList(List<Data> dataList) {
		this.dataList = dataList;
	} 
}
