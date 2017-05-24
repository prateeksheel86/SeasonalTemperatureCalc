package com.sb.miniproject.rest_api;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Simple REST end-point for retrieving weather data by geographical coordinates
 * 
 * @author Prateek Sheel
 *
 */
@Path("geoTempService")
public class GeoTempService {

	@GET
	@Path("/hello")
	@Produces(MediaType.TEXT_PLAIN)
	public String hello(){
		return "Hello There!";
	}
	
	/**
	 * Retrieve data for a coordinate from HBase
	 */
	@GET
	@Path("/getDataForCoordinate/{lat}/{lon}")
	@Produces(MediaType.APPLICATION_JSON)
	public GeoTempData getDataForCoordinate(@PathParam("lat") String lat, @PathParam("lon") String lon)
	{
		GeoTempData returnValue = new GeoTempData();

		final String tableName = "SUMMARY";
		Table table = null;
		ResultScanner scanner = null;
		Connection connection = null;
		try{
			//Create HBase Configuration Object and HTable Object
			Configuration conf = HBaseConfiguration.create();
			connection = ConnectionFactory.createConnection(conf);
			table = connection.getTable(TableName.valueOf(tableName));

			Scan scan = new Scan();
			scan.addFamily(Bytes.toBytes("SUMMARY_DETAILS"));
			scan.setMaxVersions();
			SingleColumnValueFilter filter1 = new SingleColumnValueFilter(Bytes.toBytes("SUMMARY_DETAILS"),
					Bytes.toBytes("LATTITUDE"), CompareOp.EQUAL, Bytes.toBytes(lat));
			SingleColumnValueFilter filter2 = new SingleColumnValueFilter(Bytes.toBytes("SUMMARY_DETAILS"),
					Bytes.toBytes("LONGITUDE"), CompareOp.EQUAL, Bytes.toBytes(lon));
			FilterList filterList = new FilterList();
			filterList.addFilter(filter1);
			filterList.addFilter(filter2);
			scan.setFilter(filterList);
			
			returnValue.setLattitude(lat);
			returnValue.setLongitude(lon);
			List<Data> dataList = new ArrayList<Data>();
			scanner = table.getScanner(scan);
			for(Result res: scanner){
//				System.out.println(res);
                Data data = new Data();
                data.setYear(Bytes.toString(CellUtil.cloneValue(res.getColumnLatestCell
                		(Bytes.toBytes("SUMMARY_DETAILS"), Bytes.toBytes("YEAR")))));
                data.setSeason(Bytes.toString(CellUtil.cloneValue(res.getColumnLatestCell
                		(Bytes.toBytes("SUMMARY_DETAILS"), Bytes.toBytes("SEASON")))));
                data.setAvgTemp(Bytes.toString(CellUtil.cloneValue(res.getColumnLatestCell
                		(Bytes.toBytes("SUMMARY_DETAILS"), Bytes.toBytes("AVG_TEMP")))));
                data.setDataPoints(Bytes.toString(CellUtil.cloneValue(res.getColumnLatestCell
                		(Bytes.toBytes("SUMMARY_DETAILS"), Bytes.toBytes("DATA_POINTS")))));
                data.setStations(Bytes.toString(CellUtil.cloneValue(res.getColumnLatestCell
                		(Bytes.toBytes("SUMMARY_DETAILS"), Bytes.toBytes("STATIONS_LIST")))));
                dataList.add(data);
            }
			returnValue.setDataList(dataList);

		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			if(null != table){
				try{
					table.close();
				} catch (IOException ioEx){
					ioEx.printStackTrace();
				}
			}
			if(null != scanner){
				try{
					scanner.close();
				} catch (Exception ioEx){
					ioEx.printStackTrace();
				}
			}
			if(null != connection){
				try{
					connection.close();
				} catch(IOException ioEx){
					ioEx.printStackTrace();
				}
			}
		}
		
		return returnValue;
	}
	
	/**
	 * Retrieve data for a rectangular area, over the range of years specified by year1 and year2
	 * This API returns the underlying data and the average temperature over the input range
	 * The calculation of the average takes into account the possibility of overflow due to too many records
	 * by dividing first and then adding. The modulo for the division is retained to avoid loss of precision
	 */
	@GET
	@Path("/getDataForArea")
	@Produces(MediaType.APPLICATION_JSON)
	public GeoAreaData getDataForArea(
		@QueryParam("lat1") String lat1, 
		@QueryParam("lon1") String lon1,
		@QueryParam("lat2") String lat2,
		@QueryParam("lon2") String lon2,
		@QueryParam("year1") String year1,
		@QueryParam("year2") String year2) throws BadRequestException
	{
		GeoAreaData returnValue = new GeoAreaData();
		
		Double startLattitude = null;
		Double endLattitude = null;
		Double startLongitude = null;
		Double endLongitude = null;
		int startYear;
		int endYear;
		
		boolean validInput = false;
		String error = null;
		try{
			validInput = validateAreaInput(lat1, lon1, lat2, lon2, year1, year2);
		} catch (Exception ex){
			error = ex.getMessage();
		}
		
		if(!validInput){		
			if(null != error){
				throw new BadRequestException(error);
			} else {
				throw new BadRequestException("Invalid Request");
			}
		} else {
			startLattitude = Double.parseDouble(lat1);
			endLattitude = Double.parseDouble(lat2);
			if (startLattitude.compareTo(endLattitude) > 0){
				// Swap the values
				Double temp = startLattitude;
				startLattitude = endLattitude;
				endLattitude = temp;
			}
			
			startLongitude = Double.parseDouble(lon1);
			endLongitude = Double.parseDouble(lon2);
			if(startLongitude.compareTo(endLongitude) > 0){
				Double temp = startLongitude;
				startLongitude = endLongitude;
				endLongitude = temp;
			}
			
			startYear = Integer.parseInt(year1);
			endYear = Integer.parseInt(year2);
			if(startYear > endYear){
				int temp = startYear;
				startYear = endYear;
				endYear = temp;
			}
		}
		 
		final String tableName = "SUMMARY";
		Table table = null;
		ResultScanner scanner = null;
		Connection connection = null;
		try{
			//Create HBase Configuration Object and HTable Object
			Configuration conf = HBaseConfiguration.create();
			connection = ConnectionFactory.createConnection(conf);
			table = connection.getTable(TableName.valueOf(tableName));

			Scan scan = new Scan();
			scan.addFamily(Bytes.toBytes("SUMMARY_DETAILS"));
			scan.setMaxVersions();
			
			// Create the filters for latitude, longitude and year
			SingleColumnValueFilter filter1 = new SingleColumnValueFilter(Bytes.toBytes("SUMMARY_DETAILS"),
					Bytes.toBytes("LATTITUDE"), CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(startLattitude.toString()));
			SingleColumnValueFilter filter2 = new SingleColumnValueFilter(Bytes.toBytes("SUMMARY_DETAILS"),
					Bytes.toBytes("LATTITUDE"), CompareOp.LESS_OR_EQUAL, Bytes.toBytes(endLattitude.toString()));
			SingleColumnValueFilter filter3 = new SingleColumnValueFilter(Bytes.toBytes("SUMMARY_DETAILS"),
					Bytes.toBytes("LONGITUDE"), CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(startLongitude.toString()));
			SingleColumnValueFilter filter4 = new SingleColumnValueFilter(Bytes.toBytes("SUMMARY_DETAILS"),
					Bytes.toBytes("LONGITUDE"), CompareOp.LESS_OR_EQUAL, Bytes.toBytes(endLongitude.toString()));
			SingleColumnValueFilter filter5 = new SingleColumnValueFilter(Bytes.toBytes("SUMMARY_DETAILS"),
					Bytes.toBytes("YEAR"), CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(Integer.toString(startYear)));
			SingleColumnValueFilter filter6 = new SingleColumnValueFilter(Bytes.toBytes("SUMMARY_DETAILS"),
					Bytes.toBytes("YEAR"), CompareOp.LESS_OR_EQUAL, Bytes.toBytes(Integer.toString(endYear)));
			FilterList filterList = new FilterList();
			filterList.addFilter(filter1);
			filterList.addFilter(filter2);
			filterList.addFilter(filter3);
			filterList.addFilter(filter4);
			filterList.addFilter(filter5);
			filterList.addFilter(filter6);
			scan.setFilter(filterList);
			
			//Set the basic values in the response
			returnValue.setLattitude1(startLattitude.toString());
			returnValue.setLongitude1(startLongitude.toString());
			returnValue.setLattitude2(endLattitude.toString());
			returnValue.setLongitude2(endLongitude.toString());
			returnValue.setYearFrom(Integer.toString(startYear));
			returnValue.setYearTo(Integer.toString(endYear));
			
			List<Data> dataList = new ArrayList<Data>();
			scanner = table.getScanner(scan);
			for(Result res: scanner){
//				System.out.println(res);
                Data data = new Data();
                data.setYear(Bytes.toString(CellUtil.cloneValue(res.getColumnLatestCell
                		(Bytes.toBytes("SUMMARY_DETAILS"), Bytes.toBytes("YEAR")))));
                data.setSeason(Bytes.toString(CellUtil.cloneValue(res.getColumnLatestCell
                		(Bytes.toBytes("SUMMARY_DETAILS"), Bytes.toBytes("SEASON")))));
                data.setAvgTemp(Bytes.toString(CellUtil.cloneValue(res.getColumnLatestCell
                		(Bytes.toBytes("SUMMARY_DETAILS"), Bytes.toBytes("AVG_TEMP")))));
                data.setDataPoints(Bytes.toString(CellUtil.cloneValue(res.getColumnLatestCell
                		(Bytes.toBytes("SUMMARY_DETAILS"), Bytes.toBytes("DATA_POINTS")))));
                data.setStations(Bytes.toString(CellUtil.cloneValue(res.getColumnLatestCell
                		(Bytes.toBytes("SUMMARY_DETAILS"), Bytes.toBytes("STATIONS_LIST")))));
                dataList.add(data);
            }
			returnValue.setDataList(dataList);
			
			// Calculate the average over the area by adding the quotient and remainder for the division of
			// each individual average value. This is to avoid overflow as well as to retain precision
			int numberOfRecords = dataList.size();
			int averageIntPart = 0;
			Double averageDecimalPart = new Double(0);
			
			for(Data data : dataList){
				String strAvg = data.getAvgTemp();
				
				try{
					Double avg = Double.parseDouble(strAvg) / numberOfRecords;
					averageIntPart += avg.intValue();
					averageDecimalPart += (avg - avg.intValue());
					
				} catch (Exception ex){
					// Do Nothing
				}
			}
			
			Double average = (double) averageIntPart + averageDecimalPart;
			
			returnValue.setAverageTemp(average.toString());

		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			if(null != table){
				try{
					table.close();
				} catch (IOException ioEx){
					ioEx.printStackTrace();
				}
			}
			if(null != scanner){
				try{
					scanner.close();
				} catch (Exception ioEx){
					ioEx.printStackTrace();
				}
			}
			if(null != connection){
				try{
					connection.close();
				} catch(IOException ioEx){
					ioEx.printStackTrace();
				}
			}
		}
		
		return returnValue;
	}
	
	/**
	 * Validate the input for area API
	 * @param lat1
	 * @param lon1
	 * @param lat2
	 * @param lon2
	 * @param year1
	 * @param year2
	 * @return
	 * @throws Exception
	 */
	private boolean validateAreaInput(String lat1, String lon1, String lat2, String lon2, String year1, String year2) throws Exception
	{
		boolean retVal = true;
		
		Double double_lat1 = Double.parseDouble(lat1);
		Double double_lon1 = Double.parseDouble(lon1);
		Double double_lat2 = Double.parseDouble(lat2);
		Double double_lon2 = Double.parseDouble(lon2);
		int int_year1 = Integer.parseInt(year1);
		int int_year2 = Integer.parseInt(year2);		
		
		// Check if both the points are the same
		if(double_lat1.equals(double_lat2) && double_lon1.equals(double_lon2)){
			throw new Exception("Both coordiantes are the same.");
		}
		
		// Check if the year input is > 1900 because there is no data for years before 1900
		if(int_year1 < 1900 || int_year2 < 1900){
			throw new Exception("Year1 or Year2 is less than 1900. No data exists.");
		}
		
		return retVal;
	}
}
