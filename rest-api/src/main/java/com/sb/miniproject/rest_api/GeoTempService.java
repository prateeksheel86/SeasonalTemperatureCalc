package com.sb.miniproject.rest_api;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
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
 * Simple REST end-point for retrieving temperature by geographical coordinates
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
}
