package com.sb.miniproject

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Prateek Sheel on 5/20/17.
  */
object AverageSeasonalTempIngestion {

  def main(args: Array[String]): Unit = {

    if(args.length != 2) {
      println("Error: Invalid number of arguments")
      println("Usage: Arg1 = hdfs path to data file")
      println("Usage: Arg2 = parallelism")
      return
    }

    val dataFilePath = args(0)
    val parallelism = args(1).toInt

    val sparkConf = new SparkConf().setAppName("AvgSeasonalTemp").setMaster("local[*]").set("spark.storage.memoryFraction", "1")
    val sc = new SparkContext(sparkConf)
    val doubleRegex = "[-+]?\\d+(\\.\\d+)?"

    case class Observation (key :String, station_code :String, year :Int, month :Int, season :String, precipitation :Double, avg_temp :Double)

    val tempFile = sc.textFile(dataFilePath).repartition(parallelism)
    val filterHeaders = tempFile.filter(t => !t.contains("STATION"))
    val split = filterHeaders.map(t => t.split("\",\"")) // Split each line by the "," substring
    val numElementsCheck = split.filter(t => t.length >= 108) // Reject records if Temperature reading is not available
    val combined = numElementsCheck.map(t => Array(t(0).replace("\"", "")) ++ t(1).split("-") ++ t.slice(102, 103) ++ t.slice (106,107))
    // Length should be 8: Station ID, Year, Month, lattitude, longitude, Address, PRCP, TAVG
    val filtered = combined.filter(t => t.length == 5).filter(t => t(4).matches(doubleRegex))
    val mapped = filtered.map(o => Observation("" + o(1).toInt + ":" + o(2).toInt + ":" + o(0), // Key = Year:Month:StationCode
      o(0), //Station Code
      o(1).toInt, //Year
      o(2).toInt, //Month
      o(2).toInt match {case (1) => "WINTER" case(2) => "WINTER" case(12) => "WINTER" //SEASON
      case (3) => "SPRING" case (4) => "SPRING" case(5) => "SPRING"
      case(6) => "SUMMER" case (7) => "SUMMER" case (8) => "SUMMER"
      case (9) => "FALL" case (10) => "FALL" case(11) => "FALL" case(_) => ""},
      o(3) match { case ("") => Double.NaN case(_) => o(3).toDouble }, //Precipitation
      o(4).toDouble)) //Average Temperature

    //Map the RDD to a structure as follows: Key Value ColumnFamily Column
    val keyRDD = mapped.flatMap(t => {
      Seq(
        (t.key, t.avg_temp, "WEATHER_DATA", "AVG_TEMP"),
        (t.key, t.month, "ID_DETAILS", "MONTH"),
        (t.key, t.precipitation, "WEATHER_DATA", "PRECIPITATION"),
        (t.key, t.season, "WEATHER_DATA", "SEASON"),
        (t.key, t.station_code, "ID_DETAILS", "STATION_CODE"),
        (t.key, t.year, "ID_DETAILS", "YEAR")
      )
    }).sortBy(_._1)

//    val rddContent = keyRDD.collect()
//    rddContent.foreach(t => println(t.toString()))

    // Create HBase Configuration and related objects
    val conf = HBaseConfiguration.create()
    val tableName = "SEASONAL_TEMP"
    val connection = ConnectionFactory.createConnection(conf)
    val table = connection.getTable(TableName.valueOf(tableName))

    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    val job = Job.getInstance(conf)
    job.setMapOutputKeyClass (classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass (classOf[KeyValue])
    HFileOutputFormat2.configureIncrementalLoadMap(job, table)

    // Create the key value pairs for insertion into HFile
    val putRdd = keyRDD.map( t => {
      val kv :KeyValue = new KeyValue(Bytes.toBytes(t._1), Bytes.toBytes(t._3),
        Bytes.toBytes(t._4), Bytes.toBytes(t._2.toString))

      (new ImmutableBytesWritable(Bytes.toBytes(t._1)), kv)
    })

    putRdd.saveAsNewAPIHadoopFile("hdfs://quickstart.cloudera:8020/user/cloudera/avgSeasonalTemp/hbase",
      classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], conf)
  }
}
