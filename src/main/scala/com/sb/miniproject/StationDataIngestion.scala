package com.sb.miniproject

import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Prateek Sheel on 5/21/17.
  */
object StationDataIngestion {

  case class Station (ID :String, lattitude :Double, longitude :Double, elevation :String, state :String,
                        stn_name :String, GSN :String, HCN_CRN: String, WMO_ID :String, country_cd :String, country_name :String)
  case class Country (code :String, name :String)

  def main(args: Array[String]): Unit = {

    if(args.length != 3) {
      println("Error: Invalid number of arguments")
      println("Usage: Arg1 = hdfs path to stations file")
      println("Usage: Arg2 = hdfs path to countries file")
      println("Usage: Arg3 = parallelism")
      return
    }

    val stationsFilePath = args(0)
    val countriesFilePath = args(1)
    val parallelism = args(2).toInt


    val sparkConf = new SparkConf().setAppName("StationDataIngestion").setMaster("local[*]").set("spark.storage.memoryFraction", "1")
    val sc = new SparkContext(sparkConf)

    val countriesFile = sc.textFile(countriesFilePath, parallelism)
    val mappedCountries = countriesFile.flatMap(t => Seq((t.substring(0,2), t.substring(3)))) //Code, Name
    val countryMap = sc.broadcast(mappedCountries.collectAsMap())

    val stationsFile = sc.textFile(stationsFilePath, parallelism)
    val mappedStations = stationsFile.map(t => new Station(
      t.substring(0,11), //ID
      t.substring(12,20).toDouble,//latitude
      t.substring(21,30).toDouble,//longitude
      t.substring(31,37),//elevation
      t.substring(38,40),//state
      t.substring(41,71),//name
      t.substring(72,75),//gsn
      t.substring(76,79),//hcn
      t.substring(80),//wmo id
      t.substring(0,2),//country code
      countryMap.value.get(t.substring(0,2)).get //country name
    )) // Map side join with countries by using the broadcast variable

//    val rddContent = mappedStations.collect()
//    rddContent.foreach(t => println(t.toString()))

    //Map the RDD to a structure as follows: Key Value ColumnFamily Column
    val keyRDD = mappedStations.flatMap(t => {
      Seq(
        (t.ID, t.country_name, "STATION_DETAILS", "COUNTRY"),
        (t.ID, t.country_cd, "STATION_DETAILS", "COUNTRY_CD"),
        (t.ID, t.elevation, "STATION_DETAILS", "ELEVATION"),
        (t.ID, t.HCN_CRN, "STATION_DETAILS", "HCN_CRN"),
        (t.ID, t.GSN, "STATION_DETAILS", "GSN"),
        (t.ID, t.ID, "STATION_DETAILS", "ID"),
        (t.ID, t.lattitude, "STATION_DETAILS", "LATTITUDE"),
        (t.ID, t.longitude, "STATION_DETAILS", "LONGITUDE"),
        (t.ID, t.state, "STATION_DETAILS", "STATE"),
        (t.ID, t.stn_name, "STATION_DETAILS", "STATION_NAME"),
        (t.ID, t.WMO_ID, "STATION_DETAILS", "WMO_ID")
      )
    }).sortBy(r => (r._1, r._4))


    // Create HBase Configuration and related objects
    val conf = HBaseConfiguration.create()
    val tableName = "STATIONS"
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

    putRdd.saveAsNewAPIHadoopFile("hdfs://quickstart.cloudera:8020/user/cloudera/avgSeasonalTemp/stations",
      classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], conf)
  }

}
