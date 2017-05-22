package com.sb.miniproject

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, TableInputFormat}
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Prateek Sheel on 5/22/17.
  */
object AvgSeasonalTempCalc {

  case class Observation (key :String, station_code :String, year :Int, month :Int, season :String, precipitation :Double, avg_temp :Double)
  case class Station (ID :String, lattitude :Double, longitude :Double, elevation :String, state :String,
                      stn_name :String, GSN :String, HCN_CRN: String, WMO_ID :String, country_cd :String, country_name :String)

  def main(args : Array[String]): Unit ={

    val sparkConf = new SparkConf().setAppName("AvgSeasonalTempCalc").setMaster("local[*]").set("spark.storage.memoryFraction", "1")
    val sc = new SparkContext(sparkConf)
    val sqlContext: SQLContext = new HiveContext(sc)
    import org.apache.spark.sql.functions._
    import sqlContext.implicits._

    val conf = HBaseConfiguration.create()
    val hbaseContext = new HBaseContext(sc, conf)
    val stationTableName = "STATIONS"
    val dataTableName = "SEASONAL_TEMP"
    conf.set(TableInputFormat.INPUT_TABLE, dataTableName)

    val dataRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    val mappedRDD = dataRDD.map(t => new Observation(
      null,
      Bytes.toString(CellUtil.cloneValue(t._2.getColumnLatestCell(Bytes.toBytes("ID_DETAILS"), Bytes.toBytes("STATION_CODE")))),
      Bytes.toString(CellUtil.cloneValue(t._2.getColumnLatestCell(Bytes.toBytes("ID_DETAILS"), Bytes.toBytes("YEAR")))).toInt,
      Bytes.toString(CellUtil.cloneValue(t._2.getColumnLatestCell(Bytes.toBytes("ID_DETAILS"), Bytes.toBytes("MONTH")))).toInt,
      Bytes.toString(CellUtil.cloneValue(t._2.getColumnLatestCell(Bytes.toBytes("WEATHER_DATA"), Bytes.toBytes("SEASON")))),
      Bytes.toString(CellUtil.cloneValue(t._2.getColumnLatestCell(Bytes.toBytes("WEATHER_DATA"), Bytes.toBytes("PRECIPITATION")))).toDouble,
      Bytes.toString(CellUtil.cloneValue(t._2.getColumnLatestCell(Bytes.toBytes("WEATHER_DATA"), Bytes.toBytes("AVG_TEMP")))).toDouble
    )).filter(t => t.year >= 1900) //Filter out records from before year 1900
    val dataDF = mappedRDD.toDF()
//    dataDF.printSchema()

    conf.set(TableInputFormat.INPUT_TABLE, stationTableName)
    val stationsRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    val mappedStations = stationsRDD.map(t => new Station(
      Bytes.toString(CellUtil.cloneValue(t._2.getColumnLatestCell(Bytes.toBytes("STATION_DETAILS"), Bytes.toBytes("ID")))),
      Bytes.toString(CellUtil.cloneValue(t._2.getColumnLatestCell(Bytes.toBytes("STATION_DETAILS"), Bytes.toBytes("LATTITUDE")))).toDouble,
      Bytes.toString(CellUtil.cloneValue(t._2.getColumnLatestCell(Bytes.toBytes("STATION_DETAILS"), Bytes.toBytes("LONGITUDE")))).toDouble,
      Bytes.toString(CellUtil.cloneValue(t._2.getColumnLatestCell(Bytes.toBytes("STATION_DETAILS"), Bytes.toBytes("ELEVATION")))),
      Bytes.toString(CellUtil.cloneValue(t._2.getColumnLatestCell(Bytes.toBytes("STATION_DETAILS"), Bytes.toBytes("STATE")))),
      Bytes.toString(CellUtil.cloneValue(t._2.getColumnLatestCell(Bytes.toBytes("STATION_DETAILS"), Bytes.toBytes("STATION_NAME")))),
      Bytes.toString(CellUtil.cloneValue(t._2.getColumnLatestCell(Bytes.toBytes("STATION_DETAILS"), Bytes.toBytes("GSN")))),
      Bytes.toString(CellUtil.cloneValue(t._2.getColumnLatestCell(Bytes.toBytes("STATION_DETAILS"), Bytes.toBytes("HCN_CRN")))),
      Bytes.toString(CellUtil.cloneValue(t._2.getColumnLatestCell(Bytes.toBytes("STATION_DETAILS"), Bytes.toBytes("WMO_ID")))),
      Bytes.toString(CellUtil.cloneValue(t._2.getColumnLatestCell(Bytes.toBytes("STATION_DETAILS"), Bytes.toBytes("COUNTRY_CD")))),
      Bytes.toString(CellUtil.cloneValue(t._2.getColumnLatestCell(Bytes.toBytes("STATION_DETAILS"), Bytes.toBytes("COUNTRY"))))
    ))
    val stationsDF = mappedStations.toDF()
//    stationsDF.printSchema()


    val joinDF = dataDF.join(stationsDF, dataDF("station_code") === stationsDF("ID"), "inner") // Join Data in One DF
//    joinDF.show()

    val summaryDF = joinDF.drop("key").drop("precipitation").drop("ID").drop("elevation").drop("state")
      .drop("stn_name").drop("GSN").drop("HCN_CRN").drop("WMO_ID").drop("country_cd").drop("country_name")

    val targetDF = summaryDF.withColumn("target_lattitude", round($"lattitude")).withColumn("target_longitude", round($"longitude"))
//    targetDF.show()

    val groupedDF = targetDF.groupBy("target_lattitude", "target_longitude", "year", "season").agg(
      avg("avg_temp"),
      count("avg_temp"),
      collect_set("station_code")
    )
    //groupedDF.show()

    val summaryRdd = groupedDF.rdd

    //Map the RDD to a structure as follows: Key Value ColumnFamily Column
    val keyRDD = summaryRdd.flatMap(t => {
      val key = t(0) + ":" + t(1) + ":" + t(2) + ":" + t(3)
      Seq(
        (key, t(4), "SUMMARY_DETAILS", "AVG_TEMP"),
        (key, t(5), "SUMMARY_DETAILS", "DATA_POINTS"),
        (key, t(0), "SUMMARY_DETAILS", "LATTITUDE"),
        (key, t(1), "SUMMARY_DETAILS", "LONGITUDE"),
        (key, t(3), "SUMMARY_DETAILS", "SEASON"),
        (key, t(6), "SUMMARY_DETAILS", "STATIONS_LIST"),
        (key, t(2), "SUMMARY_DETAILS", "YEAR")
      )
    }).sortBy(r => r._1)

//    val rddContent = keyRDD.collect()
//    rddContent.foreach(t => println(t))

    val connection = ConnectionFactory.createConnection(conf)
    val tableName = "SUMMARY"
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

    putRdd.saveAsNewAPIHadoopFile("hdfs://quickstart.cloudera:8020/user/cloudera/avgSeasonalTemp/summary",
      classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], conf)
    sc.stop()
  }
}
