package me.mamotis.kaspacore.jobs

import com.databricks.spark.avro.ConfluentSparkAvroUtils
import me.mamotis.kaspacore.util._
import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.functions._
import org.joda.time.DateTime

object RawDataStream extends Utils {

  def main(args: Array[String]): Unit = {
    //=================================AVRO DESERIALIZER====================================
    val utils = new ConfluentSparkAvroUtils(PropertiesLoader.schemaRegistryUrl)
    val keyDes = utils.deserializerForSubject(PropertiesLoader.kafkaInputTopic + "-key")
    val valDes = utils.deserializerForSubject(PropertiesLoader.kafkaInputTopic + "-value")

    //=================================SPARK CONFIGURATION==================================

    val sparkSession = getSparkSession(args)
    val sparkContext = getSparkContext(sparkSession)

    // Maxmind GeoIP Configuration
    sparkContext.addFile(PropertiesLoader.GeoIpPath)


    // Cassandra Connector
    val connector = getCassandraSession(sparkContext)

    // set implicit and log level
    import sparkSession.implicits._
    sparkContext.setLogLevel("ERROR")

    //==================================KAFKA DEFINITION=====================================

    val kafkaStreamDF = sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", PropertiesLoader.kafkaBrokerUrl)
      .option("subscribe", PropertiesLoader.kafkaInputTopic)
      .option("startingOffsets", PropertiesLoader.kafkaStartingOffset)
      .load()

    val decoded = kafkaStreamDF.select(
      valDes(col("value")).alias("value")
    )

    val parsedRawDf = decoded.select("value.*")

    //======================================DATAFRAME PARSING==================================

    //+++++++++++Raw Data++++++++++++++
    val eventDf = parsedRawDf.select(
      $"timestamp", $"device_id", $"protocol", $"ip_type", $"src_mac", $"dest_mac", $"src_ip",
      $"dest_ip", $"src_port", $"dst_port", $"alert_msg", $"classification", $"priority", $"sig_id",
      $"sig_gen", $"sig_rev", $"company"
    ).map { r =>
      val device_id = r.getAs[String](1)
      val protocol = r.getAs[String](2)
      val ip_type = r.getAs[String](3)
      val src_mac = r.getAs[String](4)
      val dest_mac = r.getAs[String](5)
      val src_ip = r.getAs[String](6)
      val dest_ip = r.getAs[String](7)
      val src_port = r.getAs[Long](8).toInt
      val dest_port = r.getAs[Long](9).toInt
      val alert_msg = r.getAs[String](10)
      val classification = r.getAs[Long](11).toInt
      val priority = r.getAs[Long](12).toInt
      val sig_id = r.getAs[Long](13).toInt
      val sig_gen = r.getAs[Long](14).toInt
      val sig_rev = r.getAs[Long](15).toInt
      val company = r.getAs[String](16)
//      val src_country = Tools.IpLookupCountry(src_ip)
      val src_country = "Dummy Country"
//      val src_region = Tools.IpLookupRegion(src_ip)
      val src_region = "Dummy City"
//      val dest_country = Tools.IpLookupCountry(dest_ip)
      val dest_country = "Dummy Country"
//      val dest_region = Tools.IpLookupRegion(dest_ip)
      val dest_region = "Dummy City"

      val date = new DateTime((r.getAs[String](0).toDouble * 1000).toLong)
      val year = date.getYear()
      val month = date.getMonthOfYear()
      val day = date.getDayOfMonth()
      val hour = date.getHourOfDay()
      val minute = date.getMinuteOfHour()
      val second = date.getSecondOfMinute()

      new Commons.EventObj(
        company, device_id, year, month, day, hour, minute, second,
        protocol, ip_type, src_mac, dest_mac, src_ip, dest_ip,
        src_port, dest_port, alert_msg, classification, priority,
        sig_id, sig_gen, sig_rev, src_country, src_region, dest_country, dest_region
      )
    }.toDF(ColsArtifact.colsEventObj: _*)

    val eventDs = eventDf.select($"company", $"device_id", $"year", $"month",
      $"day", $"hour", $"minute", $"second", $"protocol", $"ip_type",
      $"src_mac", $"dest_mac", $"src_ip", $"dest_ip", $"src_port",
      $"dest_port", $"alert_msg", $"classification", $"priority",
      $"sig_id", $"sig_gen", $"sig_rev", $"src_country", $"src_region",
      $"dest_country", $"dest_region").as[Commons.EventObj]

    //======================================================CASSANDRA WRITER======================================
    val writerEvent = new ForeachWriter[Commons.EventObj] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.EventObj): Unit = {
        PushArtifact.pushRawData(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val version = com.fasterxml.jackson.core.json.PackageVersion.VERSION
    println("JACKSON VERSION : " + version)

    //====================================================WRITE QUERY=================================
//    val eventConsoleQuery = eventDs
//      .writeStream
//      .outputMode("append")
//      .format("console")
//      .start().awaitTermination()

//    val eventPushQuery = eventDs
//      .writeStream
//      .outputMode("append")
//      .queryName("Event Push Cassandra")
//      .foreach(writerEvent)
//      .start()
//
//    val eventPushHDFS = eventDs
//      .writeStream
//      .format("json")
//      .option("path", PropertiesLoader.hadoopEventFilePath)
//      .option("checkpointLocation", PropertiesLoader.checkpointLocation)
//      .start()
//
//    eventPushQuery.awaitTermination()
//    eventPushHDFS.awaitTermination()
  }
}
