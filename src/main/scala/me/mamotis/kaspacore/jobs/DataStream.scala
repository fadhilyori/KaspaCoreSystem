package me.mamotis.kaspacore.jobs

import java.sql.Timestamp

import com.databricks.spark.avro.ConfluentSparkAvroUtils
import com.datastax.spark.connector.cql.CassandraConnector
import com.mongodb.client.MongoCollection
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.WriteConfig
import me.mamotis.kaspacore.util._
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Dataset, ForeachWriter, SparkSession}
import org.bson.Document
import org.joda.time.DateTime

import scala.collection.JavaConverters._
import scala.collection.mutable

object DataStream extends Utils {

  def main(args: Array[String]): Unit = {
    //=================================AVRO DESERIALIZER====================================
    val utils: ConfluentSparkAvroUtils = new ConfluentSparkAvroUtils(PropertiesLoader.schemaRegistryUrl)
    //    val keyDes = utils.deserializerForSubject(PropertiesLoader.kafkaInputTopic + "-key")
    val valDes = utils.deserializerForSubject(PropertiesLoader.kafkaInputTopic + "-value")

    //=================================SPARK CONFIGURATION==================================

    val sparkSession: SparkSession = getSparkSession(args)
    val sparkContext: SparkContext = getSparkContext(sparkSession)

    // Maxmind GeoIP Configuration
    sparkContext.addFile(PropertiesLoader.GeoIpPath)


    // Cassandra Connector
    val connector: CassandraConnector = getCassandraSession(sparkContext)

    // set implicit and log level
    import sparkSession.implicits._
    sparkContext.setLogLevel("ERROR")

    //==================================KAFKA DEFINITION=====================================

    val kafkaStreamDF: DataFrame = sparkSession
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", PropertiesLoader.kafkaBrokerUrl)
      .option("subscribe", PropertiesLoader.kafkaInputTopic)
      .option("startingOffsets", PropertiesLoader.kafkaStartingOffset)
      .load()

    val decoded: DataFrame = kafkaStreamDF.select(
      valDes(col("value")).alias("value")
    )

    val parsedRawDf: DataFrame = decoded.select("value.*")

    sparkContext.parallelize(parsedRawDf)

    //======================================DATAFRAME PARSING==================================

    //region +++++++++++Raw Data++++++++++++++

    val eventDf: DataFrame = parsedRawDf.select(
      $"timestamp", $"device_id", $"protocol", $"ip_type", $"src_mac", $"dest_mac", $"src_ip",
      $"dest_ip", $"src_port", $"dst_port", $"alert_msg", $"classification", $"priority", $"sig_id",
      $"sig_gen", $"sig_rev", $"company"
    ).map { r =>
      val ts: String = r.getAs[String](0)
      val device_id: String = r.getAs[String](1)
      val protocol: String = r.getAs[String](2)
      val ip_type: String = r.getAs[String](3)
      val src_mac: String = r.getAs[String](4)
      val dest_mac: String = r.getAs[String](5)
      val src_ip: String = r.getAs[String](6)
      val dest_ip: String = r.getAs[String](7)
      val src_port: Int = r.getAs[Long](8).toInt
      val dest_port: Int = r.getAs[Long](9).toInt
      val alert_msg: String = r.getAs[String](10)
      val classification: Int = r.getAs[Long](11).toInt
      val priority: Int = r.getAs[Long](12).toInt
      val sig_id: Int = r.getAs[Long](13).toInt
      val sig_gen: Int = r.getAs[Long](14).toInt
      val sig_rev: Int = r.getAs[Long](15).toInt
      val company: String = r.getAs[String](16)
      val src_country: String = Tools.IpLookupCountry(src_ip)
      val src_region: String = Tools.IpLookupRegion(src_ip)
      val dest_country: String = Tools.IpLookupCountry(dest_ip)
      val dest_region: String = Tools.IpLookupRegion(dest_ip)

      val date = new DateTime((ts.toDouble * 1000).toLong)

      Commons.EventObj(
        ts, company, device_id, date.getYear, date.getMonthOfYear, date.getDayOfMonth, date.getHourOfDay,
        date.getMinuteOfHour, date.getSecondOfMinute, protocol, ip_type, src_mac, dest_mac, src_ip, dest_ip,
        src_port, dest_port, alert_msg, classification, priority, sig_id, sig_gen, sig_rev, src_country,
        src_region, dest_country, dest_region
      )
    }.toDF(ColsArtifact.colsEventObj: _*)

    val eventDs: Dataset[Commons.EventObj] = eventDf.select($"ts", $"company", $"device_id", $"year", $"month",
      $"day", $"hour", $"minute", $"second", $"protocol", $"ip_type",
      $"src_mac", $"dest_mac", $"src_ip", $"dest_ip", $"src_port",
      $"dest_port", $"alert_msg", $"classification", $"priority",
      $"sig_id", $"sig_gen", $"sig_rev", $"src_country", $"src_region",
      $"dest_country", $"dest_region").as[Commons.EventObj]

    //endregion

    //region +++++++++++++Push Event Hit Company per Second++++++++++++++++++++++
    val eventHitCompanySelect: DataFrame = parsedRawDf.select(to_utc_timestamp(from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"company").withColumn("value", lit(1))

    //+++++Second

    val eventHitCompanyDFPerSecond: DataFrame = eventHitCompanySelect
      .groupBy($"company", window($"timestamp", "1 seconds").alias("windows"))
      .sum("value")
      .select($"company", $"windows.start", $"sum(value)")
      .map {
        r =>
          val date: DateTime = new DateTime(r.getAs[Timestamp](1).getTime)

          Commons.EventHitCompanyObjSec(
            // Company, Year, Month, Day, Hour, Minute, Second, Value
            r.getAs[String](0), date.getYear, date.getMonthOfYear, date.getDayOfMonth, date.getHourOfDay, date.getMinuteOfHour, date.getSecondOfMinute, r.getAs[Long](2)
          )
      }.toDF(ColsArtifact.colsEventHitCompanyObjSec: _*)

    val eventHitCompanySecDs: Dataset[Commons.EventHitCompanyObjSec] = eventHitCompanyDFPerSecond.select($"company", $"year",
      $"month", $"day", $"hour", $"minute", $"second", $"value").as[Commons.EventHitCompanyObjSec]

    //    val eventHitCompanySecKafkaDs = eventHitCompanyDFPerSecond
    //      .withColumn("value", concat_ws(";", $"company", $"year", $"month", $"day", $"hour", $"minute",
    //        $"second", $"value")).selectExpr("CAST(value AS STRING)")

    //+++++Minute
    val eventHitCompanyDFPerMinute: DataFrame = eventHitCompanySelect
      .groupBy($"company", window($"timestamp", "1 minutes").alias("windows"))
      .sum("value")
      .select($"company", $"windows.start", $"sum(value)")
      .map {
        r =>
          val date: DateTime = new DateTime(r.getAs[Timestamp](1).getTime)

          Commons.EventHitCompanyObjMin(
            // Company, Year, Month, Day, Hour, Minute, Value
            r.getAs[String](0), date.getYear, date.getMonthOfYear, date.getDayOfMonth, date.getHourOfDay, date.getMinuteOfHour, r.getAs[Long](2)
          )
      }
      .toDF(ColsArtifact.colsEventHitCompanyObjMin: _*)

    val eventHitCompanyMinDs: Dataset[Commons.EventHitCompanyObjMin] = eventHitCompanyDFPerMinute
      .select($"company", $"year", $"month", $"day", $"hour", $"minute", $"value").as[Commons.EventHitCompanyObjMin]

    //+++++Hour
    val eventHitCompanyDFPerHour: DataFrame = eventHitCompanySelect
      .groupBy($"company", window($"timestamp", "1 hours").alias("windows"))
      .sum("value")
      .select($"company", $"windows.start", $"sum(value)")
      .map {
        r =>
          val date: DateTime = new DateTime(r.getAs[Timestamp](1).getTime)

          Commons.EventHitCompanyObjHour(
            // Company, Year, Month, Day, Hour, Value
            r.getAs[String](0), date.getYear, date.getMonthOfYear, date.getDayOfMonth, date.getHourOfDay, r.getAs[Long](2)
          )
      }.toDF(ColsArtifact.colsEventHitCompanyObjHour: _*)

    val eventHitCompanyHourDs: Dataset[Commons.EventHitCompanyObjHour] = eventHitCompanyDFPerHour
      .select($"company", $"year", $"month", $"day", $"hour", $"value").as[Commons.EventHitCompanyObjHour]

    //endregion

    //region +++++++++++++Push Event Hit DeviceId per Second++++++++++++++++++++++
    val eventHitDeviceIdSelect: DataFrame = parsedRawDf.select(to_utc_timestamp(from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"device_id").withColumn("value", lit(1))

    //+++++Second
    val eventHitDeviceIdDFPerSecond: DataFrame = eventHitDeviceIdSelect
      .groupBy($"device_id", window($"timestamp", "1 seconds").alias("windows"))
      .sum("value")
      .select($"device_id", $"windows.start", $"sum(value)")
      .map {
        r =>
          val date: DateTime = new DateTime(r.getAs[Timestamp](1).getTime)

          Commons.EventHitDeviceIdObjSec(
            // Device ID, Year, Month, Day, Hour, Minute, Second, Value
            r.getAs[String](0), date.getYear, date.getMonthOfYear, date.getDayOfMonth, date.getHourOfDay, date.getMinuteOfHour, date.getSecondOfMinute, r.getAs[Long](2)
          )
      }.toDF(ColsArtifact.colsEventHitDeviceIdObjSec: _*)

    val eventHitDeviceIdSecDs: Dataset[Commons.EventHitDeviceIdObjSec] = eventHitDeviceIdDFPerSecond
      .select($"device_id", $"year", $"month", $"day", $"hour", $"minute", $"second", $"value").as[Commons.EventHitDeviceIdObjSec]

    //+++++Minute

    val eventHitDeviceIdDFPerMinute: DataFrame = eventHitDeviceIdSelect
      .groupBy($"device_id", window($"timestamp", "1 minutes").alias("windows"))
      .sum("value")
      .select($"device_id", $"windows.start", $"sum(value)")
      .map {
        r =>
          val date: DateTime = new DateTime(r.getAs[Timestamp](1).getTime)

          Commons.EventHitDeviceIdObjMin(
            // Device ID, Year, Month, Day, Hour, Minute, Value
            r.getAs[String](0), date.getYear, date.getMonthOfYear, date.getDayOfMonth, date.getHourOfDay, date.getMinuteOfHour, r.getAs[Long](2)
          )
      }.toDF(ColsArtifact.colsEventHitDeviceIdObjMin: _*)

    val eventHitDeviceIdMinDs: Dataset[Commons.EventHitDeviceIdObjMin] = eventHitDeviceIdDFPerMinute
      .select($"device_id", $"year", $"month", $"day", $"hour", $"minute", $"value").as[Commons.EventHitDeviceIdObjMin]

    //+++++Hour
    val eventHitDeviceIdDFPerHour: DataFrame = eventHitDeviceIdSelect
      .groupBy($"device_id", window($"timestamp", "1 hours").alias("windows"))
      .sum("value")
      .select($"device_id", $"windows.start", $"sum(value)")
      .map {
        r =>
          val date: DateTime = new DateTime(r.getAs[Timestamp](1).getTime)

          Commons.EventHitDeviceIdObjHour(
            // Device ID, Year, Month, Day, Hour, Value
            r.getAs[String](0), date.getYear, date.getMonthOfYear, date.getDayOfMonth, date.getHourOfDay, r.getAs[Long](2)
          )
      }.toDF(ColsArtifact.colsEventHitDeviceIdObjHour: _*)

    val eventHitDeviceIdHourDs: Dataset[Commons.EventHitDeviceIdObjHour] = eventHitDeviceIdDFPerHour
      .select($"device_id", $"year", $"month", $"day", $"hour", $"value").as[Commons.EventHitDeviceIdObjHour]

    //region sliding 1 second
    val signatureDF1Second: DataFrame = parsedRawDf
      .select(to_utc_timestamp(from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType),
        $"alert_msg", $"src_ip", $"dest_ip", $"company", $"priority")
      .withColumn("value", lit(1))
      .groupBy($"alert_msg", $"src_ip", $"dest_ip", $"company", $"priority",
        window($"timestamp", "1 second").alias("windows"))
      .sum("value")
      .select($"company", $"alert_msg", $"windows.start", $"sum(value)", $"src_ip", $"dest_ip", $"priority")
      .map {
        r =>
          val date: DateTime = new DateTime(r.getAs[Timestamp](2).getTime)

          Commons.SteviaObjSec(
            r.getAs[String](0), //company
            r.getAs[String](1), //alert_msg
            r.getAs[String](4), //src_ip
            Tools.IpLookupCountry(r.getAs[String](4)), //src_country
            r.getAs[String](5), //dest_ip
            Tools.IpLookupCountry(r.getAs[String](5)), //dest_country
            date.getYear, //year
            date.getMonthOfYear, //month
            date.getDayOfMonth, //day
            date.getHourOfDay, //hour
            date.getMinuteOfHour, //minute
            date.getSecondOfMinute, //second
            r.getAs[Long](3), //value
            r.getAs[Long](6) //priority
          )
      }.toDF(ColsArtifact.colsSteviaObjSec: _*)

    val signature1sDs = signatureDF1Second.select($"company", $"alert_msg", $"src_ip", $"src_country", $"dest_ip", $"dest_country", $"year",
      $"month", $"day", $"hour", $"minute", $"second", $"value", $"priority").as[Commons.SteviaObjSec]

    //endregion

    //    //region sliding 2 seconds
    //
    //    val signature2sDf_1 = parsedRawDf
    //      .select(
    //        to_utc_timestamp(from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType),
    //        $"alert_msg",
    //        $"company")
    //      .withColumn("value", lit(1))
    //      .groupBy(
    //        $"alert_msg",
    //        $"company",
    //        window($"timestamp", "2 seconds").alias("windows"))
    //      .sum("value")
    //
    //    val signature2sDf_2 = signature2sDf_1.select($"company", $"alert_msg", $"windows.start", $"sum(value)").map {
    //      r =>
    //        val company = r.getAs[String](0)
    //        val alert_msg = r.getAs[String](1)
    //
    //        val epoch = r.getAs[Timestamp](2).getTime
    //
    //        val date = new DateTime(epoch)
    //        val year = date.getYear()
    //        val month = date.getMonthOfYear()
    //        val day = date.getDayOfMonth()
    //        val hour = date.getHourOfDay()
    //        val minute = date.getMinuteOfHour()
    //        val second = date.getSecondOfMinute()
    //
    //        val value = r.getAs[Long](3)
    //
    //        new Commons.SignatureHitCompanyObjSec(
    //          company, alert_msg, year, month, day, hour, minute, second, value
    //        )
    //    }.toDF(ColsArtifact.colsSignatureHitCompanyObjSec: _*)
    //
    //    val signature2sDs = signature2sDf_2.select($"company", $"alert_msg", $"year",
    //      $"month", $"day", $"hour", $"minute", $"second", $"value").as[Commons.SignatureHitCompanyObjSec]
    //
    //    //endregion 2
    //
    //    //region sliding 3 seconds
    //
    //    val signature3sDf_1 = parsedRawDf
    //      .select(
    //        to_utc_timestamp(from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType),
    //        $"alert_msg",
    //        $"company")
    //      .withColumn("value", lit(1))
    //      .groupBy(
    //        $"alert_msg",
    //        $"company",
    //        window($"timestamp", "3 seconds").alias("windows"))
    //      .sum("value")
    //
    //    val signature3sDf_2 = signature3sDf_1.select($"company", $"alert_msg", $"windows.start", $"sum(value)").map {
    //      r =>
    //        val company = r.getAs[String](0)
    //        val alert_msg = r.getAs[String](1)
    //
    //        val epoch = r.getAs[Timestamp](2).getTime
    //
    //        val date = new DateTime(epoch)
    //        val year = date.getYear()
    //        val month = date.getMonthOfYear()
    //        val day = date.getDayOfMonth()
    //        val hour = date.getHourOfDay()
    //        val minute = date.getMinuteOfHour()
    //        val second = date.getSecondOfMinute()
    //
    //        val value = r.getAs[Long](3)
    //
    //        new Commons.SignatureHitCompanyObjSec(
    //          company, alert_msg, year, month, day, hour, minute, second, value
    //        )
    //    }.toDF(ColsArtifact.colsSignatureHitCompanyObjSec: _*)
    //
    //    val signature3sDs = signature3sDf_2.select($"company", $"alert_msg", $"year",
    //      $"month", $"day", $"hour", $"minute", $"second", $"value").as[Commons.SignatureHitCompanyObjSec]
    //
    //    //endregion
    //
    //    //region sliding 5 seconds
    //
    //    val signature5sDf_1 = parsedRawDf
    //      .select(
    //        to_utc_timestamp(from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType),
    //        $"alert_msg",
    //        $"company")
    //      .withColumn("value", lit(1))
    //      .groupBy(
    //        $"alert_msg",
    //        $"company",
    //        window($"timestamp", "5 seconds").alias("windows"))
    //      .sum("value")
    //
    //    val signature5sDf_2 = signature5sDf_1.select($"company", $"alert_msg", $"windows.start", $"sum(value)").map {
    //      r =>
    //        val company = r.getAs[String](0)
    //        val alert_msg = r.getAs[String](1)
    //
    //        val epoch = r.getAs[Timestamp](2).getTime
    //
    //        val date = new DateTime(epoch)
    //        val year = date.getYear()
    //        val month = date.getMonthOfYear()
    //        val day = date.getDayOfMonth()
    //        val hour = date.getHourOfDay()
    //        val minute = date.getMinuteOfHour()
    //        val second = date.getSecondOfMinute()
    //
    //        val value = r.getAs[Long](3)
    //
    //        new Commons.SignatureHitCompanyObjSec(
    //          company, alert_msg, year, month, day, hour, minute, second, value
    //        )
    //    }.toDF(ColsArtifact.colsSignatureHitCompanyObjSec: _*)
    //
    //    val signature5sDs = signature5sDf_2.select($"company", $"alert_msg", $"year",
    //      $"month", $"day", $"hour", $"minute", $"second", $"value").as[Commons.SignatureHitCompanyObjSec]
    //
    //    //endregion

    //======================================================MONGO WRITER======================================

    //    val writerMongo: ForeachWriter[Commons.EventObj] = new ForeachWriter[Commons.EventObj] {
    //
    //      val writeConfig: WriteConfig = WriteConfig(Map("uri" -> "mongodb://admin:jarkoM@127.0.0.1:27017/stevia.event?replicaSet=rs0&authSource=admin"))
    //      var mongoConnector: MongoConnector = _
    //      var events: mutable.ArrayBuffer[Commons.EventObj] = _
    //
    //      override def open(partitionId: Long, version: Long): Boolean = {
    //        mongoConnector = MongoConnector(writeConfig.asOptions)
    //        events = new mutable.ArrayBuffer[Commons.EventObj]()
    //        true
    //      }
    //
    //      override def process(value: Commons.EventObj): Unit = {
    //        events.append(value)
    //      }
    //
    //      override def close(errorOrNull: Throwable): Unit = {
    //        if (events.nonEmpty) {
    //          mongoConnector.withCollectionDo(writeConfig, { collection: MongoCollection[Document] =>
    //            collection.insertMany(events.map(sc => {
    //              val doc = new Document()
    //              doc.put("ts", sc.ts)
    //              doc.put("company", sc.company)
    //              doc.put("device_id", sc.device_id)
    //              doc.put("year", sc.year)
    //              doc.put("month", sc.month)
    //              doc.put("day", sc.day)
    //              doc.put("hour", sc.hour)
    //              doc.put("minute", sc.minute)
    //              doc.put("second", sc.second)
    //              doc.put("protocol", sc.protocol)
    //              doc.put("ip_type", sc.ip_type)
    //              doc.put("src_mac", sc.src_port)
    //              doc.put("dest_mac", sc.dest_mac)
    //              doc.put("src_ip", sc.src_ip)
    //              doc.put("dest_ip", sc.dest_ip)
    //              doc.put("src_port", sc.src_port)
    //              doc.put("dest_port", sc.dest_port)
    //              doc.put("alert_message", sc.alert_msg)
    //              doc.put("classification", sc.classification)
    //              doc.put("priority", sc.priority)
    //              doc.put("sig_id", sc.sig_id)
    //              doc.put("sig_gen", sc.sig_gen)
    //              doc.put("sig_rev", sc.sig_rev)
    //              doc.put("src_country", sc.src_country)
    //              doc.put("src_region", sc.src_region)
    //              doc.put("dest_country", sc.dest_country)
    //              doc.put("dest_region", sc.dest_region)
    //              doc
    //            }).asJava)
    //          })
    //        }
    //      }
    //    }

    def writerMongoSig(urlConnectMongo: String): ForeachWriter[Commons.SteviaObjSec] = {
      new ForeachWriter[Commons.SteviaObjSec] {
        val writeConfig: WriteConfig = WriteConfig(Map("uri" -> urlConnectMongo))
        var mongoConnector: MongoConnector = _
        var events: mutable.ArrayBuffer[Commons.SteviaObjSec] = _

        override def open(partitionId: Long, version: Long): Boolean = {
          mongoConnector = MongoConnector(writeConfig.asOptions)
          events = new mutable.ArrayBuffer[Commons.SteviaObjSec]()
          true
        }

        override def process(value: Commons.SteviaObjSec): Unit = {
          events.append(value)
        }

        override def close(errorOrNull: Throwable): Unit = {
          if (events.nonEmpty) {
            mongoConnector.withCollectionDo(writeConfig, { collection: MongoCollection[Document] =>
              collection.insertMany(events.map(
                sc => {
                  val doc = new Document()
                  doc.put("company", sc.company)
                  doc.put("year", sc.year)
                  doc.put("month", sc.month)
                  doc.put("day", sc.day)
                  doc.put("hour", sc.hour)
                  doc.put("minute", sc.minute)
                  doc.put("second", sc.second)
                  doc.put("alert_message", sc.alert_msg)
                  doc.put("src_ip", sc.src_ip)
                  doc.put("src_country", sc.src_country)
                  doc.put("dest_ip", sc.dest_ip)
                  doc.put("dest_country", sc.dest_country)
                  doc.put("value", sc.value)
                  doc.put("priority", sc.priority)
                  doc
                }).asJava
              )
            })
          }
        }
      }
    }

    //======================================================CASSANDRA WRITER======================================
    val writerEvent = new ForeachWriter[Commons.EventObj] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.EventObj): Unit = {
        PushArtifact.pushRawData(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerEventHitCompanySec = new ForeachWriter[Commons.EventHitCompanyObjSec] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.EventHitCompanyObjSec): Unit = {
        PushArtifact.pushEventHitCompanySec(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerEventHitCompanyMin = new ForeachWriter[Commons.EventHitCompanyObjMin] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.EventHitCompanyObjMin): Unit = {
        PushArtifact.pushEventHitCompanyMin(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerEventHitCompanyHour = new ForeachWriter[Commons.EventHitCompanyObjHour] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.EventHitCompanyObjHour): Unit = {
        PushArtifact.pushEventHitCompanyHour(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerEventHitDeviceIdSec = new ForeachWriter[Commons.EventHitDeviceIdObjSec] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.EventHitDeviceIdObjSec): Unit = {
        PushArtifact.pushEventHitDeviceIdSec(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerEventHitDeviceIdMin = new ForeachWriter[Commons.EventHitDeviceIdObjMin] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.EventHitDeviceIdObjMin): Unit = {
        PushArtifact.pushEventHitDeviceIdMin(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerEventHitDeviceIdHour = new ForeachWriter[Commons.EventHitDeviceIdObjHour] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.EventHitDeviceIdObjHour): Unit = {
        PushArtifact.pushEventHitDeviceIdHour(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    //====================================================Job Start=================================

    // Mongo Realtime (Stevia)
    signature1sDs
      .writeStream
      .outputMode("update")
      .queryName("Event Push Mongo 1s Window")
      .foreach(writerMongoSig(PropertiesLoader.mongodbUri("event1s")))
      .start()

    // Push Raw Data to Cassandra
    eventDs
      .writeStream
      .outputMode("append")
      .queryName("Event Push Cassandra")
      .foreach(writerEvent)
      .start()

    // Push Raw Data to Hadoop
    eventDs
      .writeStream
      .format("json")
      .option("path", PropertiesLoader.hadoopEventFilePath)
      .option("checkpointLocation", PropertiesLoader.checkpointLocation)
      .start()

    // Push Event Hit Company Per Second
    eventHitCompanySecDs
      .writeStream
      .outputMode("update")
      .queryName("EventHitCompanyPerSec")
      .foreach(writerEventHitCompanySec)
      .start()

    // Push Event Hit Company Per Minute
    eventHitCompanyMinDs
      .writeStream
      .outputMode("update")
      .queryName("EventHitCompanyPerMin")
      .foreach(writerEventHitCompanyMin)
      .start()

    // Push Event Hit Company Per Hour
    eventHitCompanyHourDs
      .writeStream
      .outputMode("update")
      .queryName("EventHitCompanyPerHour")
      .foreach(writerEventHitCompanyHour)
      .start()

    // Push Event Hit Device ID Per Second
    eventHitDeviceIdSecDs
      .writeStream
      .outputMode("update")
      .queryName("EventHitDeviceIdPerSec")
      .foreach(writerEventHitDeviceIdSec)
      .start()

    // Push Event Hit Device ID Per Minute
    eventHitDeviceIdMinDs
      .writeStream
      .outputMode("update")
      .queryName("EventHitDeviceIdPerMin")
      .foreach(writerEventHitDeviceIdMin)
      .start()

    // Push Event Hit Device ID Per Hour
    eventHitDeviceIdHourDs
      .writeStream
      .outputMode("update")
      .queryName("EventHitDeviceIdPerHour")
      .foreach(writerEventHitDeviceIdHour)
      .start()

    sparkSession.streams.awaitAnyTermination()
  }
}
