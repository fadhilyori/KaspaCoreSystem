package me.mamotis.kaspacore.jobs

import me.mamotis.kaspacore.util.{ColsArtifact, Commons, PropertiesLoader, PushArtifact}
import java.time.LocalDate

import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.functions.{desc, lit}

object DailySignatureCount extends Utils {

  def main(args: Array[String]): Unit = {
    val sparkSession = getSparkSession(args)
    val sparkContext = getSparkContext(sparkSession)

    val connector = getCassandraSession(sparkContext)

    import sparkSession.implicits._
    sparkContext.setLogLevel("ERROR")

    // Raw Event Dataframe Parsing
    val rawDf = sparkSession.read.json(PropertiesLoader.hadoopEventFilePath)

    // ======================================Company===============================
    val filteredEventsCompanyDf = rawDf
      .select($"company", $"alert_msg", $"year", $"month", $"day")
      .filter($"year" === LocalDate.now.getYear)
      .filter($"month" === LocalDate.now.getMonthValue)
      .filter($"day" === LocalDate.now.getDayOfMonth)

    val countedSignatureCompanyDf = filteredEventsCompanyDf
      .groupBy($"company", $"alert_msg")
      .count().orderBy(desc("count"))

    val pushSignatureCompanyDf = countedSignatureCompanyDf.map{
      r =>
        val company = r.getAs[String](0)
        val alert_msg = r.getAs[String](1)
        val value = r.getAs[Long](2)
        val year = LocalDate.now.getYear()
        val month = LocalDate.now.getMonthValue()
        val day = LocalDate.now.getDayOfMonth()

        new Commons.SignatureHitCompanyObjDay(
          company, alert_msg, year, month, day, value
        )
    }.toDF(ColsArtifact.colsSignatureHitCompanyObjDay: _*)

    val pushSignatureCompanyDs = pushSignatureCompanyDf
      .select($"company", $"alert_msg", $"year", $"month", $"day", $"value")
      .as[Commons.SignatureHitCompanyObjDay]

    val writerSignatureHitCompany = new ForeachWriter[Commons.SignatureHitCompanyObjDay] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.SignatureHitCompanyObjDay): Unit = {
        PushArtifact.pushSignatureHitCompanyDay(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }
    // ======================================Company===============================


    // ======================================Device ID===============================
    val filteredEventsDeviceIdDf = rawDf
      .select($"device_id", $"alert_msg", $"year", $"month", $"day")
      .filter($"year" === LocalDate.now.getYear)
      .filter($"month" === LocalDate.now.getMonthValue)
      .filter($"day" === LocalDate.now.getDayOfMonth)

    val countedSignatureDeviceIdDf = filteredEventsDeviceIdDf
      .groupBy($"device_id", $"alert_msg")
      .count().orderBy(desc("count"))

    val pushSignatureDeviceIdDf = countedSignatureDeviceIdDf.map{
      r =>
        val device_id = r.getAs[String](0)
        val alert_msg = r.getAs[String](1)
        val value = r.getAs[Long](2)
        val year = LocalDate.now.getYear()
        val month = LocalDate.now.getMonthValue()
        val day = LocalDate.now.getDayOfMonth()

        new Commons.SignatureHitDeviceIdObjDay(
          device_id, alert_msg, year, month, day, value
        )
    }.toDF(ColsArtifact.colsSignatureHitDeviceIdObjDay: _*)

    val pushSignatureDeviceIdDs = pushSignatureDeviceIdDf
      .select($"device_id", $"alert_msg", $"year", $"month", $"day", $"value")
      .as[Commons.SignatureHitDeviceIdObjDay]

    val writerSignatureHitDeviceId = new ForeachWriter[Commons.SignatureHitDeviceIdObjDay] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.SignatureHitDeviceIdObjDay): Unit = {
        PushArtifact.pushSignatureHitDeviceIdDay(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }
    // ======================================Device ID===============================

    // ======================================Query===============================
    val signatureHitCompanyDailyQuery = pushSignatureCompanyDs
      .writeStream
      .outputMode("complete")
      .queryName("Signature Hit Company Daily")
      .foreach(writerSignatureHitCompany)
      .start()

    val signatureHitDeviceIdDailyQuery = pushSignatureDeviceIdDs
      .writeStream.outputMode("complete")
      .queryName("Signature Hit Device ID Daily")
      .foreach(writerSignatureHitDeviceId)
      .start()

    signatureHitCompanyDailyQuery.awaitTermination()
    signatureHitDeviceIdDailyQuery.awaitTermination()
  }
}
