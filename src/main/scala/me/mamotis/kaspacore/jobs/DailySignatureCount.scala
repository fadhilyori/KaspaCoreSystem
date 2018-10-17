package me.mamotis.kaspacore.jobs

import me.mamotis.kaspacore.util.{ColsArtifact, Commons, PropertiesLoader, PushArtifact}
import java.time.LocalDate

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

    val pushSignatureCompanyDf = countedSignatureCompanyDf
      .withColumn("year", lit(LocalDate.now.getYear))
      .withColumn("month", lit(LocalDate.now.getMonthValue))
      .withColumn("day", lit(LocalDate.now.getDayOfMonth))

    pushSignatureCompanyDf.show()

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

    val pushSignatureDeviceIdDf = countedSignatureDeviceIdDf
      .withColumn("year", lit(LocalDate.now.getYear))
      .withColumn("month", lit(LocalDate.now.getMonthValue))
      .withColumn("day", lit(LocalDate.now.getDayOfMonth))

    pushSignatureDeviceIdDf.show()


    // ======================================Device ID===============================

    // ======================================Query===============================
  }
}
