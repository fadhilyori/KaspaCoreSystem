package me.mamotis.kaspacore.jobs

import me.mamotis.kaspacore.util.{ColsArtifact, Commons, PropertiesLoader, PushArtifact}
import java.time.LocalDate

import org.apache.spark.sql.SaveMode
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
      .select($"company", $"device_id", $"alert_msg", $"year", $"month", $"day")
      .filter($"year" === LocalDate.now.getYear)
      .filter($"month" === LocalDate.now.getMonthValue)
      .filter($"day" === LocalDate.now.getDayOfMonth)
      .cache()

    // ======================================Company==============================
    val countedSignatureCompanyDf = rawDf
      .groupBy($"company", $"alert_msg")
      .count()
      .sort($"company".desc, $"count".desc)

    val pushSignatureCompanyDf = countedSignatureCompanyDf
      .select(
        $"company", $"alert_msg",
        $"count".alias("value").as[Long]
      )
      .withColumn("year", lit(LocalDate.now.getYear))
      .withColumn("month", lit(LocalDate.now.getMonthValue))
      .withColumn("day", lit(LocalDate.now.getDayOfMonth))

    pushSignatureCompanyDf.show()

    // ======================================Company===============================


    // ======================================Device ID===============================
    val countedSignatureDeviceIdDf = rawDf
      .groupBy($"device_id", $"alert_msg")
      .count()
      .sort($"device_id".desc, $"count".desc)

    val pushSignatureDeviceIdDf = countedSignatureDeviceIdDf
      .select(
        $"device_id", $"alert_msg",
        $"count".alias("value").as[Long]
      )
      .withColumn("year", lit(LocalDate.now.getYear))
      .withColumn("month", lit(LocalDate.now.getMonthValue))
      .withColumn("day", lit(LocalDate.now.getDayOfMonth))

    pushSignatureDeviceIdDf.show()


    // ======================================Device ID===============================

    // ======================================Query===============================
//    pushSignatureCompanyDf
//      .write
//      .format("org.apache.spark.sql.cassandra")
//      .options(Map("keyspace" -> PropertiesLoader.cassandraKeyspace, "table" -> "signature_hit_on_company_day"))
//      .mode(SaveMode.Overwrite)
//      .save()
//
//    pushSignatureDeviceIdDf
//      .write
//      .format("org.apache.spark.sql.cassandra")
//      .options(Map("keyspace" -> PropertiesLoader.cassandraKeyspace, "table" -> "signature_hit_on_device_id_day"))
//      .mode(SaveMode.Overwrite)
//      .save()
  }
}
