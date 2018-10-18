package me.mamotis.kaspacore.jobs

import java.time.LocalDate

import me.mamotis.kaspacore.util.PropertiesLoader

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.{lit}

object MonthlyCount extends Utils {

  def main(args: Array[String]): Unit = {
    val sparkSession = getSparkSession(args)
    val sparkContext = getSparkContext(sparkSession)

    import sparkSession.implicits._
    sparkContext.setLogLevel("ERROR")

    val rawDf = sparkSession.read.json(PropertiesLoader.hadoopEventFilePath)
      .select($"company", $"device_id", $"protocol", $"src_port", $"dest_port", $"src_ip", $"dest_ip", $"src_country",
        $"dest_country", $"alert_msg", $"year", $"month")
      .na.fill(Map("src_country" -> "UNDEFINED", "dest_country" -> "UNDEFINED"))
      .filter($"year" === LocalDate.now.getYear)
      .filter($"month" === LocalDate.now.getMonthValue)
      .filter($"day" === LocalDate.now.getDayOfMonth)
      .cache()

    // ======================================Company==============================
    // Hit Count
    val countedHitCompanyDf = rawDf
      .groupBy($"company")
      .count()
      .sort($"company".desc, $"count".desc)

    val pushHitCompanyDf = countedHitCompanyDf
      .select(
        $"company",
        $"count".alias("value").as[Long]
      )
      .withColumn("year", lit(LocalDate.now.getYear))
      .withColumn("month", lit(LocalDate.now.getMonthValue))


    // Signature
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


    //Protocol
    val countedProtocolCompanyDf = rawDf
      .groupBy($"company", $"protocol")
      .count()
      .sort($"company".desc, $"count".desc)

    val pushProtocolCompanyDf = countedProtocolCompanyDf
      .select(
        $"company", $"protocol",
        $"count".alias("value").as[Long]
      )
      .withColumn("year", lit(LocalDate.now.getYear))
      .withColumn("month", lit(LocalDate.now.getMonthValue))


    //ProtocolBySPort
    val countedProtocolBySPortCompanyDf = rawDf
      .groupBy($"company", $"protocol", $"src_port")
      .count()
      .sort($"company".desc, $"count".desc)

    val pushProtocolBySPortCompanyDf = countedProtocolBySPortCompanyDf
      .select(
        $"company", $"protocol", $"src_port",
        $"count".alias("value").as[Long]
      )
      .withColumn("year", lit(LocalDate.now.getYear))
      .withColumn("month", lit(LocalDate.now.getMonthValue))


    //ProtocolByDPort
    val countedProtocolByDPortCompanyDf = rawDf
      .groupBy($"company", $"protocol", $"dest_port")
      .count()
      .sort($"company".desc, $"count".desc)

    val pushProtocolByDPortCompanyDf = countedProtocolByDPortCompanyDf
      .select(
        $"company", $"protocol", $"dest_port",
        $"count".alias("value").as[Long]
      )
      .withColumn("year", lit(LocalDate.now.getYear))
      .withColumn("month", lit(LocalDate.now.getMonthValue))


    //IpSrc
    val countedIpSrcCompanyDf = rawDf
      .groupBy($"company", $"src_country", $"src_ip")
      .count()
      .sort($"company".desc, $"count".desc)

    val pushIpSrcCompanyDf = countedIpSrcCompanyDf
      .select(
        $"company", $"src_country", $"src_ip",
        $"count".alias("value").as[Long]
      )
      .withColumn("year", lit(LocalDate.now.getYear))
      .withColumn("month", lit(LocalDate.now.getMonthValue))


    //IpDest
    val countedIpDestCompanyDf = rawDf
      .groupBy($"company", $"dest_country", $"dest_ip")
      .count()
      .sort($"company".desc, $"count".desc)

    val pushIpDestCompanyDf = countedIpDestCompanyDf
      .select(
        $"company", $"dest_country", $"dest_ip",
        $"count".alias("value").as[Long]
      )
      .withColumn("year", lit(LocalDate.now.getYear))
      .withColumn("month", lit(LocalDate.now.getMonthValue))


    //CountrySrc
    val countedCountrySrcCompanyDf = rawDf
      .groupBy($"company", $"src_country")
      .count()
      .sort($"company".desc, $"count".desc)

    val pushCountrySrcCompanyDf = countedCountrySrcCompanyDf
      .select(
        $"company", $"src_country",
        $"count".alias("value").as[Long]
      )
      .withColumn("year", lit(LocalDate.now.getYear))
      .withColumn("month", lit(LocalDate.now.getMonthValue))


    //CountryDest
    val countedCountryDestCompanyDf = rawDf
      .groupBy($"company", $"dest_country")
      .count()
      .sort($"company".desc, $"count".desc)

    val pushCountryDestCompanyDf = countedCountryDestCompanyDf
      .select(
        $"company", $"dest_country",
        $"count".alias("value").as[Long]
      )
      .withColumn("year", lit(LocalDate.now.getYear))
      .withColumn("month", lit(LocalDate.now.getMonthValue))



    // ======================================Company===============================


    // ======================================Device ID===============================
    // Hit Count
    val countedHitDeviceIdDf = rawDf
      .groupBy($"device_id")
      .count()
      .sort($"device_id".desc, $"count".desc)

    val pushHitDeviceIdDf = countedHitDeviceIdDf
      .select(
        $"device_id",
        $"count".alias("value").as[Long]
      )
      .withColumn("year", lit(LocalDate.now.getYear))
      .withColumn("month", lit(LocalDate.now.getMonthValue))


    // Signature
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


    // Protocol
    val countedProtocolDeviceIdDf = rawDf
      .groupBy($"device_id", $"protocol")
      .count()
      .sort($"device_id".desc, $"count".desc)

    val pushProtocolDeviceIdDf = countedProtocolDeviceIdDf
      .select(
        $"device_id", $"protocol",
        $"count".alias("value").as[Long]
      )
      .withColumn("year", lit(LocalDate.now.getYear))
      .withColumn("month", lit(LocalDate.now.getMonthValue))


    //ProtocolBySPort
    val countedProtocolBySPortDeviceIdDf = rawDf
      .groupBy($"device_id", $"protocol", $"src_port")
      .count()
      .sort($"device_id".desc, $"count".desc)

    val pushProtocolBySPortDeviceIdDf = countedProtocolBySPortDeviceIdDf
      .select(
        $"device_id", $"protocol", $"src_port",
        $"count".alias("value").as[Long]
      )
      .withColumn("year", lit(LocalDate.now.getYear))
      .withColumn("month", lit(LocalDate.now.getMonthValue))


    //ProtocolByDPort
    val countedProtocolByDPortDeviceIdDf = rawDf
      .groupBy($"device_id", $"protocol", $"dest_port")
      .count()
      .sort($"device_id".desc, $"count".desc)

    val pushProtocolByDPortDeviceIdDf = countedProtocolByDPortDeviceIdDf
      .select(
        $"device_id", $"protocol", $"dest_port",
        $"count".alias("value").as[Long]
      )
      .withColumn("year", lit(LocalDate.now.getYear))
      .withColumn("month", lit(LocalDate.now.getMonthValue))


    //IpSrc
    val countedIpSrcDeviceIdDf = rawDf
      .groupBy($"device_id", $"src_ip")
      .count()
      .sort($"device_id".desc, $"count".desc)

    val pushIpSrcDeviceIdDf = countedIpSrcDeviceIdDf
      .select(
        $"device_id", $"src_ip",
        $"count".alias("value").as[Long]
      )
      .withColumn("year", lit(LocalDate.now.getYear))
      .withColumn("month", lit(LocalDate.now.getMonthValue))


    //IpDest
    val countedIpDestDeviceIdDf = rawDf
      .groupBy($"device_id", $"dest_ip")
      .count()
      .sort($"device_id".desc, $"count".desc)

    val pushIpDestDeviceIdDf = countedIpDestDeviceIdDf
      .select(
        $"device_id", $"dest_ip",
        $"count".alias("value").as[Long]
      )
      .withColumn("year", lit(LocalDate.now.getYear))
      .withColumn("month", lit(LocalDate.now.getMonthValue))


    //CountrySrc
    val countedCountrySrcDeviceIdDf = rawDf
      .groupBy($"device_id", $"src_country")
      .count()
      .sort($"device_id".desc, $"count".desc)

    val pushCountrySrcDeviceIdDf = countedCountrySrcDeviceIdDf
      .select(
        $"device_id", $"src_country",
        $"count".alias("value").as[Long]
      )
      .withColumn("year", lit(LocalDate.now.getYear))
      .withColumn("month", lit(LocalDate.now.getMonthValue))


    //CountryDest
    val countedCountryDestDeviceIdDf = rawDf
      .groupBy($"device_id", $"dest_country")
      .count()
      .sort($"device_id".desc, $"count".desc)

    val pushCountryDestDeviceIdDf = countedCountryDestDeviceIdDf
      .select(
        $"device_id", $"dest_country",
        $"count".alias("value").as[Long]
      )
      .withColumn("year", lit(LocalDate.now.getYear))
      .withColumn("month", lit(LocalDate.now.getMonthValue))


    // ======================================Device ID===============================

    // ======================================Query Company===============================
    // Event Hit
    pushHitCompanyDf
      .write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> PropertiesLoader.cassandraKeyspace, "table" -> "event_hit_on_company_month"))
      .mode(SaveMode.Append)
      .save()

    // Signature
    pushSignatureCompanyDf
      .write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> PropertiesLoader.cassandraKeyspace, "table" -> "signature_hit_on_company_month"))
      .mode(SaveMode.Append)
      .save()

    // Protocol
    pushProtocolCompanyDf
      .write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> PropertiesLoader.cassandraKeyspace, "table" -> "protocol_hit_on_company_month"))
      .mode(SaveMode.Append)
      .save()

    // Protocol Source Port
    pushProtocolBySPortCompanyDf
      .write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> PropertiesLoader.cassandraKeyspace, "table" -> "protocol_by_sport_hit_on_company_month"))
      .mode(SaveMode.Append)
      .save()

    // Protocol Dest Port
    pushProtocolByDPortCompanyDf
      .write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> PropertiesLoader.cassandraKeyspace, "table" -> "protocol_by_dport_hit_on_company_month"))
      .mode(SaveMode.Append)
      .save()

    // IP Source
    pushIpSrcCompanyDf
      .write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> PropertiesLoader.cassandraKeyspace, "table" -> "ip_source_hit_on_company_month"))
      .mode(SaveMode.Append)
      .save()

    // IP Destination
    pushIpDestCompanyDf
      .write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> PropertiesLoader.cassandraKeyspace, "table" -> "ip_dest_hit_on_company_month"))
      .mode(SaveMode.Append)
      .save()

    // Source Country
    pushCountrySrcCompanyDf
      .write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> PropertiesLoader.cassandraKeyspace, "table" -> "country_source_hit_on_company_month"))
      .mode(SaveMode.Append)
      .save()

    // Destination Country
    pushCountryDestCompanyDf
      .write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> PropertiesLoader.cassandraKeyspace, "table" -> "country_dest_hit_on_company_month"))
      .mode(SaveMode.Append)
      .save()

    // ======================================Query Device ID===============================
    // Event Hit
    pushHitDeviceIdDf
      .write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> PropertiesLoader.cassandraKeyspace, "table" -> "event_hit_on_device_id_month"))
      .mode(SaveMode.Append)
      .save()

    // Signature
    pushSignatureDeviceIdDf
      .write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> PropertiesLoader.cassandraKeyspace, "table" -> "signature_hit_on_device_id_month"))
      .mode(SaveMode.Append)
      .save()

    // Protocol
    pushProtocolDeviceIdDf
      .write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> PropertiesLoader.cassandraKeyspace, "table" -> "protocol_hit_on_device_id_month"))
      .mode(SaveMode.Append)
      .save()

    // Protocol Source Port
    pushProtocolBySPortDeviceIdDf
      .write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> PropertiesLoader.cassandraKeyspace, "table" -> "protocol_by_sport_hit_on_device_id_month"))
      .mode(SaveMode.Append)
      .save()

    // Protocol Dest Port
    pushProtocolByDPortDeviceIdDf
      .write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> PropertiesLoader.cassandraKeyspace, "table" -> "protocol_by_dport_hit_on_device_id_month"))
      .mode(SaveMode.Append)
      .save()

    // IP Source
    pushIpSrcDeviceIdDf
      .write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> PropertiesLoader.cassandraKeyspace, "table" -> "ip_source_hit_on_device_id_month"))
      .mode(SaveMode.Append)
      .save()

    // IP Destination
    pushIpDestDeviceIdDf
      .write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> PropertiesLoader.cassandraKeyspace, "table" -> "ip_dest_hit_on_device_id_month"))
      .mode(SaveMode.Append)
      .save()

    // Source Country
    pushCountrySrcDeviceIdDf
      .write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> PropertiesLoader.cassandraKeyspace, "table" -> "country_source_hit_on_device_id_month"))
      .mode(SaveMode.Append)
      .save()

    // Destination Country
    pushCountryDestDeviceIdDf
      .write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> PropertiesLoader.cassandraKeyspace, "table" -> "country_dest_hit_on_device_id_month"))
      .mode(SaveMode.Append)
      .save()
  }

}
