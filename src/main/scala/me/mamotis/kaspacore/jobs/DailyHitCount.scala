package me.mamotis.kaspacore.jobs

import me.mamotis.kaspacore.util.PropertiesLoader
import org.apache.spark.sql.types._

object DailyHitCount extends Utils {

  def main(args: Array[String]): Unit = {
    val sparkSession = getSparkSession(args)
    val sparkContext = getSparkContext(sparkSession)

    val connector = getCassandraSession(sparkContext)

    val schema = new StructType()
      .add("ts", StringType, true)
      .add("company", StringType, true)
      .add("device_id", StringType, true)
      .add("year", IntegerType, true)
      .add("month", IntegerType, true)
      .add("day", IntegerType, true)
      .add("hour", IntegerType, true)
      .add("minute", IntegerType, true)
      .add("second", IntegerType, true)
      .add("protocol", StringType, true)
      .add("ip_type", StringType, true)
      .add("src_mac", StringType, true)
      .add("dest_mac", StringType, true)
      .add("src_ip", StringType, true)
      .add("dest_ip", StringType, true)
      .add("src_port", IntegerType, true)
      .add("dest_port", IntegerType, true)
      .add("alert_msg", StringType, true)
      .add("classification", IntegerType, true)
      .add("priority", IntegerType, true)
      .add("sig_id", IntegerType, true)
      .add("sig_gen", IntegerType, true)
      .add("sig_rev", IntegerType, true)
      .add("src_country", StringType, true)
      .add("src_region", StringType, true)
      .add("dest_country", StringType, true)
      .add("dest_region", StringType, true)
    import sparkSession.implicits._
    sparkContext.setLogLevel("ERROR")

    val df = sparkSession
        .readStream
        .format("json")
        .schema(schema)
        .load(PropertiesLoader.hadoopEventFilePath)

    df.printSchema()

    df.writeStream.format("console").start().awaitTermination()
  }
}
