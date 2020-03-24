package me.mamotis.kaspacore.jobs

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class BatchJob extends Utils {
  val schema: StructType = StructType(
    Array(
      StructField("ts", StringType, nullable = true),
      StructField("company", StringType, nullable = true),
      StructField("device_id", StringType, nullable = true),
      StructField("year", IntegerType, nullable = true),
      StructField("month", IntegerType, nullable = true),
      StructField("day", IntegerType, nullable = true),
      StructField("hour", IntegerType, nullable = true),
      StructField("minute", IntegerType, nullable = true),
      StructField("second", IntegerType, nullable = true),
      StructField("protocol", StringType, nullable = true),
      StructField("ip_type", StringType, nullable = true),
      StructField("src_mac", StringType, nullable = true),
      StructField("dest_mac", StringType, nullable = true),
      StructField("src_ip", StringType, nullable = true),
      StructField("dest_ip", StringType, nullable = true),
      StructField("src_port", IntegerType, nullable = true),
      StructField("dest_port", IntegerType, nullable = true),
      StructField("alert_msg", StringType, nullable = true),
      StructField("classification", IntegerType, nullable = true),
      StructField("priority", IntegerType, nullable = true),
      StructField("sig_id", IntegerType, nullable = true),
      StructField("sig_gen", IntegerType, nullable = true),
      StructField("sig_rev", IntegerType, nullable = true),
      StructField("src_country", StringType, nullable = true),
      StructField("src_region", StringType, nullable = true),
      StructField("dest_country", StringType, nullable = true),
      StructField("dest_region", StringType, nullable = true)
    )
  )
}
