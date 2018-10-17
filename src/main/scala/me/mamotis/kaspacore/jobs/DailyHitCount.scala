package me.mamotis.kaspacore.jobs

import me.mamotis.kaspacore.util.PropertiesLoader
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{lit}

object DailyHitCount extends Utils {

  def main(args: Array[String]): Unit = {
    val sparkSession = getSparkSession(args)
    val sparkContext = getSparkContext(sparkSession)

    val connector = getCassandraSession(sparkContext)

    import sparkSession.implicits._
    sparkContext.setLogLevel("ERROR")

    val rawDf = sparkSession.read.json(PropertiesLoader.hadoopEventFilePath)
    val countedDf = rawDf.withColumn("val", lit(1))
      .groupBy($"company").sum("val")

    countedDf.show(10)
  }
}
