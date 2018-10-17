package me.mamotis.kaspacore.jobs

import me.mamotis.kaspacore.util.PropertiesLoader

object DailyHitCount extends Utils {

  def main(args: Array[String]): Unit = {
    val sparkSession = getSparkSession(args)
    val sparkContext = getSparkContext(sparkSession)

    val connector = getCassandraSession(sparkContext)

    import sparkSession.implicits._
    sparkContext.setLogLevel("ERROR")

    val df = sparkSession
      .read
      .json(PropertiesLoader.hadoopEventFilePath)

    df.show()

  }
}
