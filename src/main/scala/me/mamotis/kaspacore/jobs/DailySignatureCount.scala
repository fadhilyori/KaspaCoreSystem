package me.mamotis.kaspacore.jobs

import me.mamotis.kaspacore.util.PropertiesLoader
import java.time.LocalDate

object DailySignatureCount extends Utils {

  def main(args: Array[String]): Unit = {
    val sparkSession = getSparkSession(args)
    val sparkContext = getSparkContext(sparkSession)

    val connector = getCassandraSession(sparkContext)

    import sparkSession.implicits._
    sparkContext.setLogLevel("ERROR")

    // Raw Event Dataframe Parsing
    val rawDf = sparkSession.read.json(PropertiesLoader.hadoopEventFilePath)

    // Filter for spesific date/todays date
    val filteredEventsDf = rawDf
      .select($"company", $"alert_msg", $"year", $"month", $"day")
      .filter($"year" === LocalDate.now.getYear)
      .filter($"month" === LocalDate.now.getMonthValue)
      .filter($"day" === LocalDate.now.getDayOfMonth)

    filteredEventsDf.show(10)

    val countedSignatureDf = filteredEventsDf
      .groupBy($"company", $"alert_msg")
      .count()

    countedSignatureDf.show()
  }

}
