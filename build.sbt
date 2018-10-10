name := "KaspaCore"

version := "0.1"

scalaVersion := "2.11.12"

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.fasterxml.**" -> "shadeio.@1")
  .inLibrary("com.fasterxml.jackson.core" % "jackson-databind" % "2.9.7")
  .inLibrary("com.fasterxml.jackson.core" % "jackson-core" % "2.9.7")
  .inLibrary("com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.9.7")
  .inLibrary("com.fasterxml.jackson.core" % "jackson-annotations" % "2.9.7")
  .inProject
)

resolvers += "confluent" at "http://packages.confluent.io/maven/"
resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.9.7"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.7"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.9.7"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-annotations" % "2.9.7"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.1" % "provided"
//libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.1"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.1" % "provided"
//libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.1"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.3.1"

libraryDependencies += "io.confluent" % "kafka-avro-serializer" % "5.0.0"

libraryDependencies += "com.databricks" % "spark-avro_2.11" % "4.0.0"

libraryDependencies += "org.scalaz" % "scalaz-core_2.11" % "7.3.0-M21"

// https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.0.0"

libraryDependencies += "com.amazonaws" % "aws-java-sdk-kms" % "1.11.313"

libraryDependencies += "joda-time" % "joda-time" % "2.10"

// https://mvnrepository.com/artifact/com.datastax.spark/spark-cassandra-connector
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.3.1"

libraryDependencies += "com.snowplowanalytics" %% "scala-maxmind-iplookups" % "0.5.0"

libraryDependencies += "org.typelevel" %% "cats-effect-laws" % "1.0.0" % "test"

libraryDependencies += "com.typesafe" % "config" % "1.3.2"

assemblyMergeStrategy in assembly := {
  {
    case "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister" => MergeStrategy.concat
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
  }
}