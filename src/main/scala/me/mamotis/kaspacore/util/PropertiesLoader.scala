package me.mamotis.kaspacore.util

import com.typesafe.config.{Config, ConfigFactory}


object PropertiesLoader {
  private val conf: Config = ConfigFactory.load("application.conf")

  val kafkaBrokerUrl = conf.getString("KAFKA_BROKER_URL")
  val schemaRegistryUrl = conf.getString("SCHEMA_REGISTRY_URL")
  val kafkaInputTopic = conf.getString("KAFKA_INPUT_TOPIC")
  val kafkaStartingOffset = conf.getString("KAFKA_STARTING_OFFSET")

  val sparkMaster = conf.getString("SPARK_MASTER")
  val sparkAppName = conf.getString("SPARK_APP_NAME")
  val sparkAppId = conf.getString("SPARK_APP_ID")
  val sparkCassandraConnectionHost = conf.getString("SPARK_CASSANDRA_CONNECTION_HOST")

  val GeoIpPath = conf.getString("GEOIP_PATH")
  val GeoIpFilename = conf.getString("GEOIP_FILENAME")

  val hadoopEventFilePath = conf.getString("HADOOP_EVENT_FILE_PATH")
  val hadoopSchemaFilePath = conf.getString("HADOOP_SCHEMA_FILE_PATH")
  val checkpointLocation = conf.getString("CHECKPOINT_LOCATION")

  val cassandraUsername = conf.getString("CASSANDRA_USERNAME")
  val cassandraPassword = conf.getString("CASSANDRA_PASSWORD")
}
