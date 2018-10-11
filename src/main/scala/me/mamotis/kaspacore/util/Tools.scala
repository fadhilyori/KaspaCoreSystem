package me.mamotis.kaspacore.util

import cats.effect.IO
import com.snowplowanalytics.maxmind.iplookups.IpLookups
import org.apache.spark.SparkFiles

object Tools {

  def IpLookupCountry(ipAddress: String): String = {
    val result = (for {
      ipLookups <- IpLookups.createFromFilenames[IO](
        geoFile = Some(SparkFiles.get(PropertiesLoader.GeoIpFilename)),
        ispFile = None,
        domainFile = None,
        connectionTypeFile = None,
        memCache = false,
        lruCacheSize = 20000
      )

      lookup <- ipLookups.performLookups(ipAddress)
    } yield lookup).unsafeRunSync()

    result.ipLocation match {
      case Some(Right(loc)) =>
        println(loc.countryName)
        "TEST Country"
      case _ =>
        "UNDEFINED"
    }
  }

  def IpLookupRegion(ipAddress: String): String = {
    val result = (for {
      ipLookups <- IpLookups.createFromFilenames[IO](
        geoFile = Some(SparkFiles.get(PropertiesLoader.GeoIpFilename)),
        ispFile = None,
        domainFile = None,
        connectionTypeFile = None,
        memCache = false,
        lruCacheSize = 20000
      )

      lookup <- ipLookups.performLookups(ipAddress)
    } yield lookup).unsafeRunSync()

    result.ipLocation match {
      case Some(Right(loc)) =>
//        loc.regionName.get
        println(loc.regionName)
        "TESTCountry"
      case _ =>
        "UNDEFINED"
    }
  }
}
