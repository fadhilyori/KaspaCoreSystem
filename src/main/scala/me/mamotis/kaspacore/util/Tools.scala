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
        if(loc.countryName == None) "UNDEFINED"
        else loc.countryName
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
        if(loc.regionName == None) "UNDEFINED"
        else loc.regionName.get
      case _ =>
        "UNDEFINED"
    }
  }
}
