package observatory

import org.apache.spark.LocalDateUDT
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import java.time.LocalDate

/**
  * 1st milestone: data extraction
  */
object Extraction extends ExtractionInterface {

  import org.apache.log4j.{Level, Logger}
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Observatory")
      .master("local[*]")
      .getOrCreate()

  import spark.implicits._

  LocalDateUDT.register()
  import LocalDateUDT.encoder


  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Temperature)] = {
   locTemps(year, readStations(stationsFile),readTemps(temperaturesFile)).collect()
  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] = {
    avgTemps(records.toSeq.toDS()).collect()
  }

  def avgTemps(t: Dataset[(LocalDate, Location, Temperature)]): Dataset[(Location, Temperature)] = {
    t.groupBy($"_2").agg(avg("_3"))
      .withColumnRenamed("_2","_1").withColumnRenamed("avg(_3)","_2")
      .as[(Location,Temperature)]
  }

  case class RawTempRec(stn: Long, wban: Long, month: Int, day: Int, temp: Double)
  case class RawStationRec(stn: Long, wban: Long, lat: Double, lon: Double)

  def locTemps(year: Year, s: Dataset[RawStationRec], t: Dataset[RawTempRec]): Dataset[(LocalDate, Location, Temperature)] = {
    val todate = udf( (month: Int, day: Int) => LocalDate.of(year, month, day) )
    t.joinWith(s, t("stn") === s("stn") && t("wban") === s("wban")).select(
      todate($"_1.month",$"_1.day").as("_1").as[LocalDate],
      struct($"_2.lat", $"_2.lon" ).as("_2").as[Location],
      $"_1.temp".as("_3").as[Temperature]
    )
  }

  def readTemps(tempFile: String): Dataset[RawTempRec] = {
    spark.read.schema(Encoders.product[RawTempRec].schema).csv(tempFile)
      .na.drop("all", Array("stn", "wban"))
      .na.fill(0, Array("stn", "wban"))
      .na.drop("any", Array("month", "day", "temp"))
      .where($"month".between(1, 12) && $"day".between(1, 31))
      .withColumn("temp", (lit(5) * ($"temp" - lit(32))) / lit(9) )
      .as[RawTempRec]
  }

  def readStations(stationsFile: String): Dataset[RawStationRec] = {
    spark.read.schema(Encoders.product[RawStationRec].schema).csv(stationsFile)
      .na.drop("all", Array(s"stn", "wban"))
      .na.fill(0, Array("stn", "wban"))
      .na.drop("any", Array("lat", "lon"))
      .where($"lat" =!= 0.0 || $"lon" =!= 0.0)
      .dropDuplicates(Seq("stn", "wban")).as[RawStationRec]
  }

}
