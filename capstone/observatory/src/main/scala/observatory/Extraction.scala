package observatory

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import java.time.LocalDate
import scala.io.Source

/**
  * 1st milestone: data extraction
  */
object Extraction extends ExtractionInterface {

  import Spark._
  import spark.implicits._

  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Temperature)] = {
   locTemps(year, readStations(readResource(stationsFile)), readTemps(readResource(temperaturesFile))).collect()
  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] = {
    avgTemps(records.toSeq.toDS()).collect()
  }

  def avgTemps(t: Dataset[(LocalDate, Location, Temperature)]): Dataset[(Location, Temperature)] = {
    t.groupBy($"_2").agg(avg("_3")).asProduct[(Location,Temperature)]
  }

  case class RawTempRec(stn: Long, wban: Long, month: Int, day: Int, temp: Double)
  case class RawStationRec(stn: Long, wban: Long, lat: Double, lon: Double)

  def locTemps(year: Year, s: Dataset[RawStationRec], t: Dataset[RawTempRec]): Dataset[(LocalDate, Location, Temperature)] = {
    val todate = udf( (month: Int, day: Int) => LocalDate.of(year, month, day) )
    t.joinWith(s, t("stn") === s("stn") && t("wban") === s("wban")).select(
      todate($"_1.month",$"_1.day"), asProduct[Location]($"_2.lat", $"_2.lon"), $"_1.temp"
      ).asProduct[(LocalDate,Location,Temperature)]
  }

  def readTemps(file: String): Dataset[RawTempRec] =
    readTemps(spark.read.text(file).as[String])

  def readTemps(lines: Dataset[String]): Dataset[RawTempRec] = {
    val s = Encoders.product[RawTempRec].schema
    spark.read.schema( s.copy(s.fields.map(_.copy(nullable = true))) ).csv(lines)
      .na.drop("all", Array("stn", "wban"))
      .na.fill(0, Array("stn", "wban"))
      .na.drop("any", Array("month", "day", "temp"))
      .where($"month".between(1, 12) && $"day".between(1, 31))
      .withColumn("temp", (lit(5) * ($"temp" - lit(32))) / lit(9) )
      .as[RawTempRec]
  }

  def readStations(file: String): Dataset[RawStationRec] =
    readStations(spark.read.text(file).as[String])


  def readStations(lines: Dataset[String]): Dataset[RawStationRec] = {
    val s = Encoders.product[RawStationRec].schema
    spark.read.schema( s.copy(s.fields.map(_.copy(nullable = true))) ).csv(lines)
      .na.drop("all", Array(s"stn", "wban"))
      .na.fill(0, Array("stn", "wban"))
      .na.drop("any", Array("lat", "lon"))
      .where($"lat" =!= 0.0 || $"lon" =!= 0.0)
      .dropDuplicates(Seq("stn", "wban")).as[RawStationRec]
  }

  def readResource(resource: String): Dataset[String] = {
    val stream = Source.getClass.getResourceAsStream(resource)
    try spark.sparkContext.makeRDD(Source.fromInputStream(stream).getLines().toList).toDS
    finally if(stream != null) stream.close()
  }

}
