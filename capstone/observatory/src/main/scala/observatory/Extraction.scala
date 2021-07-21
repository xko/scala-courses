package observatory

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import java.sql.Date
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


  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Temperature)] = {
   locTemps(readStations(stationsFile),readTemps(year,temperaturesFile)).collect().map{
     case (date, loc, temp) =>  (date.toLocalDate, loc, temp)
   }
  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] = {
    ???
  }

  type STN = Long
  type WBAN = Long
  type TempRec = (STN, WBAN, Date, Temperature)
  type StationRec = (STN, WBAN, Location)

  private val DateCol = "date"
  private val TempCol = "temp"
  private val LocationCol = "loc"
  private val StationKeyCol = "sttn"

  def locTemps(stations: Dataset[StationRec], temps: Dataset[TempRec]): Dataset[(Date, Location, Temperature)] = {
    val t = temps.withColumnRenamed(temps.columns(2), DateCol).withColumnRenamed(temps.columns(3), TempCol)
    val s = stations.withColumnRenamed(stations.columns(2), LocationCol)
    t.join(s,Seq(t.columns(0),t.columns(1)) ).select(DateCol, LocationCol, TempCol).as[(Date, Location, Temperature)]
  }

  def readTemps(year: Year, tempFile: String): Dataset[TempRec] = {
    val tod = udf((year:Int, month:Int, day:Int) => Date.valueOf(LocalDate.of(year,month,day)))
    spark.read.schema("stn LONG, wban LONG, month INT, day INT, temp DOUBLE").csv(tempFile)
      .na.drop("all",Array("stn","wban"))
      .na.fill(0,Array("stn","wban"))
      .na.drop("any",Array("month","day","temp"))
      .where( $"month".between(1,12) && $"day".between(1,31) )
      .select(
        $"stn".as[STN],$"wban".as[WBAN],
        tod(lit(year),$"month",$"day").as[Date],
        ( (lit(5) * ($"temp" - lit(32))) / lit(9) ).as[Double]
      )
  }


  def readStations(stationsFile: String): Dataset[StationRec] = {
    spark.read.schema("stn LONG, wban LONG, lat DOUBLE, lon DOUBLE").csv(stationsFile)
      .na.drop("all",Array("stn","wban"))
      .na.fill(0,Array("stn","wban"))
      .na.drop("any",Array("lat","lon"))
      .where($"lat"=!=0.0 || $"lon"=!=0.0)
      .select(
        $"stn".as[STN],$"wban".as[WBAN],
        struct($"lat", $"lon" ).as[Location]
       ).dropDuplicates(Seq("stn","wban"))
  }

}
