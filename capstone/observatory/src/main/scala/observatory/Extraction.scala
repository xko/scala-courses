package observatory

import org.apache.spark.sql._
import org.apache.spark.sql.types._

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
    ???
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
  type StationKey = (STN,WBAN)
  type TempRec = (StationKey, Date, Temperature)
  type StationRec = (StationKey, Location)


  def readTemps(year: Year, tempFile: String): Dataset[TempRec] = {
    val enc = Encoders.product[(Option[STN], Option[WBAN], Int, Int, Double)]
    spark.read.schema(enc.schema).csv(tempFile)
      .na.drop("all",Array("_1","_2"))
      .na.drop("any",Array("_3","_4","_5"))
      .where( $"_4".between(1,12) && $"_3".between(1,31) )
      .map { r =>
        ( (r.getAs[Long](0), r.getAs[Long](1)),
          Date.valueOf(LocalDate.of(year, r.getAs[Int](2), r.getAs[Int](3))),
          (5 * (r.getAs[Double](4) - 32)) / 9
        )
      }
  }


  def readStations(stationsFile: String): Dataset[StationRec] = {
    val enc = Encoders.product[(Option[STN], Option[WBAN], Double, Double)]
    spark.read.schema(enc.schema).csv(stationsFile)
      .na.drop("all",Array("_1","_2"))
      .na.drop("any",Array("_3","_4"))
      .where($"_3"=!=0.0 || $"_4"=!=0.0)
      .map { r =>
         ( (r.getAs[Long](0), r.getAs[Long](1)),
           Location(r.getAs[Double](2), r.getAs[Double](3))
         )
      }.dropDuplicates("_1")
  }

}
