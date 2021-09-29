package observatory

import java.io.FileNotFoundException
import java.nio.file.{Files, Paths}
import java.time.LocalDate
import scala.io.Source
import scala.reflect.ClassTag
import scala.util.Try

/**
  * 1st milestone: data extraction
  */
object Extraction extends ExtractionInterface {
  case class RawTempRec(stn: Long, wban: Long, month: Int, day: Int, temp: Double)
  case class RawStationRec(stn: Long, wban: Long, lat: Double, lon: Double)


  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Temperature)] = {
    val stations = PlainImpl.readStations(stationsFile).groupBy(s => (s.stn,s.wban) ).mapValues(_.head)
    val temps = PlainImpl.readTemps(temperaturesFile)
    temps.groupBy(t => (t.stn,t.wban) ).mapValues { recs =>
      for {
        trec <- recs
        srec <- stations.get((trec.stn,trec.wban))
        date = LocalDate.of(year,trec.month,trec.day)
        loc = Location(srec.lat,srec.lon)
      } yield (date,loc,trec.temp)
    }.values.flatten

  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] = {
    records.par.groupBy(_._2).mapValues(_.map(_._3)).mapValues { temps =>
      val (count,sum) = temps.aggregate(0 -> 0.0)(
        (agg, t) => (agg._1 + 1) -> (agg._2 + t),
        (agg, bgg) => (agg._1 + bgg._1) -> (agg._2 + bgg._2)
      )
      sum/count
    }.seq
  }


  object PlainImpl {
    def fromCVS[T:ClassTag](name:String)(parse: Array[String] => Iterable[T] ): Array[T] = {
      val src: Source = if( Files.isReadable(Paths.get(name))) Source.fromFile(name) else {
        val stream = Source.getClass.getResourceAsStream(name)
        if (stream == null) throw new FileNotFoundException(name)
        Source.fromInputStream(stream)
      }
      try src.getLines().map(_.split(',').map(_.trim)).flatMap(parse).toArray
      finally src.close()
    }

    def readStations(file: String): Array[RawStationRec] = fromCVS(file) {
      case Array(stn, wban, lat, lon) if stn.nonEmpty || wban.nonEmpty =>
        for {
          stn  <- Try(if (stn.nonEmpty) stn.toLong else 0).toOption
          wban <- Try(if (wban.nonEmpty) wban.toLong else 0).toOption
          lat  <- Try(lat.toDouble).toOption
          lon  <- Try(lon.toDouble).toOption
        } yield RawStationRec(stn, wban, lat, lon)
      case _ => None
    }

    def readTemps(file:String): Array[RawTempRec] = fromCVS(file) {
      case Array(stn, wban, month, day, temp) if stn.nonEmpty || wban.nonEmpty =>
        for {
          stn <- Try(if (stn.nonEmpty) stn.toLong else 0).toOption
          wban <- Try(if (wban.nonEmpty) wban.toLong else 0).toOption
          month <- Try(month.toInt).toOption if month > 0 && month <= 12
          day <- Try(day.toInt).toOption if day > 0 && day <= 31
          tempF <- Try(temp.toDouble).toOption
          tempC = (tempF-32)*5/9
        } yield RawTempRec(stn, wban, month, day, tempC)
      case _ => None
    }


  }

  object SparkImpl {
    import Spark._
    import org.apache.spark.sql._
    import org.apache.spark.sql.functions._
    import spark.implicits._

    def avgTemps(t: Dataset[(LocalDate, Location, Temperature)]): Dataset[(Location, Temperature)] = {
      t.groupBy($"_2").agg(avg("_3")).asNamed
    }

    case class RawTempRec(stn: Long, wban: Long, month: Int, day: Int, temp: Double)
    case class RawStationRec(stn: Long, wban: Long, lat: Double, lon: Double)

    def locTemps(year: Year, s: Dataset[RawStationRec], t: Dataset[RawTempRec]): Dataset[(LocalDate, Location, Temperature)] = {
      t.joinWith(s, t("stn") === s("stn") && t("wban") === s("wban")).select(
        asDate(lit(year),$"_1.month", $"_1.day"), asNamed[Location]($"_2.lat", $"_2.lon"), $"_1.temp"
        ).asNamed
    }



    def readTemps(lines: Dataset[String]): Dataset[RawTempRec] = csv[RawTempRec](lines) {
      _ .na.drop("all", Array("stn", "wban"))
        .na.fill(0, Array("stn", "wban"))
        .na.drop("any", Array("month", "day", "temp"))
        .where($"month".between(1, 12) && $"day".between(1, 31))
        .withColumn("temp", (lit(5) * ($"temp" - lit(32))) / lit(9)) //F to C
    }

    def readTemps(file: String): Dataset[RawTempRec] = readTemps(text(file))


    def readStations(lines: Dataset[String]): Dataset[RawStationRec] = csv[RawStationRec](lines){
      _ .na.drop("all", Array("stn", "wban"))
        .na.fill(0, Array("stn", "wban"))
        .na.drop("any", Array("lat", "lon"))
        .where($"lat" =!= 0.0 || $"lon" =!= 0.0)
        .dropDuplicates(Seq("stn", "wban"))
    }

    def readStations(file: String): Dataset[RawStationRec] = readStations(text(file))

  }

}
