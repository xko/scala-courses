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
    val stations = readStations(stationsFile).groupBy(s => (s.stn,s.wban) ).mapValues(_.head)
    val temps = readTemps(temperaturesFile)
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


  def fromCVS[T: ClassTag](name: String)(parse: Array[String] => Iterable[T]): Array[T] = {
    val src: Source = if (Files.isReadable(Paths.get(name))) Source.fromFile(name) else {
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

  def readTemps(file: String): Array[RawTempRec] = fromCVS(file) {
    case Array(stn, wban, month, day, temp) if stn.nonEmpty || wban.nonEmpty =>
      for {
        stn <- Try(if (stn.nonEmpty) stn.toLong else 0).toOption
        wban <- Try(if (wban.nonEmpty) wban.toLong else 0).toOption
        month <- Try(month.toInt).toOption if month > 0 && month <= 12
        day <- Try(day.toInt).toOption if day > 0 && day <= 31
        tempF <- Try(temp.toDouble).toOption
        tempC = (tempF - 32) * 5 / 9
      } yield RawTempRec(stn, wban, month, day, tempC)
    case _ => None
  }


}
