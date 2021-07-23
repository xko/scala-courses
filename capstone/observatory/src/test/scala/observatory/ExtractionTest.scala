package observatory

import org.junit.Assert._
import org.junit.Test
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.scalatest.matchers.should.Matchers

import java.sql.Date
import java.time.LocalDate


trait ExtractionTest extends MilestoneSuite with Matchers {
  private val milestoneTest = namedMilestoneTest("data extraction", 1) _

  import Extraction._
  import spark.implicits._

  @Test def reads_temperatures(): Unit = milestoneTest{
    val d = readTemps("src/test/resources/temps.csv")
    d.printSchema()
    d.show()
    d.collect() should equal (Array(
      RawTempRec( 10010,0, 1,  1,  -4.888888888888889d),
      RawTempRec( 0,10010, 1,  1,  37.611111111111114d),
      RawTempRec( 0,10010, 12, 12, 37.05555555555556d ),
      RawTempRec( 1,2,     12, 21, 37.05555555555556d ),
    ))
  }

  @Test def reads_stations(): Unit = milestoneTest{
    val d = readStations("src/test/resources/stations.csv")
    d.collect() should contain only (
      RawStationRec(10010,0,+69.293,+016.144),
      RawStationRec(10110,0,0,+031.500),
      RawStationRec(10090,0,+80.650,+025.000),
      RawStationRec(0,10110,0,+031.500),
      RawStationRec(1,2,+69.293,+014.144)
    )
  }

  @Test def locates_temperatures(): Unit = milestoneTest {
    val d = locTemps(2048, readStations("src/test/resources/stations.csv"), readTemps("src/test/resources/temps.csv"))
    d.show()
    d.printSchema()
    d.collect() should contain only  (
      (LocalDate.of(2048,12,21),Location(69.293,14.144),37.05555555555556),
      (LocalDate.of(2048,1,1), Location(69.293,16.144),-4.888888888888889)
    )
  }

}
