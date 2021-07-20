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
    val d = readTemps(2048, "src/test/resources/temps.csv")
    d.collect() should equal ( Array(
      ((10010,0), Date.valueOf(LocalDate.of(2048,1,1)), -4.888888888888889d),
      ((0,10010), Date.valueOf(LocalDate.of(2048,1,1)), 37.611111111111114d),
      ((0,10010), Date.valueOf(LocalDate.of(2048, 12, 12)), 37.05555555555556d),
      ((1,2), Date.valueOf(LocalDate.of(2048, 12, 21)), 37.05555555555556d),
      )  )
    d.printSchema()
    d.show()
  }

  @Test def reads_stations(): Unit = milestoneTest{
    val d = readStations("src/test/resources/stations.csv")
    d.collect() should contain ( ((10090,0),Location(+80.650, +025.000)) )
    d.printSchema()
    d.show()
  }

}
