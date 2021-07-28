package observatory

import observatory.Extraction.locationYearlyAverageRecords
import observatory.Visualization._
import org.junit.Test
import org.scalatest.matchers.should.Matchers

import java.time.LocalDate

trait VisualizationTest extends MilestoneSuite with Matchers {
  private val milestoneTest = namedMilestoneTest("raw data display", 2) _

  import Spark.spark.implicits._

  @Test def interpolates_temperatures(): Unit = milestoneTest{
    val refs = Extraction.avgTemps(
      Extraction.locTemps( 2048, Extraction.readStations("src/test/resources/stations.csv"),
                           Extraction.readTemps("src/test/resources/temps.csv")) )
    val targets = Seq(Location(69.293, 15.144),Location(69.293, 14.144)).toDS

    sparkPredictTemperatures(refs,targets).collect() should contain only (
      (Location(69.293,15.144),5.597222222222224), (Location(69.293,14.144),-4.888888888888889)
    )
  }

  @Test def interpolates_temperature_from_array(): Unit = milestoneTest{
    predictTemperature(locationYearlyAverageRecords( Array (
      (LocalDate.of(2048,12,21),Location(69.293,14.144),37.05555555555556),
      (LocalDate.of(2048,1,1),Location(69.293,14.144),-4.888888888888889),
      (LocalDate.of(2048,1,1), Location(69.293,16.144),-4.888888888888889)
    )),Location(69.293,15.144) ) should be ( 5.597222222222224 )
  }

  @Test def interpolates_temperature_1p(): Unit = milestoneTest{
    predictTemperature(Array((Location(0.0,0.0),10.0)),Location(90.0,-180.0) ) should be ( 10d )
  }

  @Test def interpolates_temperature_2p_exact(): Unit = milestoneTest{
    predictTemperature( List((Location(45.0,-90.0),10.0), (Location(-45.0,0.0),20.0)),Location(45.0,-90.0) ) should be ( 10d )
  }

  @Test def interpolates_temperature_2p_middle(): Unit = milestoneTest{
    predictTemperature( List((Location(45.0,-90.0),10.0), (Location(-45.0,0.0),20.0)),Location(0,-45.0) ) should be ( 15d )
  }

  @Test def interpolates_color(): Unit = milestoneTest {
    interpolateColor(List((1.0,Color(255,0,0)), (2.0,Color(0,0,255)), (3.0,Color(0,255,0))),1.5) should be (
      Color(128,0,128)
    )
  }

}
