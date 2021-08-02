package observatory

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.time.LocalDate


class ExtractionTest extends AnyFunSpec with Matchers {
  import Extraction._
  describe("graded methods") {
    it("locates temperatures") {
      locateTemperatures(2048, "/stations.csv", "/temps.csv") should contain only(
        (LocalDate.of(2048, 12, 21), Location(69.293, 14.144), 37.05555555555556),
        (LocalDate.of(2048, 1, 1), Location(69.293, 14.144), -4.888888888888889),
        (LocalDate.of(2048, 1, 1), Location(69.293, 16.144), -4.888888888888889)
      )
    }

    it("averages temperatures") {
      locationYearlyAverageRecords( Array (
        (LocalDate.of(2048,12,21),Location(69.293,14.144),37.05555555555556),
        (LocalDate.of(2048,1,1),Location(69.293,14.144),-4.888888888888889),
        (LocalDate.of(2048,1,1), Location(69.293,16.144),-4.888888888888889)
        )) should contain only (
        (Location(69.293, 16.144),-4.888888888888889),
        (Location(69.293, 14.144),16.083333333333336)
      )
    }

  }

  describe("spark implementation"){
    import SparkImpl._
    it("reads temperatures") {
      val d = readTemps("src/test/resources/temps.csv")
      d.collect() should equal(Array(
        RawTempRec(10010, 0, 1, 1, -4.888888888888889d),
        RawTempRec(1, 2, 1, 1, -4.888888888888889d),
        RawTempRec(0, 10010, 1, 1, 37.611111111111114d),
        RawTempRec(0, 10010, 12, 12, 37.05555555555556d),
        RawTempRec(1, 2, 12, 21, 37.05555555555556d),
        ))
    }
   it("reads stations") {
      val d = readStations("src/test/resources/stations.csv")
      d.collect() should contain only (
        RawStationRec(10010,0,+69.293,+016.144),
        RawStationRec(10110,0,0,+031.500),
        RawStationRec(10090,0,+80.650,+025.000),
        RawStationRec(0,10110,0,+031.500),
        RawStationRec(1,2,+69.293,+014.144)
      )
    }
    it("locates temperatures") {
      val d = locTemps(2048, readStations("src/test/resources/stations.csv"), readTemps("src/test/resources/temps.csv"))
      d.collect() should contain only  (
        (LocalDate.of(2048,12,21),Location(69.293,14.144),37.05555555555556),
        (LocalDate.of(2048,1,1),Location(69.293,14.144),-4.888888888888889),
        (LocalDate.of(2048,1,1), Location(69.293,16.144),-4.888888888888889)
      )
    }

    it("averages temperatures") {
      val d = avgTemps(locTemps(2048, readStations("src/test/resources/stations.csv"), readTemps("src/test/resources/temps.csv")))
      d.collect() should contain only (
        (Location(69.293, 16.144),-4.888888888888889),
        (Location(69.293, 14.144),16.083333333333336)
      )
    }
  }

}
