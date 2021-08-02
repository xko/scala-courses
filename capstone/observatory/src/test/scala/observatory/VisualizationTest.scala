package observatory

import com.sksamuel.scrimage.RGBColor
import com.sksamuel.scrimage.nio.ImageWriter
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.time.LocalDate

class VisualizationTest extends AnyFunSpec with Matchers {
  import Visualization._
  import Extraction._

  describe("graded methods") {

    describe("temperature interpolation") {
      it("works on some points") {
        predictTemperature( locationYearlyAverageRecords( Array(
            (LocalDate.of(2048, 12, 21), Location(69.293, 14.144), 37.05555555555556),
            (LocalDate.of(2048, 1, 1), Location(69.293, 14.144), -4.888888888888889),
            (LocalDate.of(2048, 1, 1), Location(69.293, 16.144), -4.888888888888889)
          ) ), Location(69.293, 15.144) ) should be(5.597222222222224)
      }
      it("works on 1 ref point") {
        predictTemperature(Array((Location(0.0, 0.0), 10.0)), Location(90.0, -180.0)) should be(10d)
      }
      describe("with 2 ref points"){
        it("works on the edge") {
          predictTemperature(List((Location(45.0, -90.0), 10.0), (Location(-45.0, 0.0), 20.0)),
                             Location(45.0, -90.0)) should be(10d)
        }
        it("works between") {
          predictTemperature( List((Location(45.0,-90.0),10.0), (Location(-45.0,0.0),20.0)),Location(0,-45.0) ) should be ( 15d )
        }
      }
    }

    describe("color interpolation") {
      it("works on some points") {
        interpolateColor(List((1.0, Color(255, 0, 0)), (2.0, Color(0, 0, 255)),
                              (3.0, Color(0, 255, 0))), 1.5 ) should be ( Color(128, 0, 128) )
      }
    }

    describe("visualization"){
      it("writes image file") {
        val image = visualize(List((Location(-170, 80), 33), (Location(170, 80), 33),
                                   (Location(-170, -80), 33), (Location(170, -80), 33),
                                   (Location(0, 0), -15)), Colors)
        val f = new File("target/simple.png")
        f.delete()
        image.output(f)(ImageWriter.default)
        f should exist
      }

      describe("with 2 ref. points"){
        it("works between") {
          val image = visualize( List((Location(45.0, -90.0), 0.0), (Location(-45.0, 0.0), 6.632951209392111)),
                                 List((0.0, Color(254, 0, 0)), (6.632951209392111, Color(0, 0, 254))) )
          image.pixel(-45 + 180, 90).toColor should be (RGBColor(127, 0, 127, 255))
        }
        it("works on the edge") {
          val image = visualize( List((Location(45.0, -90.0), 0.0), (Location(-45.0, 0.0), 6.632951209392111)),
                                 List((0.0, Color(255, 0, 0)), (6.632951209392111, Color(0, 0, 255))) )
          image.pixel(-90 + 180, 90 - 45).toColor should be (RGBColor(255, 0, 0, 255))
        }
      }
    }
  }

  describe("spark implementation") {
    import Visualization.SparkImpl._
    import Extraction.SparkImpl._
    ignore("generates full map"){
      val refs = avgTemps( locTemps(1987, readStations("src/main/resources/stations.csv"),
                                          readTemps("src/main/resources/1987.csv")) )
      val image = visualize(refs, Colors)
      image.output("target/full.png")(ImageWriter.default)

    }

  }


}
