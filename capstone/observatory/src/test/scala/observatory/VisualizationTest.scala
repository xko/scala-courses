package observatory

import com.sksamuel.scrimage.Pixel
import com.sksamuel.scrimage.nio.ImageWriter
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import java.io.File
import java.time.LocalDate

class VisualizationTest extends AnyFunSpec with Matchers with ScalaCheckPropertyChecks with TestUtil {
  import Visualization._
  import Extraction._

  describe("graded methods") {
    describe("distance calculation") {
      it("measures short") {
        dSigma(52.54555032383161, 13.429522693308664, 52.54027818496617, 13.4750987873352) * BigR  shouldBe 3.1 +- .1
        dSigma(52.54027818496617, 13.4750987873352, 52.54555032383161, 13.429522693308664) * BigR  shouldBe 3.1 +- .1
        dSigma(78.24679661339356, 15.42422547530977, 78.25574486615133, 15.726963466565975) * BigR shouldBe 6.9 +- .1
        dSigma(50.943527257197935, 15, 50.916614875561656, 15) * BigR shouldBe 3.0 +- .5
        dSigma(50, 15.43780955087213,50, 15.384514971147482) * BigR shouldBe   3.6 +- .5
      }
      it("measures mid") {
        dSigma(48.00071909593895, 2.872122953377574, 47.67743435797542, 31.98477638656484) * BigR shouldBe 2166.0 +- 10
      }
      it("measures long") {
        dSigma(73.13477526915966, -38.780725997932024, -28.083865379340455, 140.41896872640262) * BigR shouldBe 14992.0 +- 20
      }

      it("produces no negatives"){
        forAll{ (a:Location, b:Location) =>  dSigma(a,b) should be >= 0.0 }
      }

      it("is commutative"){
        forAll{ (a:Location, b:Location) => dSigma(a,b) shouldEqual dSigma(b,a) }
      }
    }

    describe("temperature interpolation") {
      it("works on some refs") {
        predictTemperature( locationYearlyAverageRecords( Array(
            (LocalDate.of(2048, 12, 21), Location(69.293, 14.144), 37.05555555555556),
            (LocalDate.of(2048, 1, 1), Location(69.293, 14.144), -4.888888888888889),
            (LocalDate.of(2048, 1, 1), Location(69.293, 16.144), -4.888888888888889)
          ) ), Location(69.293, 15.144) ) shouldBe 5.597222222222224
      }
      it("works on 1 ref") {
        predictTemperature( Array( Location(0.0, 0.0)->10.0 ), Location(90.0, -180.0) ) shouldBe 10.0
      }
      describe("with 2 refs"){
        it("works on the edge") {
          predictTemperature( List( Location(45.0, -90.0)->10.0, Location(-45.0, 0.0)->20.0 ),
                              Location(45.0, -90.0) ) shouldBe 10.0
        }
        it("works between") {
          predictTemperature( List( Location(45.0, -90.0)->10.0, Location(-45.0, 0.0)->20.0 ),
                              Location(0,-45.0) ) shouldBe 15.0
        }

        describe("between arbitrary refs") {
          it("predicts temp closer to the closer ref") {
            import org.scalacheck.Shrink.shrinkAny
            forAll { (near:Location, tNear: Temperature, far:Location, tFar:Temperature, x: Location) =>
              whenever(dSigma(near, x) < dSigma(far, x)) {
                predictTemperature( List(near->tNear, far->tFar), x ) should beCloserTo(tNear).thenTo(tFar)
              }
            }
          }
        }
      }
    }

    describe("color interpolation") {
      it("works on some points") {
        val color = interpolateColor( List(1.0->Color(255, 0, 0), 2.0->Color(0, 0, 255), 3.0->Color(0, 255, 0)), 1.5 )
        color shouldBe Color(128, 0, 128)
      }
    }

    describe("visualization"){
      it("writes image file") {
        val image = visualize( List( Location(-170, 80)->33, Location(170, 80)->33,
                                     Location(-170, -80)->33, Location(170, -80)->33,
                                     Location(0, 0)-> -15 ), Colors )
        val f = new File("target/simple.png")
        f.delete()
        image.output(f)(ImageWriter.default)
        f should exist
      }

      describe("with 2 refs"){
        def px(loc: Location): (Int, Int) = (loc.lon.floor.toInt + 180, 90 - loc.lat.ceil.toInt)

        it("works between") {
          val image = visualize( List( Location(45.0, -90.0)->0.0, Location(-45.0, 0.0)->6.632951209392111 ),
                                 List(0.0->Color(255, 0, 0), 6.632951209392111->Color(0, 0, 255)) )
          image.pixel(px(Location(0,-45))) shouldBe Pixel(128, 0, 128, 255)
        }
        it("works on the edge") {
          val image = visualize( List((Location(45.0, -90.0), 0.0), (Location(-45.0, 0.0), 6.632951209392111)),
                                 List((0.0, Color(255, 0, 0)), (6.632951209392111, Color(0, 0, 255))) )
          image.pixel(px(Location(-45,0))) shouldBe Pixel(0, 0, 255, 255)
        }

        it("produces color closer to the closer ref") {
          import org.scalacheck.Shrink.shrinkAny
          forAll { (near:Location, tNear: Temperature, far:Location, tFar:Temperature, x: Location) =>
            whenever(dSigma(near, x) < dSigma(far, x)) {
              val image = visualize( List(near->tNear, far->tFar),
                                     List(tNear->Color(255, 0, 0), tFar->Color(0, 255, 0)) )
              image.pixel(px(x)).red should beCloserTo(255).thenTo(0)
              image.pixel(px(x)).green should beCloserTo(0).thenTo(255)
            }
          }
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
