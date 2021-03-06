package observatory

import com.sksamuel.scrimage.Pixel
import com.sksamuel.scrimage.nio.ImageWriter
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import Visualization._
import observatory.Extraction.{locateTemperatures, locationYearlyAverageRecords}

import java.io.File

class VisualizationTest extends AnyFunSpec with Matchers with ScalaCheckPropertyChecks with TestUtil {

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

    it("produces no negatives"){ forAll{ (a:Location, b:Location) =>  dSigma(a,b) should be >= 0.0 } }

    it("is commutative"){ forAll{ (a:Location, b:Location) => dSigma(a,b) shouldEqual dSigma(b,a) } }
  }


  describe("temperature interpolation") {
    it("works on some refs") {
      predictTemperature(List( Location(52.39203305441807, 13.090866701525755) -> 75,
                               Location(52.756102500413114, 13.750213841183095) -> 101,
                               Location(52.23623375478595, 13.955751077705838) -> 16),
                          Location(52.4586444832789, 13.57802892835192)) shouldBe (64.0 +- 2)
    }
    it("works on 1 ref") {
      predictTemperature(Array( Location(0.0, 0.0) -> 10.0), Location(90.0, -180.0) ) shouldBe 10.0
    }
    describe("with 2 refs") {
      it("works on the edge") {
        predictTemperature(List( Location(45.0, -90.0) -> 10.0, Location(-45.0, 0.0) -> 20.0 ),
                           Location(45.0, -90.0)) shouldBe 10.0
      }
      it("works between") {
        predictTemperature(List(Location(45.0, -90.0) -> 10.0, Location(-45.0, 0.0) -> 20.0),
                           Location(0, -45.0)) shouldBe 15.0
      }

      describe("between arbitrary refs") {
        it("predicts temp closer to the closer ref") {
          import org.scalacheck.Shrink.shrinkAny
          forAll { (near: Location, tNear: Temperature, far: Location, tFar: Temperature, x: Location) =>
            whenever(dSigma(near, x) < dSigma(far, x)) {
              predictTemperature(List(near -> tNear, far -> tFar), x) should beCloserTo(tNear).thenTo(tFar)
            }
          }
        }
      }
    }
  }

    describe("color interpolation") {
      it("works on 3 particular refs") {
        val color = interpolateColor( List(1.0->Color(255, 0, 0), 2.0->Color(0, 0, 255), 3.0->Color(0, 255, 0)), 1.5 )
        color shouldBe Color(128, 0, 128)
      }
      it("works on 2 far apart refs"){
        val c = interpolateColor(List(1.5937842330603102E210 -> Color(255, 0, 0), -0.260475835897054 -> Color(0, 255, 0)),
                                      1.5776989721396933E210)
        c.red should beCloserTo(255).thenTo(0)
        c.green should beCloserTo(0).thenTo(255)
      }
      it("works on 2 arbitrary refs") {
        import org.scalacheck.Shrink.shrinkAny
        forAll { (tNear: Temperature, tFar: Temperature, t: Temperature) =>
          whenever((tNear - t).abs < (tFar - t).abs) {
            val c = interpolateColor(List(tNear->Color(255, 0, 0), tFar->Color(0, 255, 0)),t)
            c.red should beCloserTo(255).thenTo(0)
            c.green should beCloserTo(0).thenTo(255)
          }
        }
      }
    }

    describe("visualization"){
      it("writes image file") {
        val image = visualize( List( Location(-170, 80)->33, Location(170, 80)->33,
                                     Location(-170, -80)->33, Location(170, -80)->33,
                                     Location(0, 0)-> -15 ), Colors.temperatures )
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
      ignore("generates full map"){
        val ye = 2015
        val temps = locateTemperatures(ye, "src/main/resources/stations.csv", s"src/main/resources/$ye.csv")
        val refs: Iterable[(Location, Temperature)] = locationYearlyAverageRecords(temps)
        val image = visualize(refs, Colors.temperatures)
        image.output("target/full.png")(ImageWriter.default)

      }
    }

}
