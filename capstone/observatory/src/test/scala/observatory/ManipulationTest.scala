package observatory

import observatory.Manipulation._
import org.scalacheck.Gen._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class ManipulationTest extends AnyFunSpec with Matchers with ScalaCheckPropertyChecks with TestUtil  {

  describe("temperatures grid"){
    val temps = chooseNum(-100.0, 100.0)
    val refs = listOfN(5, zip(locations, temps))
    val glocs = zip(chooseNum(-89,90),chooseNum(-180,179)).map(GridLocation.tupled)

    it("equals at exact location"){
      forAll(refs, glocs ,temps){ (refs, testGLoc, testTemp) =>
        val g = makeGrid(testGLoc.loc -> testTemp :: refs)
        g(testGLoc) shouldBe testTemp
      }
    }

    it("averages at exact location"){
      forAll( listOfN(20,zip(refs,temps)), glocs ){ (refss,testGLoc) =>
        val g = average( refss.map{ case (refs,testTemp)=> testGLoc.loc -> testTemp :: refs } )
        val expectedAvg = refss.map(_._2).sum / refss.size
        g(testGLoc) shouldBe expectedAvg
      }
    }
  }

}
