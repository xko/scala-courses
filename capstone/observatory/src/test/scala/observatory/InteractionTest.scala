package observatory

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks



class InteractionTest extends AnyFunSpec with Matchers with ScalaCheckPropertyChecks with TestUtil {
  describe("tiles"){
    they("convert to lat/lon"){
      val loc = Tile(1072, 661, 11).loc
      loc.lat shouldBe 53.644 +- 0.001
      loc.lon shouldBe 8.437 +-0.001
    }

    they("divide correctly"){
      Tile(1072, 661, 11).subTile(0,0,1)  shouldBe Tile(2144,1322,12)
      Tile(1072, 661, 11).subTile(6,3,2)  shouldBe Tile(4294,2647,13)
    }
  }

  describe("image generation"){
    import com.sksamuel.scrimage.nio.ImageWriter
    import java.nio.file.{Paths,Files}
    import Extraction.{locateTemperatures, locationYearlyAverageRecords}
    import Interaction.tile

    def img(year: Year, refs: Iterable[(Location, Temperature)])(t: Tile) = {
      val image = tile(refs, Visualization.Colors, t)
      val path = Paths.get(s"target/temperatures/$year/${t.zoom}/${t.x}-${t.y}.png")
      Files.createDirectories(path.getParent)
      image.output(path)(ImageWriter.default)
    }

    ignore("generates 2015 zoom 0"){
      val ye = 2015
      val temps = locateTemperatures(ye, "src/main/resources/stations.csv", s"src/main/resources/$ye.csv")
      val refs: Iterable[(Location, Temperature)] = locationYearlyAverageRecords(temps)
      img(ye,refs)(Tile(0,0,0))
    }

    ignore("generates 2015 zoom 1-2"){
      val ye = 2015
      val temps = locateTemperatures(ye, "src/main/resources/stations.csv", s"src/main/resources/$ye.csv")
      val refs: Iterable[(Location, Temperature)] = locationYearlyAverageRecords(temps)
      Tile(0,0,0).zoomIn(1).foreach(img(ye,refs))
      Tile(0,0,0).zoomIn(2).foreach(img(ye,refs))
    }

    ignore("generates 2015 zoom 3"){
      val ye = 2015
      val temps = locateTemperatures(ye, "src/main/resources/stations.csv", s"src/main/resources/$ye.csv")
      val refs: Iterable[(Location, Temperature)] = locationYearlyAverageRecords(temps)
      Tile(0,0,0).zoomIn(3).foreach(img(ye,refs))
    }


  }


}
