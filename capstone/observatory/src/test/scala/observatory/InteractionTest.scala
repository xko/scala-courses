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
    import observatory.Extraction.SparkImpl._
    import observatory.Visualization.SparkImpl._

    import org.apache.spark.sql.Dataset
    import Spark.spark.implicits._
    import java.nio.file.{Paths,Files}


    def img(year: Year, refs: Dataset[(Location, Temperature)])(tile: Tile) = {
      val image = render(interpolate(refs, tile.pixLocs.toDS()), Visualization.Colors, 256, 127)
      val path = Paths.get(s"target/temperatures/$year/${tile.zoom}/${tile.x}-${tile.y}.png")
      Files.createDirectories(path.getParent)
      image.output(path)(ImageWriter.default)
    }

    ignore("generates 2015 zoom 0"){
      val stations = readStations("src/main/resources/stations.csv")
      stations.persist()
      val year = 2015
      val refs = avgTemps(locTemps(year, stations, readTemps("src/main/resources/2015.csv")))
      refs.persist()
      img(year,refs)(Tile(0,0,0))
    }


  }


}
