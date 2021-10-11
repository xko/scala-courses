package observatory

import com.sksamuel.scrimage.nio.ImageWriter
import observatory.Extraction.{locateTemperatures, locationYearlyAverageRecords}
import observatory.Interaction.tile

import java.nio.file.{Files, Paths}

object Main extends App {

  def img(year: Year, refs: Iterable[(Location, Temperature)])(t: Tile) = {
    println(s"Generating $year: $t")
    val image = tile(refs, Visualization.Colors, t)
    val path = Paths.get(s"target/temperatures/$year/${t.zoom}/${t.x}-${t.y}.png")
    Files.createDirectories(path.getParent)
    image.output(path)(ImageWriter.default)
  }

  val ye = args(0).toInt

  val temps = locateTemperatures(ye, "src/main/resources/stations.csv", s"src/main/resources/$ye.csv")
  val refs: Iterable[(Location, Temperature)] = locationYearlyAverageRecords(temps)
  img(ye,refs)(Tile(0,0,0))
//  Tile(0,0,0).zoomIn(1).foreach(img(ye,refs))
//  Tile(0,0,0).zoomIn(2).foreach(img(ye,refs))
//  Tile(0,0,0).zoomIn(3).foreach(img(ye,refs))

}
