package observatory

import com.sksamuel.scrimage.Image
import com.sksamuel.scrimage.nio.ImageWriter
import observatory.Extraction.{locateTemperatures, locationYearlyAverageRecords}

import java.nio.file.{Files, Paths}
import scala.io.Source

object Main extends App {

//  writeTemperatures(args(0).toInt)
//  writeNorms
   writeDeviations(args(0).toInt)

  def writeDeviations(ye: Year): Unit = {
    println(s"Visualizing deviations $ye")
    val norms = Source.fromFile("src/main/resources/norms.txt")
    try {
      val normals = norms.getLines().map(_.toDouble).toArray
      val dev = Manipulation.deviation(temperatures(ye), gl=> normals(gl.i))
      def write = writeTile( t=> s"target/deviations/$ye/${t.zoom}/${t.x}-${t.y}.png",
                             t=> Visualization2.visualizeGrid(dev, Colors.deviations, t)) _
      write(Tile.World)
      Tile.World.zoomIn(1).foreach(write)
      Tile.World.zoomIn(2).foreach(write)
      Tile.World.zoomIn(3).foreach(write)
    } finally norms.close()
  }

  def writeNorms(): Unit = {
    println(s"Computing norms")
    val path = Paths.get(s"src/main/resources/norms.txt")
    Files.createDirectories(path.getParent)
    val norms  = Manipulation.average((1975 to 1990).toStream.map(temperatures))
    import collection.JavaConverters._
    Files.write(path, Manipulation.gridLocations.map(norms(_).toString).seq.asJava )
  }

  def writeTemperatures(ye: Year): Unit = {
    val refs: Iterable[(Location, Temperature)] = temperatures(ye)
    val write = writeTile( t=> s"target/temperatures/$ye/${t.zoom}/${t.x}-${t.y}.png",
                           t=> Interaction.tile(refs, Colors.temperatures, t)) _
    write(Tile.World)
    Tile.World.zoomIn(1).foreach(write)
    Tile.World.zoomIn(2).foreach(write)
    Tile.World.zoomIn(3).foreach(write)
  }

  private def temperatures(ye: Year) = locationYearlyAverageRecords(
    locateTemperatures(ye, "src/main/resources/stations.csv", s"src/main/resources/$ye.csv")
  )

  def writeTile(toPath: Tile => String, render: Tile => Image )(t:Tile) = {
    println(s"Rendering ${toPath(t)}")
    val image = render(t)
    val path = Paths.get(toPath(t))
    Files.createDirectories(path.getParent)
    image.output(path)(ImageWriter.default)
  }

}
