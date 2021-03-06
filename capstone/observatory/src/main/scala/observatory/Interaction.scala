package observatory

import com.sksamuel.scrimage.Image
import observatory.Visualization.{pPredictTemperature, render}

/**
  * 3rd milestone: interactive visualization
  */
object Interaction extends InteractionInterface {

  /**
    * @param tile Tile coordinates
    * @return The latitude and longitude of the top-left corner of the tile, as per http://wiki.openstreetmap.org/wiki/Slippy_map_tilenames
    */
  def tileLocation(tile: Tile): Location = tile.loc

  /**
    * @param temperatures Known temperatures
    * @param colors Color scale
    * @param tile Tile coordinates
    * @return A 256×256 image showing the contents of the given tile
    */
  def tile(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)], tile: Tile): Image =
    render(tile.pixLocs, colors, 256, 127)(pPredictTemperature(temperatures.par))

  /**
    * Generates all the tiles for zoom levels 0 to 3 (included), for all the given years.
    * @param yearlyData Sequence of (year, data), where `data` is some data associated with
    *                   `year`. The type of `data` can be anything.
    * @param generateImage Function that generates an image given a year, a zoom level, the x and
    *                      y coordinates of the tile and the data to build the image from
    */
  def generateTiles[Data](
    yearlyData: Iterable[(Year, Data)],
    generateImage: (Year, Tile, Data) => Unit
  ): Unit = {
    yearlyData.foreach{ case (y,d) =>
      val t = Tile(0,0,0)
      generateImage(y,t,d)
      t.zoomIn(1).foreach(generateImage(y, _, d))
      t.zoomIn(2).foreach(generateImage(y, _, d))
      t.zoomIn(3).foreach(generateImage(y, _, d))
    }
  }

}
