package observatory

import com.sksamuel.scrimage.{Image, Pixel}

/**
  * 5th milestone: value-added information visualization
  */
object Visualization2 extends Visualization2Interface {
  val Colors = List( (-7.0,Color(0,0,255)),  (-2.0,Color(0,255,255)), (0.0,Color(255,255,255)),
                     (2.0,Color(255,255,0)), (4.0,Color(255,0,0)),    (7.0, Color(0,0,0))         )


  /**
    * @param point (x, y) coordinates of a point in the grid cell
    * @param d00 Top-left value
    * @param d01 Bottom-left value
    * @param d10 Top-right value
    * @param d11 Bottom-right value
    * @return A guess of the value at (x, y) based on the four known values, using bilinear interpolation
    *         See https://en.wikipedia.org/wiki/Bilinear_interpolation#Unit_Square
    */
  def bilinearInterpolation(
    point: CellPoint,
    d00: Temperature,
    d01: Temperature,
    d10: Temperature,
    d11: Temperature
  ): Temperature = d00*(1-point.x)*(1-point.y) + d10*point.x*(1-point.y)+
                   d01*(1-point.x)*point.y     + d11*point.x*point.y

  /**
    * @param grid Grid to visualize
    * @param colors Color scale to use
    * @param tile Tile coordinates to visualize
    * @return The image of the tile at (x, y, zoom) showing the grid using the given color scale
    */
  def visualizeGrid(
    grid: GridLocation => Temperature,
    colors: Iterable[(Temperature, Color)],
    tile: Tile
  ): Image = {
    Visualization.render(tile.pixLocs,Colors,256,127) { l =>
      val g00 = GridLocation(l.lat.floor.toInt, l.lon.floor.toInt).normalize
      val g01 = GridLocation(l.lat.floor.toInt, l.lon.ceil.toInt).normalize
      val g10 = GridLocation(l.lat.ceil.toInt,  l.lon.floor.toInt).normalize
      val g11 = GridLocation(l.lat.ceil.toInt,  l.lon.ceil.toInt).normalize
      bilinearInterpolation( CellPoint(l.lat - l.lat.floor,l.lon - l.lon.floor),
                             grid(g00),grid(g01),grid(g10),grid(g11) )

    }
  }

}
