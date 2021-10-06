package observatory

import com.sksamuel.scrimage.{Image, Pixel}

import scala.collection.parallel.ParIterable

/**
  * 2nd milestone: basic visualization
  */
object Visualization extends VisualizationInterface {

  val Colors = List( (-60d,Color(0,0,0)), (-50d,Color(33,0,107)), (-27d,Color(255,0,255)), (-15d,Color(0,0,255)),
                     (0d,Color(0,255,255)), (12d, Color(255,255,0)), (32d,Color(255,0,0)), (60d,Color(255,255,255)))

  val BigR = 6371.0088
  val P = 4

  def dSigma(aLat: Double, aLon: Double, bLat: Double, bLon: Double): Double = {
    import math._
    if (aLat == bLat && aLon == bLon) 0d
    else if (abs(aLon - bLon) == 180 && aLat + bLat == 0) Pi
    else acos( sin(toRadians(aLat)) * sin(toRadians(bLat)) +
               cos(toRadians(aLat)) * cos(toRadians(bLat)) * cos(toRadians(abs(aLon-bLon))) )
  }

  def dSigma(a: Location, b:Location): Double = dSigma(a.lat, a.lon, b.lat, b.lon)


  /**
    * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
    * @param location Location where to predict the temperature
    * @return The predicted temperature at `location`
    */
  def predictTemperature(temperatures: Iterable[(Location, Temperature)], location: Location): Temperature =
    pPredictTemperature(temperatures.par)(location)

  def pPredictTemperature(refs: ParIterable[(Location, Temperature)])(target: Location): Temperature = {
    val gcds = refs.map { case (loc, temp) => (dSigma(loc, target) * BigR, temp)  }
    gcds.find(_._1 < 1).map(_._2).getOrElse {
      val (num, den) = gcds.aggregate(0.0 -> 0.0)(
        (agg, ref) => (agg, ref) match {
          case ((num, den), (gcd, temp)) =>
              val w = 1 / math.pow(gcd, P)
              (num + w * temp, den + w)
        },
        (agg, bgg) => (agg, bgg) match {
          case ((n1, d1), (n2, d2)) => (n1 + n2, d1 + d2)
        })
      num / den
    }
  }


  /**
    * @param points Pairs containing a value and its associated color
    * @param value The value to interpolate
    * @return The color that corresponds to `value`, according to the color scale defined by `points`
    */
  def interpolateColor(points: Iterable[(Temperature, Color)], value: Temperature): Color = {
    import scala.collection.Searching._
    def linear(x: Double, x0: Double, y0: Double, x1: Double, y1: Double) = (y0*(x1-x) + y1*(x-x0))/(x1-x0)
    val (temps,colors) = points.toIndexedSeq.sortBy(_._1).unzip
    temps.search(value) match {
      case Found(i) => colors(i)
      case InsertionPoint(i) if i == temps.length => colors.last
      case InsertionPoint(i) if i == 0 => colors.head
      case InsertionPoint(i) =>
        val (t0,c0,t1,c1) = (temps(i-1),colors(i-1),temps(i),colors(i))
        Color( linear(value, t0, c0.red,   t1, c1.red).round.toInt,
               linear(value, t0, c0.green, t1, c1.green).round.toInt,
               linear(value, t0, c0.blue,  t1, c1.blue).round.toInt )
    }
  }

  /**
    * @param temperatures Known temperatures
    * @param colors Color scale
    * @return A 360×180 image where each pixel shows the predicted temperature at its location
    */
  def visualize(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): Image =
    render( for (y <- 0 until 180; x <- 0 until 360) yield Location(90 - y, x - 180),
            colors, 360, 255 )(pPredictTemperature(temperatures.par))


  def render( locs: Iterable[Location], colors: Iterable[(Temperature, Color)], width: Int, alpha:Int )
            ( predict: Location => Temperature ): Image = {
    def toPx(c:Color) = Pixel(c.red,c.green,c.blue,alpha)
    val pixels = locs.par map predict map (temp => toPx(interpolateColor(colors, temp)))
    Image(width, pixels.size / width, pixels.toArray)
  }

  def visualizeMemOpt(refs: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): Image = {
    val img = Image(360, 180)
    val predict = pPredictTemperature(refs.par) _
    for (y <- 0 until 180; x <- 0 until 360) {
      val t = predict(Location(90 - y, x - 180))
      val c = interpolateColor(colors, t)
      img.setPixel(x, y, Pixel(c.red, c.green, c.blue, 255))
    }
    img
  }


}
