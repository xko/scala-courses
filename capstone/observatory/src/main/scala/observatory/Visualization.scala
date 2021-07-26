package observatory

import com.sksamuel.scrimage.{Image, Pixel}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{Column, Dataset, Row}
import org.apache.spark.sql.functions._

import scala.annotation.tailrec


/**
  * 2nd milestone: basic visualization
  */
object Visualization extends VisualizationInterface {
  import Extraction.spark
  import spark.implicits._

  /**
    * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
    * @param location Location where to predict the temperature
    * @return The predicted temperature at `location`
    */
  def predictTemperature(temperatures: Iterable[(Location, Temperature)], location: Location): Temperature = {
    @tailrec def sums(temps: Stream[(Location, Temperature)], num: Double, den: Double):(Double,Double) = temps match {
      case Stream.Empty => (num,den)
      case (refLoc,refTemp) #:: tail =>
        val gcd = dSigma(refLoc.lat, refLoc.lon, location.lat, location.lon) * BigR
        if(gcd < 1) (refTemp,1)
        else {
          val w = 1 / math.pow(gcd, 2)
          sums(tail, num + w*refTemp, den + w)
        }
    }
    val (num,den) = sums(temperatures.toStream,0 ,0)
    num/den
  }

  /**
    * @param points Pairs containing a value and its associated color
    * @param value The value to interpolate
    * @return The color that corresponds to `value`, according to the color scale defined by `points`
    */
  def interpolateColor(points: Iterable[(Temperature, Color)], value: Temperature): Color = {
    ???
  }

  /**
    * @param temperatures Known temperatures
    * @param colors Color scale
    * @return A 360×180 image where each pixel shows the predicted temperature at its location
    */
  def visualize(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): Image = {
    ???
  }

  def sparkPredictTemperatures(refs: Dataset[(Location, Temperature)], targets: Dataset[Location]): Dataset[(Location, Temperature)] =
    targets.select(struct($"lat", $"lon").as("loc"))
      .crossJoin(refs).groupBy($"loc")
      .agg( tempIDW($"loc", $"_1", $"_2") ).as[(Location,Temperature)]

  def tempIDW(targetLoc: Column, refLoc: Column, temp: Column): Column = {
    val w = lit(1) / pow(gcd(targetLoc, refLoc), lit(2))
    val exact = first(when(gcd(refLoc, targetLoc) < 1, temp))
    when(isnull(exact), sum(w * temp) / sum(w)).otherwise(exact)
  }

  val gcd: UserDefinedFunction = udf { (aLoc: Row, bLoc: Row) =>
    dSigma(aLoc.getDouble(0),aLoc.getDouble(1),bLoc.getDouble(0),bLoc.getDouble(1)) * BigR
  }

  val BigR = 6731

  def dSigma(aLat: Double, aLon: Double, bLat: Double, bLon: Double): Double = {
    import math._
    if (aLat == bLat && aLon == bLon) 0d
    else if (abs(aLon - bLon) == 180 && aLat + bLat == 0) Pi
         else acos( sin(toRadians(aLat)) * sin(toRadians(bLat)) +
                    cos(toRadians(aLat)) * cos(toRadians(bLat)) * cos(toRadians(aLon) - toRadians(bLon)) )
  }

  val ImgW = 360
  val ImgH = 180

  def pxLocation(pxNo: Int ): Location = {
    val x = pxNo % ImgW
    val y = pxNo / ImgW
    Location(90-y,x-180)
  }

}

