package observatory

import observatory.Visualization.pPredictTemperature

import scala.collection.parallel.ParIterable
import scala.collection.parallel.mutable.ParArray

/**
  * 4th milestone: value-added information
  */
object Manipulation extends ManipulationInterface {
  private lazy val grid = (for (lat <- -89 to 90; lon <- -180 to 179) yield GridLocation(lat, lon)).toParArray

  def gridLocations:Iterable[GridLocation] = grid.seq


  /**
    * @param temperatures Known temperatures
    * @return A function that, given a latitude in [-89, 90] and a longitude in [-180, 179],
    *         returns the predicted temperature at this location
    */
  def makeGrid(temperatures: Iterable[(Location, Temperature)]): GridLocation => Temperature = {
    val tempGrid = pMakeGrid(temperatures.par)
    (gloc: GridLocation) => tempGrid(gloc.i)
  }

  private def pMakeGrid(pRefs: ParIterable[(Location, Temperature)]): ParArray[Temperature] = {
    System.gc()
    grid map (gloc => pPredictTemperature(pRefs)(gloc.loc))
  }

  private def average[T : Numeric](vs: Iterable[T]) = {
    import scala.math.Numeric.Implicits._
    val zero = implicitly[Numeric[T]].zero
    val (sum,n) = vs.aggregate[(T,Int)]( zero->0 )(
      (p,v) => p match { case (sum, n) => (sum + v, n + 1) } ,
      (a,b) => (a._1 + b._1)->(a._2+b._2)
    )
    sum.toDouble / n.toDouble
  }

  /**
    * @param temperaturess Sequence of known temperatures over the years (each element of the collection
    *                      is a collection of pairs of location and temperature)
    * @return A function that, given a latitude and a longitude, returns the average temperature at this location
    */
  def average(temperaturess: Iterable[Iterable[(Location, Temperature)]]): GridLocation => Temperature = {
    val tempGrids = temperaturess map { temps => pMakeGrid(temps.par) }
    val avgGrid = grid map { gloc => average[Temperature](tempGrids.map(_(gloc.i))) }
    (gloc: GridLocation) => avgGrid(gloc.i)
  }

  /**
    * @param temperatures Known temperatures
    * @param normals A grid containing the “normal” temperatures
    * @return A grid containing the deviations compared to the normal temperatures
    */
  def deviation(temperatures: Iterable[(Location, Temperature)], normals: GridLocation => Temperature): GridLocation => Temperature = {
    val tempGrid = pMakeGrid(temperatures.par)
    val devGrid = grid.map(gloc=> tempGrid(gloc.i) - normals(gloc))
    (gloc: GridLocation) => devGrid(gloc.i)
  }


}
