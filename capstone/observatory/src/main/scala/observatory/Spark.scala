package observatory

import org.apache.spark.LocalDateUDT
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import scala.reflect.runtime.universe.TypeTag

import java.time.LocalDate

object Spark {
  import org.apache.log4j.{Level, Logger}
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  val spark: SparkSession = SparkSession.builder().appName("Observatory").master("local[*]").getOrCreate()
  import spark.implicits._

  LocalDateUDT.register()
  implicit def encoder: Encoder[LocalDate] = ExpressionEncoder()


}
