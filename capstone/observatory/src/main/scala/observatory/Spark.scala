package observatory

import org.apache.spark.LocalDateUDT
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql._
import org.apache.spark.sql.functions._


import scala.reflect.runtime.universe.TypeTag
import java.time.LocalDate

object Spark {
  import org.apache.log4j.{Level, Logger}
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  val spark: SparkSession = SparkSession.builder().appName("Observatory")
    .config("spark.ui.enabled",false)
    .config("spark.memory.storageFraction",0.1)
    .master("local[*]").getOrCreate()
  import spark.implicits._

  LocalDateUDT.register()
  implicit def encoder: Encoder[LocalDate] = ExpressionEncoder()

  implicit class AsNamedProductDS(ds:Dataset[_]){
    def asProduct[T <: Product : TypeTag]: Dataset[T] =
      Encoders.product[T].schema.fields.zipWithIndex.foldLeft(ds.toDF){ (ds,fld_i) =>
        val (fld,i) = fld_i
         ds.withColumnRenamed(ds.columns(i),fld.name)
      }.as[T]
  }

  def asProduct[T <: Product : TypeTag](cols: Column*): TypedColumn[_, T] = {
    val cs = Encoders.product[T].schema.fields.zip(cols).map { case (fld, col) => col.as(fld.name) }
    struct(cs:_*).as[T]
  }



}
