package observatory

import org.apache.spark.LocalDateUDT
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

import java.io.FileNotFoundException
import java.nio.file.{Files, Paths}
import scala.reflect.runtime.universe.TypeTag
import java.time.LocalDate
import scala.io.Source

object Spark {
  import org.apache.log4j.{Level, Logger}
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  val spark: SparkSession = SparkSession.builder().appName("Observatory")
    .config("spark.ui.enabled",false)
    .config("spark.sql.shuffle.partitions",50)
    .master("local[*]").getOrCreate()
  import spark.implicits._

  LocalDateUDT.register()
  implicit def encoder: Encoder[LocalDate] = ExpressionEncoder()
  val asDate: UserDefinedFunction = udf((year:Int, month: Int, day: Int) => LocalDate.of(year, month, day))


  implicit class DSofNamedProducts(ds:Dataset[_]){
    def asNamed[T <: Product : TypeTag]: Dataset[T] =
      Encoders.product[T].schema.fields.zipWithIndex.foldLeft(ds.toDF) { case (ds, (fld, i)) =>
        ds.withColumnRenamed(ds.columns(i), fld.name)
      }.as[T]
  }

  def asNamed[T <: Product : TypeTag](cols: Column*): TypedColumn[_, T] = {
    val cs = Encoders.product[T].schema.fields.zip(cols).map { case (fld, col) => col.as(fld.name) }
    struct(cs:_*).as[T]
  }

  def csv[T <: Product : TypeTag](lines: Dataset[String])(sanitize: DataFrame => DataFrame): Dataset[T] = {
    val s = Encoders.product[T].schema
    sanitize( spark.read.schema( s.copy(s.fields.map(_.copy(nullable = true))) ).csv(lines) ).asNamed[T]
  }

  def text(fileOrResource:String): Dataset[String] = {
    if( Files.isReadable(Paths.get(fileOrResource)) ) spark.read.text(fileOrResource).as[String]
    else {
      val stream = Source.getClass.getResourceAsStream(fileOrResource)
      if (stream == null) throw new FileNotFoundException(fileOrResource)
      else
        try spark.sparkContext.makeRDD(Source.fromInputStream(stream).getLines().toList).toDS
        finally  stream.close()
    }
  }


}
