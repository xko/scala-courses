package observatory

import com.sksamuel.scrimage.{Image, Pixel}
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.matchers.{MatchResult, Matcher}

import scala.math.Ordering.Implicits._
import scala.math.Numeric.Implicits._
import scala.reflect.ClassTag

trait TestUtil {
  val locations: Gen[Location] = for {
    lat <- Gen.choose(-90.0, 90.0)
    lon <- Gen.choose(-180.0, 180.0)
  } yield Location(lat, lon)

  implicit val arbLocation: Arbitrary[Location] = Arbitrary(locations)

  case class beCloserTo[T : Numeric : ClassTag](near: T){
    def thenTo(far: T): Matcher[T] = Matcher[T] { x =>
        MatchResult( (x - near).abs <= (x - far).abs,
                     s"$x was closer to $far then to $near", s"$x was closer to $near then to $far" )
      }
  }

}
