package observatory

import com.sksamuel.scrimage.{Image, Pixel}
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.matchers.{MatchResult, Matcher}

import scala.math.Ordering.Implicits._
import scala.math.Numeric.Implicits._
import scala.reflect.ClassTag

trait TestUtil {
  implicit val arbLocation: Arbitrary[Location] = Arbitrary( for {
    lat <- Gen.choose(-90.0, 90.0)
    lon <- Gen.choose(-180.0, 180.0)
  } yield Location(lat, lon) )

  case class beCloserTo[T : Numeric : ClassTag](near: T){
    def thenTo(far: T): Matcher[T] = Matcher[T] { x =>
        MatchResult( (x - near).abs <= (x - far).abs,
                     s"$x was closer to $far then to $near", s"$x was closer to $near then to $far" )
      }
  }

  implicit class LocAwareImage(img: Image) {
    def pixel(location: Location): Pixel = img.pixel(location.lon.round.toInt + 180, 90 - location.lat.round.toInt)
  }
}
