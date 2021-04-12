package quickcheck

import org.scalacheck._
import Arbitrary._
import Gen._
import Prop._

abstract class QuickCheckHeap extends Properties("Heap") with IntHeap {

  lazy val nonEmpty: Gen[H] = for {
    i <- arbitrary[Int]
    h <- genHeap
  } yield insert(i, h)

  lazy val genHeap: Gen[H] = oneOf(const(empty), nonEmpty)

  implicit lazy val arbHeap: Arbitrary[H] = Arbitrary(genHeap)

  property("gen1") = forAll { (h: H) =>
    val m = if (isEmpty(h)) 0 else findMin(h)
    findMin(insert(m, h)) == m
  }

  property("inOut") = forAll {(x:Int) =>
    isEmpty(deleteMin(insert(x,empty)))
  }

  property("minOf2") = forAll { (a:Int, b:Int) =>
    findMin( insert(a,insert(b,empty)) ) == math.min(a,b)
  }

  def toList(h:H):List[Int] = if(isEmpty(h)) Nil else findMin(h) :: toList(deleteMin(h))

  property("sorted") = forAll{h:H =>
    toList(h) == toList(h).sorted
  }

  property("meldMin") = forAll(nonEmpty, nonEmpty){ (a:H, b:H) =>
    val ab = meld(a,b)
    findMin(ab) == findMin(a) || findMin(ab) == findMin(b)
  }

  property("meldFull") = forAll{(a:H, b:H)=>
    toList(meld(a,b)) == (toList(a) ::: toList(b)).sorted
  }

  property("meldCommutative") = forAll { (a: H, b: H) =>
    toList(meld(a,b)) == toList(meld(b,a))
  }



}
