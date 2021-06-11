package reductions

import scala.annotation._
import org.scalameter._

import scala.collection.immutable.ArraySeq

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime")
    println(s"speedup: ${seqtime.value / fjtime.value}")
  }
}

object ParallelParenthesesBalancing extends ParallelParenthesesBalancingInterface {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {
    @tailrec
    def bal(b: Int, chars: Seq[Char]):Int = if (b<0 || chars.isEmpty) b else
        chars.head match {
          case '(' => bal(b + 1, chars.tail)
          case ')' => bal(b - 1, chars.tail)
          case _  => bal(b, chars.tail)
        }

    bal(0, ArraySeq.unsafeWrapArray(chars)) == 0
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    @tailrec
    def traverse(idx: Int, until: Int, unclosed: Int, unopened: Int) : (Int, Int) = {
      if (idx == until) (unclosed, unopened) else chars(idx) match {
        case '(' => traverse(idx + 1, until, unclosed + 1, unopened)
        case ')' if unclosed > 0 => traverse(idx + 1, until, unclosed - 1, unopened)
        case ')' => traverse(idx + 1, until, unclosed, unopened + 1)
        case _ => traverse(idx + 1, until, unclosed, unopened)
      }
    }

    def reduce(from: Int, until: Int) : (Int, Int) = {
      if(until - from <= threshold) traverse(from, until, 0, 0)
      else {
        val mid = from + (until - from) / 2
        val ((ucl, uol),(ucr, uor)) = parallel(reduce(from, mid), reduce(mid, until))
        val closed = ucl.min(uor)
        (ucl - closed + ucr,uol + uor - closed)
      }
    }

    reduce(0, chars.length) == (0,0)
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
