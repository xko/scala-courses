package recfun

import scala.annotation.tailrec

object RecFun extends RecFunInterface {

  def main(args: Array[String]): Unit = {
    println("Pascal's Triangle")
    for (row <- 0 to 10) {
      for (col <- 0 to row)
        print(s"${pascal(col, row)} ")
      println()
    }
  }

  /**
   * Exercise 1
   */
  def pascal(c: Int, r: Int): Int = {
    if(c > r) throw new IllegalArgumentException(s"No column $c in row $r")
    if(r == 0 || c == 0 || c == r) 1 else pascal(c-1, r-1) + pascal(c, r-1)
  }

  /**
   * Exercise 2
   */
  def balance(chars: List[Char]): Boolean = {
    @tailrec
    def bal(b: Int, chars: List[Char]):Int = {
      if(b>=0) {
        chars match {
          case '(' :: tail => bal(b + 1, tail)
          case ')' :: tail => bal(b - 1, tail)
          case _ :: tail => bal(b, tail)
          case Nil => b
        }
      } else b
    }
    bal(0, chars) == 0
  }

  /**
   * Exercise 3
   */
  def countChange(money: Int, coins: List[Int]): Int = {
    def cc(money: Int, coinsDec: List[Int]): Int = {
      coinsDec match {
        case Nil => 0
        case smallest :: Nil => if (money % smallest == 0) 1 else 0
        case biggest :: rest if biggest > money => cc(money, rest)
        case biggest :: rest =>
          cc(money - biggest, coinsDec) + cc(money, rest)
      }
    }
    cc(money, coins.sorted(Ordering[Int].reverse))
  }
}
