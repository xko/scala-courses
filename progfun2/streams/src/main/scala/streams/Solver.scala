package streams

/**
 * This component implements the solver for the Bloxorz game
 */
trait Solver extends GameDef {

  /**
   * Returns `true` if the block `b` is at the final position
   */
  def done(b: Block): Boolean = b.isStanding && b.b1 == goal

  case class Path(toBlock: Block, history: List[Move])

  def evolve(p:Path, except: Set[Block]): LazyList[Path] =
    for ((b,m) <- p.toBlock.legalNeighbors.to(LazyList) if !except.contains(b)) yield Path(b, m::p.history)

  /**
   * The function `from` returns the lazy list of all possible paths
   * that can be followed, starting at the `head` of the `initial`
   * lazy list.
   *
   * The blocks in the lazy list `initial` are sorted by ascending path
   * length: the block positions with the shortest paths (length of
   * move list) are at the head of the lazy list.
   *
   * The parameter `explored` is a set of block positions that have
   * been visited before, on the path to any of the blocks in the
   * lazy list `initial`. When search reaches a block that has already
   * been explored before, that position should not be included a
   * second time to avoid cycles.
   *
   * The resulting lazy list should be sorted by ascending path length,
   * i.e. the block positions that can be reached with the fewest
   * amount of moves should appear first in the lazy list.
   *
   * Note: the solution should not look at or compare the lengths
   * of different paths - the implementation should naturally
   * construct the correctly sorted lazy list.
   */
  def from(initial: LazyList[Path], explored: Set[Block]): LazyList[Path] =
    if (initial.isEmpty) LazyList.empty
    else {
      val more = initial.flatMap(evolve(_, explored))
      initial #::: from(more, explored ++ more.map(_.toBlock))
    }

  /**
   * The lazy list of all paths that begin at the starting block.
   */
  lazy val pathsFromStart: LazyList[Path] = from(LazyList(Path(startBlock,Nil)),Set.empty)

  /**
   * Returns a lazy list of all possible pairs of the goal block along
   * with the history how it was reached.
   */
  lazy val pathsToGoal: LazyList[Path] = pathsFromStart.filter(p => done(p.toBlock))

  /**
   * The (or one of the) shortest sequence(s) of moves to reach the
   * goal. If the goal cannot be reached, the empty list is returned.
   *
   * Note: the `head` element of the returned list should represent
   * the first move that the player should perform from the starting
   * position.
   */
  lazy val solution: List[Move] = pathsToGoal.headOption.toList.flatMap(_.history.reverse)
}
