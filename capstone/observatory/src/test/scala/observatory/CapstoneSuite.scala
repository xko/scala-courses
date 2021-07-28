package observatory

import org.junit.Assert._
import org.scalacheck.Test

trait MilestoneSuite {

  def namedMilestoneTest(milestoneName: String, level: Int)(block: => Unit): Unit =
    if (Grading.milestone >= level) {
      block
    } else {
      fail(s"Milestone $level ($milestoneName) is disabled. To enable it, set the 'Grading.milestone' value to '$level'.")
    }

}

