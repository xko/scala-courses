package kvstore

import akka.actor.*
import akka.event.LoggingReceive

import concurrent.duration.given

object TimedCounter {
  case object AckNow
  case object Timeout

  def props(count: Int, requester: ActorRef, success: Any, failure: Any): Props =
    Props(new TimedCounter(count, requester, success, failure))
}

import scala.concurrent.duration.*
class TimedCounter(count: Int, val requester: ActorRef, val success: Any, val failure: Any) extends Actor with ActorLogging {
  import TimedCounter.*
  import context.dispatcher

  val timeout: Cancellable = context.system.scheduler.scheduleOnce(1.second, self, Timeout)
  override def postStop(): Unit = timeout.cancel()

  override def receive: Receive = receive(count-1)

  def receive(count:Int ): Receive= {
    case Timeout =>
      requester.tell(failure,context.parent)
      context.stop(self)
    case _ if count == 0 =>
      requester.tell(success,context.parent)
      context.stop(self)
    case m =>
      context.become(receive(count-1))
  }
}
