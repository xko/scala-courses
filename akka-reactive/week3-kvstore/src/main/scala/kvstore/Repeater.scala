package kvstore

import akka.actor.*
import akka.event.LoggingReceive

import concurrent.duration.given

object Repeater {
  case object AckNow

  def apply(to: ActorRef, req: Any, ackTo: ActorRef, ack: Any): Props = Props(classOf[Repeater], to, req, ackTo, ack)
}

class Repeater(val to:ActorRef, val req: Any, val ackTo: ActorRef, val ack: Any) extends Actor with ActorLogging {

  import context.dispatcher
  import Repeater.*

  val repeat: Cancellable =
    context.system.scheduler.scheduleWithFixedDelay(0.milli, 100.milli, to, req)


  override def postStop(): Unit =
    repeat.cancel()

  override def receive: Receive = {
    case resp =>
      ackTo.tell(ack, context.parent)
      context.parent.forward(resp)
      context.stop(self)
  }
}
