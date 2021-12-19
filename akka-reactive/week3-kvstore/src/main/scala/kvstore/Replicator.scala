package kvstore

import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable, Props, actorRef2Scala}

import scala.concurrent.duration.*

object Replicator:
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  case object Release

  def props(replica: ActorRef): Props = Props(Replicator(replica))

class Replicator(val replica: ActorRef) extends Actor:
  import Replicator.*

  def receive(curSeq: Long): Receive = {
    case Release =>
      context.children.foreach(_ ! Repeater.AckNow)
      context.stop(self)
    case Replicate(k, v, id) =>
      context.actorOf(Repeater(replica, Snapshot(k, v, curSeq), sender, Replicated(k, id)))
      context.become(receive(curSeq + 1))
  }

  val receive: Receive = receive(0)
