package kvstore

import akka.actor.{ OneForOneStrategy, PoisonPill, Props, SupervisorStrategy, Terminated, ActorRef, Actor, actorRef2Scala }
import kvstore.Arbiter.*
import akka.pattern.{ ask, pipe }
import scala.concurrent.duration.*
import akka.util.Timeout

object Replica:
  sealed trait Operation:
    def key: String
    def id: Long
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(Replica(arbiter, persistenceProps))

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor:
  import Replica.*
  import Replicator.*
  import Persistence.*
  import context.dispatcher

  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  arbiter ! Join

  def receive =
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica(0))

  val persistence: ActorRef = context.system.actorOf(persistenceProps)

  def upd(k: String, v: Option[String]) = v match {
    case Some(v) => kv = kv + (k -> v)
    case None => kv = kv - k
  }

  def leaderUpd(k: String, v: Option[String], id: Long) = {
    upd(k,v)
    val timer = context.actorOf(TimedCounter.props( replicators.size+1, sender,
                                                    OperationAck(id), OperationFailed(id) ))
    context.actorOf(Repeater( persistence, Persist(k,v,id),
                              timer, TimedCounter.AckNow  ))
    replicators.foreach( _.tell(Replicate(k,v,id),timer) )
  }

  val leader: Receive = {
    case Get(k, id) => sender ! GetResult(k, kv.get(k), id)
    case Insert(k, v, id) => leaderUpd(k,Some(v),id)
    case Remove(k, id) => leaderUpd(k,None,id)
    case Replicas(replicas) =>
      secondaries = ( for {
        replica <- replicas - self
        replicator = secondaries.getOrElse(replica, context.actorOf(Replicator.props(replica)) )
      } yield replica->replicator ).toMap
      val newRs = secondaries.values.toSet
      (replicators -- newRs).foreach(_ ! Release)
      for(r <- newRs -- replicators; (k,v)<-kv ) r ! Replicate(k,Some(v),-1)
      replicators = newRs
  }

  def replica(curSeq: Long): Receive = {
    case Get(k, id) =>
      sender ! GetResult(k, kv.get(k), id)
    case Snapshot(k, _, seq) if seq < curSeq =>
      sender ! SnapshotAck(k, seq)
    case Snapshot(k, v, seq) if seq == curSeq =>
      upd(k,v)
      context.actorOf(Repeater( persistence, Persist(k,v,curSeq),
                                      sender, SnapshotAck(k,curSeq) ))
    case Persisted(_,seq) if seq == curSeq =>
      context.become(replica(curSeq+1))

  }
