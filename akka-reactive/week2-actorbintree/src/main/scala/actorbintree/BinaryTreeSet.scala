/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor.*
import scala.collection.immutable.Queue

object BinaryTreeSet:

  trait Operation:
    def requester: ActorRef
    def id: Int
    def elem: Int

  object Operation:
    def unapply(op: Operation): Option[Int] = Some(op.elem)

  trait OperationReply:
    def id: Int

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection */
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply

  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply



class BinaryTreeSet extends Actor:
  import BinaryTreeSet.*
  import BinaryTreeNode.*

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional (used to stash incoming operations during garbage collection)
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case op: Operation => root ! op
    case GC =>
      val newRoot = createRoot
      root ! CopyTo (newRoot)
      context.become(garbageCollecting(newRoot))
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case op:Operation => pendingQueue = pendingQueue.appended(op)
    case CopyFinished =>
      root = newRoot
      pendingQueue.foreach(root ! _)
      pendingQueue = Queue.empty[Operation]
      context.become(normal)
    case GC => ()
  }


object BinaryTreeNode:
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  /**
   * Acknowledges that a copy has been completed. This message should be sent
   * from a node to its parent, when this node and all its children nodes have
   * finished being copied.
   */
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean): Props = Props(classOf[BinaryTreeNode], elem, initiallyRemoved)

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor with ActorLogging:
  import BinaryTreeNode.*
  import BinaryTreeSet.*

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  private def newLeaf(el: Int) =
    context.actorOf(BinaryTreeNode.props(el, initiallyRemoved = true),s"$el")

  private def subtree(el:Int) =
    require(el!=elem)
    val side = if el > elem then Right else Left
    subtrees = subtrees.updatedWith(side){
      case None => Some(newLeaf(el))
      case some => some
    }
    subtrees(side)

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive =  {
    case Contains(rr,id,el) if el == elem => rr ! ContainsResult(id,!removed)
    case Insert(rr,id,el) if el == elem =>   removed = false
                                             rr ! OperationFinished(id)
    case Remove(rr,id,el) if el == elem =>   removed = true
                                             rr ! OperationFinished(id)
    case op @ Operation(el) =>               subtree(el) ! op

    case CopyTo(target) =>
      subtrees.values.foreach(_ ! CopyTo(target) )
      if !removed then target ! Insert(self, self.hashCode, elem)
      doCopy(subtrees.values.toSet, insertConfirmed = removed)
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    case OperationFinished(_) => doCopy(expected, insertConfirmed = true)
    case CopyFinished =>         doCopy(expected.excl(sender), insertConfirmed)
  }

  def doCopy(expected: Set[ActorRef], insertConfirmed: Boolean): Unit =
    if expected.isEmpty && insertConfirmed then
      context.parent ! CopyFinished
      context.stop(self)
    else context.become(copying(expected,insertConfirmed))
