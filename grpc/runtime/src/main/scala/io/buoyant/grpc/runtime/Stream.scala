package io.buoyant.grpc.runtime

import com.twitter.concurrent.AsyncQueue
import com.twitter.finagle.buoyant.h2
import com.twitter.io.Buf
import com.twitter.util.{Activity, Event, Future, Promise, Return, Throw, Try, Var}
import java.util.concurrent.atomic.AtomicReference

trait Stream[+T] {
  def recv(): Future[Stream.Releasable[T]]
}

object Stream {

  case class Releasable[+T](value: T, release: () => Future[Unit])

  trait Tx[-T] {
    def send(t: T): Future[Unit]

    def close(): Future[Unit]

    // TODO support grpc error types
    // def reset(err: Grpc.Error): Unit
  }

  def apply[T](): Stream[T] with Tx[T] = new Stream[T] with Tx[T] {
    // TODO bound queue? not strictly necessary if send() future observed...
    private[this] val q = new AsyncQueue[Releasable[T]]

    override def recv(): Future[Releasable[T]] = q.poll()

    override def send(msg: T): Future[Unit] = {
      val p = new Promise[Unit]
      val release: () => Future[Unit] = { () =>
        p.setDone()
        Future.Unit
      }
      if (q.offer(Releasable(msg, release))) p
      else Future.exception(Rejected)
    }

    override def close(): Future[Unit] = {
      q.fail(Closed, discard = false)
      Future.Unit
    }
  }

  object Closed extends Throwable
  object Rejected extends Throwable

  def fromActivity[T](act: Activity[T]): Stream[T] =
    fromEvent(act.values)

  def fromVar[T](v: Var[Try[T]]): Stream[T] =
    fromEvent(v.changes)

  /**
   * Convert an Event to a gRPC Stream.
   *
   * Because this is designed to be used in the context of an
   * Activity/Var, the stream may not expose ALL state transitions --
   * intermediate state transitions may be dropped in favor of
   * buffering only the last-observed state.
   *
   * TODO streams are not properly failed on event failure.
   *
   * TODO when a stream fails (from the remote), we should eagerly
   * stop observing the event.
   */
  def fromEvent[T](event: Event[Try[T]]): Stream[T] = {

    // We only buffer at most one event state.  Event states are not
    // written directly from the Event, since this provide no
    // mechanism for back-pressure.
    val ref = new AtomicReference[EventState[T]](EventState.Empty)

    @scala.annotation.tailrec def updateState(value: Try[T]): Unit = {
      ref.get match {
        case EventState.Closed =>
        case EventState.Enqueued(Throw(_)) =>

        case state@(EventState.Empty | EventState.Enqueued(Return(_))) =>
          if (!ref.compareAndSet(state, EventState.Enqueued(value))) updateState(value)

        case state@EventState.Waiting(promise) =>
          if (ref.compareAndSet(state, EventState.Enqueued(value))) {
            promise.updateIfEmpty(Return.Unit); ()
          } else updateState(value)
      }
    }
    val updater = event.respond(updateState(_))

    // Stream write failures mean the stream is closed...
    val rescueUpdate: PartialFunction[Throwable, Future[Unit]] = {
      case e: Throwable =>
        ref.getAndSet(EventState.Closed) match {
          case EventState.Closed => Future.Unit
          case _ => updater.close()
        }
    }

    val stream = Stream[T]()
    def loop(): Future[Unit] = ref.get match {
      case EventState.Closed =>
        Future.Unit

      case s@EventState.Empty =>
        ref.compareAndSet(s, EventState.Waiting(new Promise))
        loop()

      case EventState.Waiting(wait) =>
        wait.before(loop())

      case s@EventState.Enqueued(Return(v)) =>
        if (ref.compareAndSet(s, EventState.Empty)) {
          stream.send(v).rescue(rescueUpdate).before(loop())
        } else loop()

      case s@EventState.Enqueued(Throw(e)) =>
        if (ref.compareAndSet(s, EventState.Closed)) {
          // TODO reset stream
          updater.close().before(stream.close())
        } else loop()
    }

    loop()
    stream
  }

  private[this] sealed trait EventState[+T]
  private[this] object EventState {
    object Empty extends EventState[Nothing]
    case class Waiting[T](next: Promise[Unit]) extends EventState[T]
    case class Enqueued[T](pending: Try[T]) extends EventState[T]
    object Closed extends EventState[Nothing]
  }

}
