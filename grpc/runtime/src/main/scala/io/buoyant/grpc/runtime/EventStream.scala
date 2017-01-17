package io.buoyant.grpc.runtime

import com.twitter.util.{Activity, Event, Future, Promise, Return, Throw, Try, Var}
import java.util.concurrent.atomic.AtomicReference

object EventStream {

  def apply[T](event: Event[Ev[T]]): Stream[T] =
    new EventStream(event)

  def apply[T](var0: Var[Ev[T]]): Stream[T] =
    apply(var0.changes)

  def apply[T](act: Activity[T]): Stream[T] =
    apply(act.values.map {
      case Return(t) => Val(t)
      case Throw(e) => End(Throw(e))
    })

  sealed trait Ev[+T]
  case class Val[T](value: T) extends Ev[T]
  case class End[T](value: Try[T]) extends Ev[T]

  private sealed trait State[+T]
  private object State {
    object Empty extends State[Nothing]
    case class Waiting[T](next: Promise[Unit]) extends State[T]
    case class Ready[T](value: Ev[T]) extends State[T]
    object Closed extends State[Nothing]
  }
}

/**
 * Convert an Event to a gRPC Stream.
 *
 * Because this is designed to be used in the context of an
 * Activity/Var, the stream may NOT reflect all event states.
 * Intermediate state transitions may be dropped in favor of buffering
 * only the last-observed state.
 *
 * TODO streams are not properly failed on event failure.
 *
 * TODO when a stream fails (from the remote), we should eagerly
 * stop observing the event.
 */
class EventStream[+T](events: Event[EventStream.Ev[T]]) extends Stream[T] {
  import EventStream._

  // We only buffer at most one event state.  Event states are not
  // written directly from the Event, since this provide no mechanism
  // for back-pressure. Updates to this state are performed without
  // locking.
  private[this] val stateRef: AtomicReference[State[T]] =
    new AtomicReference(State.Empty)

  // Updates from the Event move the state to Ready if the Stream is
  // not otherwise closed/completed.
  private[this] val updater = {
    @scala.annotation.tailrec
    def updateState(value: Ev[T]): Unit = stateRef.get match {
      case State.Closed | State.Ready(End(_)) =>

      case state@(State.Empty | State.Ready(Val(_))) =>
        stateRef.compareAndSet(state, State.Ready(value)) match {
          case true =>
          case false => updateState(value)
        }

      case state@State.Waiting(promise) =>
        // A recver is waiting for a value, so update before notifying
        // the recver of the update.
        stateRef.compareAndSet(state, State.Ready(value)) match {
          case true =>
            promise.updateIfEmpty(Return.Unit); ()
          case false => updateState(value)
        }
    }

    events.respond(updateState(_))
  }

  /**
   * Consume the most recent event value or wait for the next event
   * value.
   */
  override def recv(): Future[Stream.Releasable[T]] = stateRef.get match {
    // basically tailrec, except futurey.

    case State.Closed =>
      Future.exception(Stream.Closed)

    case s@State.Empty =>
      val p = new Promise[Unit]
      stateRef.compareAndSet(s, State.Waiting(p)) match {
        case true => p.before(recv())
        case false => recv()
      }

    case State.Waiting(p) =>
      // Shouldn't actually reach this, but for the sake of completeness...
      p.before(recv())

    case s@State.Ready(Val(v)) =>
      stateRef.compareAndSet(s, State.Empty) match {
        case true => Future.value(Stream.Releasable(v))
        case false => recv()
      }

    case s@State.Ready(End(t)) =>
      stateRef.compareAndSet(s, State.Closed) match {
        case true =>
          updater.close()
            .before(Future.const(t.map(toReleasable)))
        case false => recv()
      }
  }

  private[this] def toReleasable: T => Stream.Releasable[T] =
    Stream.Releasable(_)
}
