package io.buoyant.grpc.runtime

import com.twitter.concurrent.{AsyncMutex, Permit}
import com.twitter.finagle.buoyant.h2
import com.twitter.io.Buf
import com.twitter.util.{Future, Return, Promise, Throw, Try}
import java.nio.ByteBuffer
import scala.util.control.NoStackTrace

/**
 * Presents a T-typed [[Stream]] from an h2.Stream of HTTP/2 frames
 * and a [[Codec]]. Handles framing and buffering of partial/mutiple
 * messages.
 *
 * Like other readable interfaces, [[DecodingStream.recv]] must be
 * invoked serially, i.e. when there are no pending `recv()` calls on
 * the stream.
 */
private[runtime] trait DecodingStream[+T] extends Stream[T] {
  import DecodingStream._

  protected[this] def frames: h2.Stream
  protected[this] def decoder: ByteBuffer => T
  protected[this] def getStatus: h2.Frame.Trailers => GrpcStatus

  // convenience
  private[this]type Releasable = Stream.Releasable[T]

  /**
   * Holds HTTP/2 data that has not yet been returned from recv().
   *
   * These states hold a ByteBuffer-backed Buf, as well as a
   * `Releaser`, which manages when HTTP/2 frames are released
   * (i.e. for flow control).
   */
  private[this] var recvState: RecvState = RecvState.Empty

  /**
   * Ensures that at most one recv call is actively being processed,
   * so that read-and-update access to `recvState` is safe.
   */
  private[this] val recvMu = new AsyncMutex()

  override def reset(e: GrpcStatus): Unit = synchronized {
    recvState = RecvState.Reset(e)
  }

  /**
   * Obtain the next gRPC message from the underlying H2 stream.
   */
  override def recv(): Future[Releasable] =
    recvMu.acquireAndRun {
      // Now we have exclusive access of `recvState` until the call
      // completes. Start by trying to obtain a message directly from
      // the buffer:
      val updateF = decode(recvState, decoder) match {
        case (s, Some(msg)) => Future.value(s -> Return(msg))
        case (s, None) => read(s)
      }
      updateF.flatMap(_updateBuffer)
    }

  /**
   * Update the receive buffer before returning the result
   * (and i.e. releasing the mutex).
   */
  private[this] val _updateBuffer: ((RecvState, Try[Releasable])) => Future[Releasable] = {
    case (rb, v) =>
      recvState = rb
      Future.const(v)
  }

  /**
   * Read from the h2 stream until the next message is fully buffered.
   */
  private[this] def read(s0: RecvState): Future[(RecvState, Try[Stream.Releasable[T]])] = {
    frames.read().transform {
      case Throw(rst: h2.Reset) => Future.exception(GrpcStatus.fromReset(rst))
      case Throw(e) => Future.exception(e)
      case Return(t: h2.Frame.Trailers) => Future.exception(getStatus(t))
      case Return(f: h2.Frame.Data) =>
        decodeFrame(s0, f, decoder) match {
          case (s1, Some(msg)) => Future.value(s1 -> Return(msg))
          case (s1, None) =>
            if (f.isEnd) Future.value(s1 -> Throw(GrpcStatus.Ok()))
            else read(s1)
        }
    }
  }
}

object DecodingStream {

  def apply[T](
    req: h2.Request,
    decodeF: ByteBuffer => T
  ): Stream[T] = new DecodingStream[T] {
    protected[this] val frames: h2.Stream = req.stream
    protected[this] def decoder: ByteBuffer => T = decodeF
    protected[this] val getStatus: h2.Frame.Trailers => GrpcStatus = _ => GrpcStatus.Ok()
  }

  def apply[T](
    rsp: h2.Response,
    decodeF: ByteBuffer => T
  ): Stream[T] = new DecodingStream[T] {
    protected[this] val frames: h2.Stream = rsp.stream
    protected[this] def decoder: ByteBuffer => T = decodeF
    protected[this] val getStatus: h2.Frame.Trailers => GrpcStatus = GrpcStatus.fromTrailers(_)
  }

  private sealed trait RecvState

  private object RecvState {

    case class Buffer(
      /** The current gRPC message header, if one has been parsed */
      header: Option[Header] = None,
      /** Unparsed bytes */
      buf: Buf = Buf.Empty,
      /** Tracks how many bytes are consumed and when the underling data may be released */
      releaser: Releaser = Releaser.Empty
    ) extends RecvState

    case class Reset(error: GrpcStatus) extends RecvState

    val Empty: RecvState = Buffer()
  }

  /**
   * Keeps track of when underlying h2 frames should be released.
   *
   * As bytes are read from H2 frames, they are _tracked_. As headers
   * and messages are read, bytes are _consumed_. When a message is
   * being constructed, it is paired with a _releasable_ function that
   * frees the underlying H2 frames as needed.
   */
  private trait Releaser {

    /**
     * Add the given H2 frame to be tracked and released.
     */
    def track(frame: h2.Frame): Releaser

    /**
     * Mark `n` bytes as consumed.
     */
    def consume(n: Int): Releaser

    /**
     * Reap all consumed frames into a releasable function, and return
     * the updated state afterwards.
     */
    def releasable(): (Releaser, Releaser.Func)
  }

  private object Releaser {
    type Func = () => Future[Unit]
    val NopFunc: Func = () => Future.Unit

    class Underflow extends Exception("released too many bytes") with NoStackTrace

    object Empty extends Releaser {
      override def track(f: h2.Frame): Releaser = mk(f)

      override def consume(n: Int): Releaser =
        if (n == 0) this
        else throw new Underflow

      override def releasable(): (Releaser, Func) = (this, NopFunc)
    }

    private[this] case class FrameReleaser(
      remaining: Int,
      release: () => Future[Unit],
      next: Releaser
    ) extends Releaser {
      require(remaining >= 0)

      override def track(f: h2.Frame): Releaser =
        copy(next = next.track(f))

      override def consume(n: Int): Releaser =
        if (n == 0) this
        else if (n >= remaining) {
          // Leave a 0-length releasable, so that `release` is chained
          // into the next `releasable` call.
          copy(
            remaining = 0,
            next = next.consume(n - remaining)
          )
        } else copy(remaining = remaining - n)

      override def releasable(): (Releaser, Func) =
        if (remaining == 0) {
          val (rest, releaseRest) = next.releasable()
          val doRelease = () => release().before(releaseRest())
          rest -> doRelease
        } else {
          // This frame isn't yet releasable, but we require that this
          // sub-slice is released before releasing the frame:
          val p = new Promise[Unit]
          val rest = copy(release = () => p.before(release()))
          val doRelease = () => { p.setDone(); Future.Unit }
          rest -> doRelease
        }
    }

    private[this] def mk(frame: h2.Frame): Releaser = frame match {
      case f: h2.Frame.Data => FrameReleaser(f.buf.length, f.release, Empty)
      case f: h2.Frame.Trailers => FrameReleaser(0, f.release, Empty)
    }
  }

  private[this] case class Header(compressed: Boolean, size: Int)

  private[this] val GrpcFrameHeaderSz = Codec.GrpcFrameHeaderSz

  private val hasHeader: ByteBuffer => Boolean =
    bb => { GrpcFrameHeaderSz <= bb.remaining }

  private val hasMessage: (ByteBuffer, Int) => Boolean =
    (bb, msgsz) => { msgsz <= bb.remaining }

  private def decodeHeader(bb0: ByteBuffer): Header = {
    require(hasHeader(bb0))
    val bb = bb0.duplicate()
    bb.limit(bb.position + GrpcFrameHeaderSz)
    val compressed = (bb.get == 1)
    val sz = bb.getInt
    Header(compressed, sz)
  }

  private def decode[T](
    s: RecvState,
    decoder: ByteBuffer => T
  ): (RecvState, Option[Stream.Releasable[T]]) =
    s match {
      case RecvState.Buffer(Some(h0), buf, releaser) =>
        val Buf.ByteBuffer.Owned(bb0) = Buf.ByteBuffer.coerce(buf)
        decodeMessage(h0, bb0.duplicate(), releaser, decoder)

      case s => (s, None)
    }

  private def decodeFrame[T](
    s0: RecvState,
    frame: h2.Frame.Data,
    decoder: ByteBuffer => T
  ): (RecvState, Option[Stream.Releasable[T]]) =
    s0 match {
      case e@RecvState.Reset(_) => (e, None)

      case RecvState.Buffer(None, initbuf, releaser0) =>
        val buf = initbuf.concat(frame.buf)
        val Buf.ByteBuffer.Owned(bb0) = Buf.ByteBuffer.coerce(buf)
        val bb = bb0.duplicate()
        val releaser = releaser0.track(frame)

        if (hasHeader(bb)) {
          val h0 = decodeHeader(bb)
          bb.position(bb.position + GrpcFrameHeaderSz)
          decodeMessage(h0, bb, releaser.consume(GrpcFrameHeaderSz), decoder)
        } else (RecvState.Buffer(None, buf, releaser), None)

      case RecvState.Buffer(Some(h0), initbuf, releaser) =>
        val buf = initbuf.concat(frame.buf)
        val Buf.ByteBuffer.Owned(bb0) = Buf.ByteBuffer.coerce(buf)
        decodeMessage(h0, bb0.duplicate(), releaser.track(frame), decoder)
    }

  private def decodeMessage[T](
    h0: Header,
    bb: ByteBuffer,
    releaser: Releaser,
    decoder: ByteBuffer => T
  ): (RecvState, Option[Stream.Releasable[T]]) =
    if (h0.compressed) throw new IllegalArgumentException("compression not supported yet")
    else if (hasMessage(bb, h0.size)) {
      // Frame fully buffered.
      val end = bb.position + h0.size
      val (nextReleaser, release) = releaser.consume(h0.size).releasable()
      val msg = {
        val msgbb = bb.duplicate()
        msgbb.limit(end)
        val msg = decoder(msgbb)
        Some(Stream.Releasable(msg, release))
      }
      bb.position(end)
      if (hasHeader(bb)) {
        // And another header buffered...
        val h1 = decodeHeader(bb)
        bb.position(bb.position + GrpcFrameHeaderSz)
        val r = nextReleaser.consume(GrpcFrameHeaderSz)
        (RecvState.Buffer(Some(h1), Buf.ByteBuffer.Owned(bb), r), msg)
      } else (RecvState.Buffer(None, Buf.ByteBuffer.Owned(bb), nextReleaser), msg)
    } else (RecvState.Buffer(Some(h0), Buf.ByteBuffer.Owned(bb), releaser), None)

}