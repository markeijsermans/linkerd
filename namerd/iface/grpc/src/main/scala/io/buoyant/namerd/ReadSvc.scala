package io.buoyant.namerd

import com.google.protobuf.{ByteString, GeneratedMessageV3}
import com.twitter.finagle.{Dentry, Dtab => FDtab, NameTree, Path => FPath, Service}
import com.twitter.finagle.buoyant.h2
import com.twitter.io.Buf
import com.twitter.util.{Future, Return, Throw, Try}
import io.buoyant.dtab.DtabOuterClass._
import java.nio.ByteBuffer
import scala.collection.JavaConverters._

trait ReadSvc {
  def parse(req: NamerdGrpc.ParseReq): Future[NamerdGrpc.ParseRsp]
}

object ReadSvc {

  def service(iface: ReadSvc): Service[h2.Request, h2.Response] =
    new ReadSvcService(iface)

  def mk(): Service[h2.Request, h2.Response] =
    service(Server)

  object Server extends ReadSvc {
    override def parse(req: NamerdGrpc.ParseReq): Future[NamerdGrpc.ParseRsp] =
      Future { FDtab.read(req.getText) }.transform(mkParseRsp)

    private[this] val mkParseRsp: Try[FDtab] => Future[NamerdGrpc.ParseRsp] = {
      case Return(dtab) =>
        val rb = NamerdGrpc.ParseRsp.newBuilder
        rb.setDtab(mkDtab(dtab))
        Future.value(rb.build())

      case Throw(exc) =>
        val rb = NamerdGrpc.ParseRsp.newBuilder
        val eb = NamerdGrpc.ParseRsp.Error.newBuilder
        eb.setDescription(exc.getMessage)
        rb.setError(eb)
        Future.value(rb.build())
    }
  }

  private[this] def newElem =
    Dtab.Dentry.Prefix.Elem.newBuilder

  private[this] val WildcardElem =
    newElem.setWildcard(Dtab.Dentry.Prefix.Elem.Wildcard.newBuilder).build()

  private def mkPrefix(pfx: Dentry.Prefix): Dtab.Dentry.Prefix = {
    val pb = Dtab.Dentry.Prefix.newBuilder
    val iter = pfx.elems.iterator
    while (iter.hasNext) {
      iter.next() match {
        case Dentry.Prefix.AnyElem =>
          pb.addElems(WildcardElem)

        case Dentry.Prefix.Label(buf) =>
          val Buf.ByteArray.Owned(ba, off, len) = Buf.ByteArray.coerce(buf)
          val label = ByteString.copyFrom(ba, off, len)
          pb.addElems(newElem.setLabel(label))
      }
    }
    pb.build()
  }

  private def mkPath(path: FPath): Path = {
    val pb = Path.newBuilder
    val iter = path.elems.iterator
    while (iter.hasNext) {
      val Buf.ByteArray.Owned(ba, off, len) = Buf.ByteArray.coerce(iter.next())
      pb.addElems(ByteString.copyFrom(ba, off, len))
    }
    pb.build()
  }

  private[this] val Neg = PathNameTree.Nop.NEG
  private[this] val Fail = PathNameTree.Nop.FAIL
  private[this] val Empty = PathNameTree.Nop.EMPTY
  private[this] def newTree = PathNameTree.newBuilder

  private[this] val mkPathNameTree: NameTree[FPath] => PathNameTree = {
    case NameTree.Neg => newTree.setNop(Neg).build()
    case NameTree.Fail => newTree.setNop(Fail).build()
    case NameTree.Empty => newTree.setNop(Empty).build()

    case NameTree.Leaf(path) =>
      val l = PathNameTree.Leaf.newBuilder.setPath(mkPath(path))
      newTree.setLeaf(l).build()

    case NameTree.Alt(trees@_*) =>
      val altb = PathNameTree.Alt.newBuilder
      val iter = trees.iterator
      while (iter.hasNext) altb.addTrees(mkPathNameTree(iter.next()))
      newTree.setAlt(altb).build()

    case NameTree.Union(trees@_*) =>
      val unionb = PathNameTree.Union.newBuilder
      val iter = trees.iterator
      while (iter.hasNext) {
        val wt = iter.next()
        val w = PathNameTree.Union.Weighted.newBuilder
          .setWeight(wt.weight)
          .setTree(mkPathNameTree(wt.tree))
        unionb.addTrees(w)
      }
      newTree.setUnion(unionb).build()
  }

  private[this] def mkDtab(dtab: FDtab): Dtab = {
    val dtabb = Dtab.newBuilder

    val iter = dtab.iterator
    while (iter.hasNext) {
      val Dentry(pfx, dst) = iter.next()
      val dentry = Dtab.Dentry.newBuilder
        .setPrefix(mkPrefix(pfx))
        .setDst(mkPathNameTree(dst))
      dtabb.addDentries(dentry)
    }

    dtabb.build()
  }

  private class ReadSvcService(readSvc: ReadSvc) extends Service[h2.Request, h2.Response] {

    def apply(req: h2.Request): Future[h2.Response] = {
      req.method match {
        case h2.Method.Post =>
          req.path match {
            case "/io.buoyant.namerd.ReadSvc/Parse" =>
              toParseReq(req).flatMap(serveParse).map(toRsp)

            case _ => Future.value(NotFound)
          }
        case _ => Future.value(InvalidMethod)
      }
    }

    private[this] val serveParse: NamerdGrpc.ParseReq => Future[NamerdGrpc.ParseRsp] =
      readSvc.parse _
  }

  private def NotFound: h2.Response =
    h2.Response(h2.Status.NotFound, h2.Stream.empty())

  private def InvalidMethod: h2.Response =
    h2.Response(h2.Status.MethodNotAllowed, h2.Stream.empty())

  private def toParseReq(req: h2.Request): Future[NamerdGrpc.ParseReq] =
    req.headers.get("content-type") match {
      case Seq("application/grpc" | "application/grpc+proto") =>
        // TODO check grpc-encoding
        readAll(req.stream).map(decodeParseReq)

      case typ =>
        Future.exception(new IllegalArgumentException(s"unexpected type: $typ"))
    }

  // Does a copy. ;(
  private val decodeParseReq: Buf => NamerdGrpc.ParseReq = { buf =>
    val Buf.ByteArray.Owned(ba, start, end) = Buf.ByteArray.coerce(buf)
    val len = end - start
    if (len < 5) throw new IllegalArgumentException("too short")

    val compressed = ba(0) == 1
    if (compressed) throw new IllegalArgumentException("compressed")

    val off = start + 5
    val bs = ByteString.copyFrom(ba, off, end - off)

    NamerdGrpc.ParseReq.parseFrom(bs)
  }

  private[this] val UncompressedBuf = Buf.ByteArray.Owned(Array[Byte](0))

  private def toRsp(msg: GeneratedMessageV3): h2.Response = {
    val buf = {
      val bytes = msg.toByteArray
      val lenbb = ByteBuffer.allocate(4).putInt(bytes.length)
      lenbb.flip()
      val lenBuf = Buf.ByteBuffer.Owned(lenbb)
      val msgBuf = Buf.ByteArray.Owned(bytes)
      UncompressedBuf.concat(lenBuf).concat(msgBuf)
    }

    // Just set up the stream writes but don't wait for it to complete.
    val stream = h2.Stream()
    stream.write(h2.Frame.Data(buf, eos = false))
      .before(stream.write(h2.Frame.Trailers("grpc-status" -> "0")))

    val rsp = h2.Response(h2.Status.Ok, stream)
    rsp.headers.set("content-length", buf.length.toString)
    rsp
  }

  private def readAll(stream: h2.Stream): Future[Buf] = {
    def appendTo(orig: Buf): Future[Buf] =
      stream.read().flatMap {
        case data: h2.Frame.Data =>
          val accum = orig.concat(data.buf)
          if (data.isEnd) Future.value(accum)
          else appendTo(accum)

        case trls: h2.Frame.Trailers => Future.value(orig)
      }
    appendTo(Buf.Empty)
  }

}
