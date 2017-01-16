package io.buoyant.namerd
package iface

import com.twitter.concurrent.AsyncQueue
import com.twitter.finagle.{Dentry, Dtab => FDtab, Namer, NameTree, Path => FPath, Service}
import com.twitter.finagle.buoyant.h2
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.io.Buf
import com.twitter.util.{Activity, Closable, Event, Future, Return, Promise, Throw, Try}
import io.buoyant.grpc.runtime.Stream
import io.buoyant.namerd.{VersionedDtab => VDtab}
import io.buoyant.proto._
import io.buoyant.proto.namerd._
import java.nio.ByteBuffer
import scala.collection.JavaConverters._

object GrpcInterpreter {

  def mk(store: DtabStore, namers: Map[FPath, Namer], stats: StatsReceiver): Interpreter.Server =
    new Interpreter.Server(Iface(store, namers, stats))

  case class Iface(store: DtabStore, namers: Map[FPath, Namer], stats: StatsReceiver)
    extends Interpreter {

    override def parse(req: ParseReq): Future[ParseRsp] =
      Future {
        req.text match {
          case Some(txt) => FDtab.read(txt)
          case None => FDtab.empty
        }
      }.transform(mkParseRsp)

    private[this] val mkParseRsp: Try[FDtab] => Future[ParseRsp] = {
      case Return(dtab) =>
        val result = ParseRsp.OneofResult.Dtab(mkDtab(dtab))
        Future.value(ParseRsp(Some(result)))

      case Throw(exc) =>
        val e = ParseRsp.Error(Option(exc.getMessage), None)
        val result = ParseRsp.OneofResult.Error(e)
        Future.value(ParseRsp(Some(result)))
    }

    override def getDtab(req: DtabReq): Future[DtabRsp] =
      req.ns match {
        case None => Future.value(DtabRspNoNamespace)
        case Some(ns) => store.observe(ns).toFuture.transform(mkDtabRsp)
      }

    private[this] val mkDtabRsp: Try[Option[VDtab]] => Future[DtabRsp] = {
      case Return(None) => Future.value(DtabRspNotFound)
      case Return(Some(vdtab)) =>
        Future.value(DtabRsp(Some(DtabRsp.OneofResult.Dtab(mkVersionedDtab(vdtab)))))
      case Throw(e) =>
        Future.value(DtabRspError(e.getMessage, DtabRsp.Error.Code.UNKNOWN))
    }

    override def streamDtab(req: DtabReq): Stream[DtabRsp] = {
      req.ns match {
        case None =>
          val stream = Stream[DtabRsp]()
          // XXX FIXME ERROR
          stream.close()
          stream

        case Some(ns) =>
          Stream.fromEvent(store.observe(ns).values.map {
            case Throw(exc) =>
              //Return(DtabRspError(exc.getMessage, DtabRsp.Error.Code.UNKNOWN))
              Throw(exc)
            case Return(None) =>
              Return(DtabRspNotFound)
            case Return(Some(vdtab)) =>
              Return(DtabRsp(Some(DtabRsp.OneofResult.Dtab(mkVersionedDtab(vdtab)))))
          })
      }
    }
  }

  private[this] val WildcardElem =
    Dtab.Dentry.Prefix.Elem(Some(
      Dtab.Dentry.Prefix.Elem.OneofValue.Wildcard(
        Dtab.Dentry.Prefix.Elem.Wildcard()
      )
    ))

  private def mkPrefix(pfx: Dentry.Prefix): Dtab.Dentry.Prefix =
    Dtab.Dentry.Prefix(pfx.elems.map {
      case Dentry.Prefix.AnyElem => WildcardElem
      case Dentry.Prefix.Label(buf) =>
        Dtab.Dentry.Prefix.Elem(Some(Dtab.Dentry.Prefix.Elem.OneofValue.Label(buf)))
    })

  private def mkPath(path: FPath): Path = Path(path.elems)

  private[this] val Neg = PathNameTree(Some(PathNameTree.OneofNode.Nop(PathNameTree.Nop.NEG)))
  private[this] val Fail = PathNameTree(Some(PathNameTree.OneofNode.Nop(PathNameTree.Nop.FAIL)))
  private[this] val Empty = PathNameTree(Some(PathNameTree.OneofNode.Nop(PathNameTree.Nop.EMPTY)))

  private[this] val mkPathNameTree: NameTree[FPath] => PathNameTree = {
    case NameTree.Neg => Neg
    case NameTree.Fail => Fail
    case NameTree.Empty => Empty

    case NameTree.Leaf(path) =>
      PathNameTree(Some(PathNameTree.OneofNode.Leaf(PathNameTree.Leaf(Some(mkPath(path))))))

    case NameTree.Alt(trees@_*) =>
      PathNameTree(Some(PathNameTree.OneofNode.Alt(PathNameTree.Alt(trees.map(mkPathNameTree)))))

    case NameTree.Union(trees@_*) =>
      val weighted = trees.map { wt =>
        PathNameTree.Union.Weighted(Some(wt.weight), Some(mkPathNameTree(wt.tree)))
      }
      PathNameTree(Some(PathNameTree.OneofNode.Union(PathNameTree.Union(weighted))))
  }

  private[this] def mkDtab(dtab: FDtab): Dtab =
    Dtab(dtab.map {
      case Dentry(pfx, dst) => Dtab.Dentry(Some(mkPrefix(pfx)), Some(mkPathNameTree(dst)))
    })

  private[this] def mkVersionedDtab(vdtab: VDtab): VersionedDtab = {
    val v = VersionedDtab.Version(Some(vdtab.version))
    val d = mkDtab(vdtab.dtab)
    VersionedDtab(Some(v), Some(d))
  }

  private[this] def DtabRspError(description: String, code: DtabRsp.Error.Code.Value) = {
    val error = DtabRsp.Error(Some(description), Some(code))
    DtabRsp(Some(DtabRsp.OneofResult.Error(error)))
  }

  private[this] val DtabRspNoNamespace =
    DtabRspError("No namespace specified", DtabRsp.Error.Code.BAD_REQUEST)

  private[this] val DtabRspNotFound =
    DtabRspError("Namespace not found", DtabRsp.Error.Code.NOT_FOUND)

}
