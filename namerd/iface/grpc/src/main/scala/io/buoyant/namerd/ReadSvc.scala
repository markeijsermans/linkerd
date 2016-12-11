package io.buoyant.namerd

import com.google.protobuf.{ByteString, GeneratedMessageV3}
import com.twitter.finagle.{Dentry, Dtab => FDtab, NameTree, Path => FPath, Service}
import com.twitter.finagle.buoyant.h2
import com.twitter.io.Buf
import com.twitter.util.{Future, Return, Throw, Try}
import io.buoyant.dtab._
import io.buoyant.glomgold.runtime.ServerDispatcher
import java.nio.ByteBuffer
import scala.collection.JavaConverters._

object ReadSvc {

  def mk(): Service[h2.Request, h2.Response] =
    new ServerDispatcher(new NamerdGrpc.ReadSvc.Server(Iface) :: Nil)

  object Iface extends NamerdGrpc.ReadSvc {
    override def parse(req: NamerdGrpc.ParseReq): Future[NamerdGrpc.ParseRsp] =
      Future {
        req.text match {
          case Some(txt) => FDtab.read(txt)
          case None => FDtab.empty
        }
      }.transform(mkParseRsp)

    private[this] val mkParseRsp: Try[FDtab] => Future[NamerdGrpc.ParseRsp] = {
      case Return(dtab) =>
        val d = NamerdGrpc.ParseRsp.OneofResult.Dtab(mkDtab(dtab))
        val r = NamerdGrpc.ParseRsp(Some(d))
        Future.value(r)

      case Throw(exc) =>
        val e = NamerdGrpc.ParseRsp.OneofResult.Error(NamerdGrpc.ParseRsp.Error(Option(exc.getMessage), None))
        val r = NamerdGrpc.ParseRsp(Some(e))
        Future.value(r)
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
      case Dentry(pfx, dst) =>
        Dtab.Dentry(Some(mkPrefix(pfx)), Some(mkPathNameTree(dst)))
    })

}
