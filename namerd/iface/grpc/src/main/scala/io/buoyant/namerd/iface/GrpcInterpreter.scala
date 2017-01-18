package io.buoyant.namerd
package iface

import com.twitter.concurrent.AsyncQueue
import com.twitter.finagle.{Dentry, Dtab, Name, Namer, NameTree, Path, Service}
import com.twitter.finagle.buoyant.h2
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.io.Buf
import com.twitter.util.{Activity, Closable, Event, Future, Return, Promise, Throw, Try}
import io.buoyant.grpc.runtime.{Stream, EventStream}
import io.buoyant.namer.ConfiguredDtabNamer
import io.buoyant.proto.{Dtab => ProtoDtab, Path => ProtoPath, _}
import io.buoyant.proto.namerd.{VersionedDtab => ProtoVersionedDtab, _}
import java.nio.ByteBuffer
import scala.collection.JavaConverters._

object GrpcInterpreter {

  def mk(store: DtabStore, namers: Map[Path, Namer], stats: StatsReceiver): Interpreter.Server =
    new Interpreter.Server(Iface(store, namers, stats))

  case class Iface(store: DtabStore, namers: Map[Path, Namer], stats: StatsReceiver)
    extends Interpreter {

    override def parse(req: ParseReq): Future[ParseRsp] =
      Future {
        req.text match {
          case Some(txt) => Dtab.read(txt)
          case None => Dtab.empty
        }
      }.transform(mkParseRsp)

    private[this] val mkParseRsp: Try[Dtab] => Future[ParseRsp] = {
      case Return(dtab) =>
        val result = ParseRsp.OneofResult.Dtab(toProto(dtab))
        Future.value(ParseRsp(Some(result)))

      case Throw(exc) =>
        val e = ParseRsp.Error(Option(exc.getMessage), None)
        val result = ParseRsp.OneofResult.Error(e)
        Future.value(ParseRsp(Some(result)))
    }

    override def getDtab(req: DtabReq): Future[DtabRsp] =
      req.ns match {
        case None => Future.value(DtabRspNoNamespace)
        case Some(ns) => store.observe(ns).toFuture.transform(transformDtabRsp)
      }

    override def streamDtab(req: DtabReq): Stream[DtabRsp] =
      req.ns match {
        case None => Stream.value(DtabRspNoNamespace)
        case Some(ns) => EventStream(store.observe(ns).values.map(mkDtabRspEv))
      }

    override def getBoundTree(req: BindReq): Future[BoundTreeRsp] =
      req.ns match {
        case None => Future.value(BoundTreeRspNoNamespace)
        case Some(ns) =>
          req.name match {
            case None => Future.value(BoundTreeRspNoName)
            case Some(pname) if pname.elems.isEmpty => Future.value(BoundTreeRspNoName)
            case Some(pname) =>
              val dtab = req.dtab match {
                case None => Dtab.empty
                case Some(d) => fromProto(d)
              }
              val name = fromProto(pname)
              bind(ns, dtab, name).toFuture.map(mkBoundTreeRsp)
          }
      }

    override def streamBoundTree(req: BindReq): Stream[BoundTreeRsp] =
      req.ns match {
        case None => Stream.value(BoundTreeRspNoNamespace)
        case Some(ns) =>
          req.name match {
            case None => Stream.value(BoundTreeRspNoName)
            case Some(pname) if pname.elems.isEmpty => Stream.value(BoundTreeRspNoName)
            case Some(pname) =>
              val dtab = req.dtab match {
                case None => Dtab.empty
                case Some(d) => fromProto(d)
              }
              val name = fromProto(pname)
              val ev = bind(ns, dtab, name).values.map(mkBoundTreeRspEv)
              EventStream(ev)
          }
      }

    private[this] def bind(ns: String, localDtab: Dtab, name: Path) = {
      val dtabVar = store.observe(ns).map(extractDtab)
      val interpreter = ConfiguredDtabNamer(dtabVar, namers.toSeq)
      interpreter.bind(localDtab, name)
    }
  }

  private[this] val extractDtab: Option[VersionedDtab] => Dtab = {
    case None => Dtab.empty
    case Some(VersionedDtab(dtab, _)) => dtab
  }

  private[this] val WildcardElem =
    ProtoDtab.Dentry.Prefix.Elem(Some(
      ProtoDtab.Dentry.Prefix.Elem.OneofValue.Wildcard(
        ProtoDtab.Dentry.Prefix.Elem.Wildcard()
      )
    ))

  private def toProto(pfx: Dentry.Prefix): ProtoDtab.Dentry.Prefix =
    ProtoDtab.Dentry.Prefix(pfx.elems.map {
      case Dentry.Prefix.AnyElem => WildcardElem
      case Dentry.Prefix.Label(buf) =>
        ProtoDtab.Dentry.Prefix.Elem(Some(ProtoDtab.Dentry.Prefix.Elem.OneofValue.Label(buf)))
    })

  private def fromProto(ppfx: ProtoDtab.Dentry.Prefix): Dentry.Prefix =
    Dentry.Prefix(ppfx.elems.map(mapPrefixElemToProto): _*)

  private[this] val mapPrefixElemToProto: ProtoDtab.Dentry.Prefix.Elem => Dentry.Prefix.Elem = {
    case WildcardElem => Dentry.Prefix.AnyElem
    case ProtoDtab.Dentry.Prefix.Elem(Some(ProtoDtab.Dentry.Prefix.Elem.OneofValue.Label(buf))) =>
      Dentry.Prefix.Label(buf)
    case elem =>
      throw new IllegalArgumentException(s"Illegal prefix element: $elem")
  }

  private[this] def toProto(dtab: Dtab): ProtoDtab =
    ProtoDtab(dtab.map { dentry =>
      val ppfx = toProto(dentry.prefix)
      val pdst = mkPathNameTree(dentry.dst)
      ProtoDtab.Dentry(Some(ppfx), Some(pdst))
    })

  private[this] def fromProto(pdtab: ProtoDtab): Dtab =
    Dtab(pdtab.dentries.toIndexedSeq.map {
      case ProtoDtab.Dentry(Some(ppfx), Some(pdst)) =>
        val pfx = fromProto(ppfx)
        val dst = mkNameTreePath(pdst)
        Dentry(pfx, dst)
      case dentry =>
        throw new IllegalArgumentException(s"Illegal dentry: $dentry")
    })

  private def toProto(path: Path): ProtoPath = ProtoPath(path.elems)
  private def fromProto(ppath: ProtoPath): Path = Path(ppath.elems: _*)

  private[this] val mkPathNameTree: NameTree[Path] => PathNameTree = {
    case NameTree.Neg =>
      PathNameTree(Some(PathNameTree.OneofNode.Nop(PathNameTree.Nop.NEG)))

    case NameTree.Fail =>
      PathNameTree(Some(PathNameTree.OneofNode.Nop(PathNameTree.Nop.FAIL)))

    case NameTree.Empty =>
      PathNameTree(Some(PathNameTree.OneofNode.Nop(PathNameTree.Nop.EMPTY)))

    case NameTree.Leaf(path) =>
      PathNameTree(Some(PathNameTree.OneofNode.Leaf(PathNameTree.Leaf(Some(toProto(path))))))

    case NameTree.Alt(trees@_*) =>
      PathNameTree(Some(PathNameTree.OneofNode.Alt(PathNameTree.Alt(trees.map(mkPathNameTree)))))

    case NameTree.Union(trees@_*) =>
      val weighted = trees.map { wt =>
        PathNameTree.Union.Weighted(Some(wt.weight), Some(mkPathNameTree(wt.tree)))
      }
      PathNameTree(Some(PathNameTree.OneofNode.Union(PathNameTree.Union(weighted))))
  }

  private[this] val mkNameTreePath: PathNameTree => NameTree[Path] = {
    case PathNameTree(Some(PathNameTree.OneofNode.Nop(nop))) =>
      nop match {
        case PathNameTree.Nop.NEG => NameTree.Neg
        case PathNameTree.Nop.FAIL => NameTree.Fail
        case PathNameTree.Nop.EMPTY => NameTree.Empty
      }

    case PathNameTree(Some(PathNameTree.OneofNode.Leaf(PathNameTree.Leaf(Some(path))))) =>
      NameTree.Leaf(fromProto(path))

    case PathNameTree(Some(PathNameTree.OneofNode.Alt(PathNameTree.Alt(ptrees)))) =>
      val trees = ptrees.map(mkNameTreePath)
      NameTree.Alt(trees: _*)

    case PathNameTree(Some(PathNameTree.OneofNode.Union(PathNameTree.Union(ptrees)))) =>
      val trees = ptrees.collect {
        case PathNameTree.Union.Weighted(Some(weight), Some(ptree)) =>
          NameTree.Weighted(weight, mkNameTreePath(ptree))
      }
      NameTree.Union(trees: _*)

    case tree =>
      throw new IllegalArgumentException(s"illegal name tree: $tree")
  }

  private[this] def DtabRspError(description: String, code: DtabRsp.Error.Code.Value) = {
    val error = DtabRsp.Error(Some(description), Some(code))
    DtabRsp(Some(DtabRsp.OneofResult.Error(error)))
  }

  private[this] val DtabRspNoNamespace =
    DtabRspError("No namespace specified", DtabRsp.Error.Code.BAD_REQUEST)

  private[this] val DtabRspNotFound =
    DtabRspError("Namespace not found", DtabRsp.Error.Code.NOT_FOUND)

  private[this] val mkDtabRspEv: Try[Option[VersionedDtab]] => EventStream.Ev[DtabRsp] = {
    case Return(None) => EventStream.Val(DtabRspNotFound)
    case Return(Some(vdtab)) => EventStream.Val(mkDtabRsp(vdtab))
    case Throw(e) => EventStream.End(Return(DtabRspError(e.getMessage, DtabRsp.Error.Code.UNKNOWN)))
  }

  private[this] val mkDtabRsp: VersionedDtab => DtabRsp = { vdtab =>
    val v = ProtoVersionedDtab.Version(Some(vdtab.version))
    val d = toProto(vdtab.dtab)
    DtabRsp(Some(DtabRsp.OneofResult.Dtab(ProtoVersionedDtab(Some(v), Some(d)))))
  }

  private[this] val transformDtabRsp: Try[Option[VersionedDtab]] => Future[DtabRsp] = {
    case Return(None) => Future.value(DtabRspNotFound)
    case Return(Some(vdtab)) => Future.value(mkDtabRsp(vdtab))
    case Throw(e) => Future.value(DtabRspError(e.getMessage, DtabRsp.Error.Code.UNKNOWN))
  }

  private[this] def BoundTreeRspError(desc: String, code: BoundTreeRsp.Error.Code.Value) = {
    val error = BoundTreeRsp.Error(Some(desc), Some(code))
    BoundTreeRsp(Some(BoundTreeRsp.OneofResult.Error(error)))
  }

  private[this] val BoundTreeRspNoNamespace =
    BoundTreeRspError("Namespaces not found", BoundTreeRsp.Error.Code.NOT_FOUND)

  private[this] val BoundTreeRspNoName =
    BoundTreeRspError("No name given", BoundTreeRsp.Error.Code.BAD_REQUEST)

  private[this] val mkBoundNameTree: NameTree[Name.Bound] => BoundNameTree = { tree =>
    val ptree = tree match {
      case NameTree.Neg =>
        BoundNameTree.OneofNode.Nop(BoundNameTree.Nop.NEG)

      case NameTree.Fail =>
        BoundNameTree.OneofNode.Nop(BoundNameTree.Nop.FAIL)

      case NameTree.Empty =>
        BoundNameTree.OneofNode.Nop(BoundNameTree.Nop.EMPTY)

      case NameTree.Leaf(name) =>
        name.id match {
          case id: Path =>
            val leaf = BoundNameTree.Leaf(Some(toProto(id)), Some(toProto(name.path)))
            BoundNameTree.OneofNode.Leaf(leaf)

          case _ =>
            BoundNameTree.OneofNode.Nop(BoundNameTree.Nop.NEG)
        }

      case NameTree.Alt(trees@_*) =>
        BoundNameTree.OneofNode.Alt(BoundNameTree.Alt(trees.map(mkBoundNameTree)))

      case NameTree.Union(trees@_*) =>
        BoundNameTree.OneofNode.Union(BoundNameTree.Union(trees.map(mkBoundWeightedTree)))
    }
    BoundNameTree(Some(ptree))
  }

  private[this] val mkBoundWeightedTree: NameTree.Weighted[Name.Bound] => BoundNameTree.Union.Weighted =
    wt => BoundNameTree.Union.Weighted(Some(wt.weight), Some(mkBoundNameTree(wt.tree)))

  private[this] val mkBoundTreeRsp: NameTree[Name.Bound] => BoundTreeRsp =
    t => BoundTreeRsp(Some(BoundTreeRsp.OneofResult.Tree(mkBoundNameTree(t))))

  private[this] val mkBoundTreeRspEv: Try[NameTree[Name.Bound]] => EventStream.Ev[BoundTreeRsp] = {
    case Return(tree) => EventStream.Val(mkBoundTreeRsp(tree))
    case Throw(e) => EventStream.End(Throw(e))
  }
}