package io.buoyant.namerd
package iface

import com.fasterxml.jackson.annotation.JsonIgnore
import com.twitter.finagle.{Path, Namer, Service, Stack}
import com.twitter.finagle.buoyant.{H2, h2}
import com.twitter.finagle.naming.NameInterpreter
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.util.Duration
import com.twitter.util.TimeConversions._
import io.buoyant.grpc.runtime.ServerDispatcher
import java.net.InetSocketAddress

class GrpcConfig extends InterfaceConfig {
  @JsonIgnore
  override protected def defaultAddr = GrpcConfig.defaultAddr

  @JsonIgnore
  override def mk(
    store: DtabStore,
    namers: Map[Path, Namer],
    stats: StatsReceiver
  ): Servable = new Servable {
    def kind = GrpcConfig.kind
    def serve() = {
      val interpreter = GrpcInterpreter.mk(store, namers, stats)
      // val controller = GrpcController.mk(store)
      H2.serve(addr, ServerDispatcher(interpreter))
    }
  }
}

object GrpcConfig {
  val kind = "io.l5d.namerd.grpc"
  val defaultAddr = new InetSocketAddress(4321)
}

class GrpcInitializer extends InterfaceInitializer {
  override val configId = GrpcConfig.kind
  override val configClass = classOf[GrpcConfig]
}
