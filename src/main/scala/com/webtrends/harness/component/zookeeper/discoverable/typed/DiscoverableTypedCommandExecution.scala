package com.webtrends.harness.component.zookeeper.discoverable.typed

import akka.actor.{ActorContext, ActorRef, ActorSystem}
import akka.util.Timeout
import akka.pattern._
import com.webtrends.harness.HarnessConstants
import com.webtrends.harness.command.typed.{ExecuteTypedCommand}
import com.webtrends.harness.component.zookeeper.discoverable.{DiscoverableService}

import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import com.webtrends.harness.utils.FutureExtensions._

class DiscoverableTypedCommandExecution[T, V: ClassTag](name: String, args: T)(implicit context: ActorContext) {

  implicit val timeout: Timeout = 10 seconds
  implicit val system: ActorSystem = context.system
  implicit val ec: ExecutionContext = context.dispatcher
  implicit val service = DiscoverableService()

  def execute(discoveryPath: String): Future[V] = {
    getRemoteActorRef(s"$discoveryPath/typed").flatMapAll[V] {
      case Success(commandActor) =>
        (commandActor ? ExecuteTypedCommand(args)).map(_.asInstanceOf[V])
      case Failure(f) =>
        Future.failed(new IllegalArgumentException(s"Unable to find remote actor for command $name at $discoveryPath.", f))
    }
  }

  private def getRemoteActorRef(discoveryPath: String): Future[ActorRef] = {
    service.getInstance(discoveryPath, name).flatMap { r =>
      context.actorSelection(remotePath(r.getAddress, r.getPort)).resolveOne()
    }
  }

  private def remotePath(server: String, port: Int): String = {
    s"akka.tcp://server@$server:$port${HarnessConstants.TypedCommandFullName}/$name"
  }
}
