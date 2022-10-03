package it.unibo.pcd.akka.cluster.advanced

import akka.actor.typed.receptionist.ServiceKey
import akka.actor.typed.ActorRef
import akka.actor.typed.receptionist.Receptionist
import akka.actor.typed.scaladsl.*
import akka.actor.typed.scaladsl.adapter.*
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import it.unibo.pcd.akka.Message
import concurrent.duration.DurationInt

import scala.language.postfixOps

object AntsRender:
  // Constants
  val width = 800
  val height = 600
  sealed trait Command
  final case class Render(x: Int, y: Int, id: ActorRef[_]) extends Message with Command
  private case object Flush extends Command // Private message (similar to private method in OOP)
  val Service = ServiceKey[Render]("RenderService") // Generica in un tipo che è il tipo di messaggio che può gestire l'attore che voglio registrare.
  def apply(): Behavior[Command] = {
    Behaviors.setup { ctx =>
      val frontendGui = SimpleGUI(width, height) // init the gui
      Behaviors.withTimers { timers =>
        timers.startTimerAtFixedRate(Flush, 33 milliseconds)
        var toRender: Map[ActorRef[_], (Int, Int)] = Map.empty
        ctx.system.receptionist ! Receptionist.Register(Service, ctx.self) // d'ora in poi gli altri attori possono trovarmi come servizio.
        Behaviors.receiveMessage {
          case Render(x, y, id) =>
            ctx.log.info(s"render.. $id")
            toRender = toRender + (id -> (x, y))
            frontendGui.render(toRender.values.toList)
            Behaviors.same

          case Flush =>
            frontendGui.render(toRender.values.toList)
            Behaviors.same
        }
      }
    }
  }
