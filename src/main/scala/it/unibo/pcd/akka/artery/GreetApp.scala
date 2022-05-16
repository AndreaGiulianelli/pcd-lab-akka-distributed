package it.unibo.pcd.akka.artery

import akka.actor.typed.scaladsl.*
import akka.actor.typed.scaladsl.adapter.*
import akka.actor.typed.{Behavior, ActorRef, ActorSystem}
import akka.actor.ActorSystem as ClassicActorSystem // Used for actorSelection
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import it.unibo.pcd.akka.Message
import it.unibo.pcd.akka.artery.GreetApp.*

import concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

import scala.concurrent.ExecutionContext.Implicits.global

import scala.language.postfixOps

// NB! Do not use akka artery, it is a low level library (indeed it does not support typed)
object GreetApp:
  case class Greet(whom: String, replyTo: ActorRef[Greeted]) extends Message // extends Required for serialization. Si può fare un trait vuoto da cui si estende.
  case class Greeted(whom: String, sender: ActorRef[Greet]) extends Message

  def greet(me: String): Behavior[Greet] = Behaviors.receive { case (ctx, Greet(whom, replyTo)) =>
    ctx.log.info(s"$whom at ${replyTo.path} greet to me!")
    replyTo ! Greeted(me, ctx.self)
    Behaviors.same
  }

  def greeted(): Behavior[Greeted] = Behaviors.receive { case (ctx, Greeted(whom, sender)) =>
    ctx.log.info(s"$whom at ${sender.path} has received my greet!")
    Behaviors.stopped
  }

def configFrom(port: Int): Config = ConfigFactory
  .parseString(s"""akka.remote.artery.canonical.port=$port""")
  .withFallback(ConfigFactory.load("base-remote"))

@main def alice(): Unit =
  val config = configFrom(8080)
  ActorSystem(greet("alice"), "foo", config)

@main def gianluca(): Unit =
  anyGreet("gianluca", 8081)

@main def anyGreet(who: String, port: Int): Unit =
  given Timeout = 2 seconds // required for actorSelection.
  val remoteReferencePath = "akka://foo@127.0.0.1:8080/user/"
  val config = configFrom(port)
  val system = ClassicActorSystem.apply("foo", config)
  val remoteReference = system.actorSelection(remoteReferencePath).resolveOne() // mi permette di, dato l'url di un attore, ottenerne un riferimento, restituisce una Future la quale viene risolta con la for comprehension.
  for remote <- remoteReference do
    val actor = system.spawn(greeted(), who) // Creo il mio attore
    remote ! GreetApp.Greet(who, actor) // Mando il saluto all'attore remoto
