package queue

import akka.actor.{Actor,ActorRef, ActorSystem, Props}
import scalaz.stream.async.mutable.Queue
import scalaz.stream.{async,Cause,io,Process,Sink}
import scalaz.concurrent.Task
import java.util.concurrent.atomic.AtomicBoolean

sealed trait Messages
/** A string to enqueue */
case class Str(s: String) extends Messages
/** A signal to terminate the process normally */
case object End extends Messages
/** A signal to fail the process with an error */
case object AbEnd extends Messages

class EnqueueActor(queue: Queue[String]) extends Actor {
  def receive: Receive = {
    case Str(s) =>
      // add the string to the queue
      val enq: Task[Unit] = queue.enqueueOne(s)
      enq.run

    case End =>
      // close the queue which will halt the Process normally
      val close: Task[Unit] = queue.close
      close.run

    case AbEnd =>
      // fail the queue which will halt the Process with an error
      val fail: Task[Unit] = queue.fail(new Exception("fail"))
      fail.run
  }
}

class ConsoleInput(queue: Queue[String]) extends Runnable {
  val system = ActorSystem("queue-demo")

  // a Sink which will pass messages to our akka actor
  def toActor(recv: ActorRef): Sink[Task,String] = io.channel { str =>
    str match {
      case "bye" => Task.delay {
        recv ! End
        throw Cause.Terminated(Cause.End)
      }
      case "die" => Task.delay {
        recv ! AbEnd
        throw Cause.Terminated(Cause.End)
      }
      case x => Task.delay {
        recv ! Str(x)
      }
    }
  }

  override def run(): Unit = {
    val actor = system.actorOf(Props(classOf[EnqueueActor], queue))
    (io.stdInLines to toActor(actor)).run.run
    system.shutdown()
  }
}

object QueueDemo extends App {
  val queue: Queue[String] = async.unboundedQueue[String]
  val strings: Process[Task,String] = queue.dequeue

  val t = new Thread(new ConsoleInput(queue))
  t.start()

  val counted = strings map (str => s"${str.length} chars")
  (counted to io.printLines(System.out)).run.run

  t.join()
}
