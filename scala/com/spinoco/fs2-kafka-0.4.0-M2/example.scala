//import $ivy.`org.typelevel::cats-effect:1.0.0`
import $ivy.`com.spinoco::fs2-kafka:0.4.0-M2`

import java.util.concurrent.Executors
import java.nio.channels.AsynchronousChannelGroup

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext
import ExecutionContext.Implicits._

import cats._, cats.data._, cats.implicits._
import cats.effect._, cats.implicits._

import fs2.Scheduler
import spinoco.fs2.kafka
import spinoco.fs2.kafka._
import spinoco.protocol.kafka._
import shapeless.tag

class KafkaLogger[F[_]](implicit F: Effect[F]) extends Logger[F] {
  override def log(level: Logger.Level.Value,
                   msg: => String,
                   throwable: Throwable): F[Unit] =
    F.delay {
      println(s"LOGGER: $level: $msg")
      if (throwable != null) throwable.printStackTrace()
    }
}

implicit val _cxs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
implicit val _timer: Timer[IO] = IO.timer(ExecutionContext.global)
implicit val _concurrent: Concurrent[IO] = IO.ioConcurrentEffect(_cxs)
implicit val _scheduler: Scheduler = fs2.Scheduler.fromScheduledExecutorService(
  new java.util.concurrent.ScheduledThreadPoolExecutor(5))
implicit val AG: AsynchronousChannelGroup =
  AsynchronousChannelGroup.withThreadPool(Executors.newFixedThreadPool(8))
implicit val _kafkaLogger: Logger[IO] = new KafkaLogger[IO]()

val stream =
  kafka
    .client[IO](
      ensemble = Set(broker("kafka01.iwilab.com", port = 9092)),
      protocol = ProtocolVersion.Kafka_0_10_2,
      clientName = "my-client-name"
    )
    .flatMap { client =>
      client.leaderFor(1.seconds)(tag[TopicName]("story-log"))
    }
    .evalMap(x => IO(println(x)))
