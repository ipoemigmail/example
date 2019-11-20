package example

import cats.effect._
import cats.implicits._
import cats.{Applicative, Functor, Monad}
import fs2.kafka._
import fs2.{Pipe, Stream}
import org.apache.kafka.clients.consumer.{ConsumerRecord, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._
import scala.concurrent.duration._

object Main extends IOApp {
  final val servers = Seq(
    "localhost:9092"
    //"alphakafka.iwilab.com:9092"
    //"kafka01.iwilab.com:9092"
  )

  final val groupId = "fs2-kafka-test"

  final val topicName = "test"

  def processRecord[F[_]: Applicative](record: ConsumerRecord[String, String]): F[(String, String)] =
    (record.key -> record.value).pure[F]

  def nonAssigned[F[_]: Functor](consumer: KafkaConsumer[F, String, String]): F[Boolean] =
    consumer.assignment.map(_.isEmpty)

  def processStream[F[_]: Monad](stream: Stream[F, CommittableMessage[F, String, String]]): F[Stream[F, Unit]] =
    stream
      .evalMap { message =>
        for {
          tuple <- processRecord[F](message.record)
          _     <- println(s"${message.committableOffset.topicPartition} => $tuple").pure[F]
        } yield message.committableOffset
      }
      .through(commitBatch)
      .pure[F]

  def restoreOffset[F[_]: Monad: Timer](consumer: KafkaConsumer[F, String, String],
                                        offsetInfoMap: Map[TopicPartition, OffsetAndMetadata]): F[Unit] = {
    val seekPosition = for {
      (topicPartition, offsetInfo) <- offsetInfoMap.toList if topicPartition.topic() == topicName
    } yield consumer.seek(topicPartition, offsetInfo.offset())

    // kafka client 가 partition 에 assign 하기를 기다린다
    // KafkaConsumerActor 의 withConsumer 가 다른 Thread 에서 비동기로 돌기 때문에 Effect 를 이용해 동기화가 불가능함
    Timer[F].sleep(3.seconds).whileM_(nonAssigned(consumer)) >> seekPosition.sequence_
  }

  def printOffsetInfo[F[_]: Applicative: Concurrent](adminClientSettings: AdminClientSettings): Pipe[F, Any, Unit] = {
    s: Stream[F, _] =>
      s.flatMap { _ =>
        adminClientStream[F](adminClientSettings)
          .evalMap { x =>
            for {
              offsetAndMetadata <- x.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata
              _                 <- offsetAndMetadata.foreach(println).pure[F]
            } yield ()
          }
      }
  }

  override def run(args: List[String]): IO[ExitCode] = {
    val consumerSettings =
      ConsumerSettings[String, String]
        .withAutoOffsetReset(AutoOffsetReset.Earliest)
        .withBootstrapServers(servers.mkString(","))
        .withGroupId(groupId)

    val adminClientSettings =
      AdminClientSettings.Default
        .withBootstrapServers(servers.mkString(","))

    val stream = adminClientStream[IO](adminClientSettings)
      .evalMap { client =>
        for {
          offsetInfoMap <- client.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata
          topicDescMap  <- client.describeTopics(topicName.some)
          partitions = topicDescMap(topicName).partitions().asScala.toList
        } yield (partitions, offsetInfoMap)
      }
      .evalTap(x => IO(x._2.foreach(println)))
      .flatMap {
        case (partitions, offsetInfoMap) =>
          consumerStream[IO]
            .using(consumerSettings)
            .evalTap(_.subscribeTo(topicName))
            .evalTap(_ => IO(println("Offset Restore!!")))
            .evalTap(restoreOffset(_, offsetInfoMap))
            .evalTap(_ => IO(println("Streaming Start!!")))
            .flatMap(_.partitionedStream.mapAsync(partitions.size)(processStream[IO]).parJoinUnbounded)
      }
    //.through(printOffsetInfo[IO](adminClientSettings))

    stream.compile.drain.as(ExitCode.Success)
  }
}
