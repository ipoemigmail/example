package example

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object Main {
  final val servers = Seq(
    "localhost:9092"
    //"alphakafka.iwilab.com:9092"
    //"kafka01.iwilab.com:9092"
  )

  final val groupId = "test-spark-streaming-kafka"

  final val topicName = "test"

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setMaster("local[*]")

    val sparkSession: SparkSession = SparkSession
      .builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()

    val sc: SparkContext = sparkSession.sparkContext
    val ssc              = new StreamingContext(sc, Duration(5 * 1000))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers"  -> servers.mkString(","),
      "key.deserializer"   -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id"           -> groupId,
      "auto.offset.reset"  -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array(topicName)
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    stream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      if (offsetRanges.nonEmpty) {
        rdd
          .map(x => s"${x.partition()} ==> ${x.value}")
          .collect()
          .foreach(println)
        stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      }
    }

    ssc.start()

    ssc.awaitTermination()

  }
}
