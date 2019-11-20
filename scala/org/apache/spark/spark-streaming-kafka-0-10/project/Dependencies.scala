import sbt._

object Versions {
  lazy val spark = "2.4.0"
}

object Dependencies {
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.5"
  lazy val spark = Seq(
    "org.apache.spark" %% "spark-yarn" % Versions.spark,
    "org.apache.spark" %% "spark-core" % Versions.spark,
    "org.apache.spark" %% "spark-sql" % Versions.spark,
    "org.apache.spark" %% "spark-mllib" % Versions.spark,
    "org.apache.spark" %% "spark-streaming" % Versions.spark,
    "org.apache.spark" %% "spark-hive" % Versions.spark,
    "org.apache.spark" %% "spark-streaming-kafka-0-10" % Versions.spark
  )
}
