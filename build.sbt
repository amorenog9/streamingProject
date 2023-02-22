ThisBuild / resolvers ++= Seq(
  "Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/",
  Resolver.mavenLocal
)

name := "flinkProject"

version := "0.1"

organization := "es.upm.dit"

ThisBuild / scalaVersion := "2.11.12"

val flinkVersion = "1.12.1"
val sparkVersion = "2.4.8"
val deltaVersion = "0.6.1"

val Dependencies = Seq(
  // Flink
  "org.apache.flink" %% "flink-runtime-web" % flinkVersion, //interfaz web
  "org.apache.flink" %% "flink-clients" % flinkVersion,
  "org.apache.flink" %% "flink-scala" % flinkVersion,
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion,
  "org.apache.flink" %% "flink-connector-kafka" % flinkVersion,
  "org.apache.flink" % "flink-json" % flinkVersion,
  //"ch.qos.logback" % "logback-classic" % "1.2.3",


  // Circe
  "io.circe" %% "circe-core" % "0.12.0-M3",
  "io.circe" %% "circe-generic" % "0.12.0-M3",
  "io.circe" %% "circe-parser" % "0.11.1",

  // config
  "com.typesafe" % "config" % "1.4.2",

  //Spark
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,

  //Delta
  "io.delta" %% "delta-core" % deltaVersion



)
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _                        => MergeStrategy.first
}



lazy val root = (project in file(".")).
  settings(
    libraryDependencies ++= Dependencies
  )

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _                        => MergeStrategy.first
}

assembly / mainClass := Some("es.upm.dit.Job")

