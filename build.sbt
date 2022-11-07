ThisBuild / resolvers ++= Seq(
  "Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/",
  Resolver.mavenLocal
)

name := "flinkProject"

version := "0.1"

organization := "es.upm.dit"

ThisBuild / scalaVersion := "2.11.12"

val flinkVersion = "1.14.0"

val flinkDependencies = Seq(
  "org.apache.flink" %% "flink-runtime-web" % flinkVersion, //añadido para interfaz web
  "org.apache.flink" %% "flink-clients" % flinkVersion % "compile", // cambiado a compile
  "org.apache.flink" %% "flink-scala" % flinkVersion % "compile", // cambiado a compile
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "compile", // cambiado a compile
  "org.apache.flink" %% "flink-connector-kafka" % flinkVersion,
  "org.apache.flink" % "flink-json" % flinkVersion,

  "ch.qos.logback" % "logback-classic" % "1.2.3"
) // unique % to avoid appending scala version

lazy val root = (project in file(".")).
  settings(
    libraryDependencies ++= flinkDependencies
  )

assembly / mainClass := Some("es.upm.dit.Job")

// make run command include the provided dependencies
Compile / run  := Defaults.runTask(Compile / fullClasspath,
  Compile / run / mainClass,
  Compile / run / runner
).evaluated

// stays inside the sbt console when we press "ctrl-c" while a Flink programme executes with "run" or "runMain"
Compile / run / fork := true
Global / cancelable := true

// exclude Scala library from assembly
assembly / assemblyOption  := (assembly / assemblyOption).value.copy(includeScala = false)