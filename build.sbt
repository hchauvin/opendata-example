name := "opendata"

version := "0.1"

scalaVersion := "2.12.11"

libraryDependencies ++= Seq(
  "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2",
  "io.projectreactor" %% "reactor-scala-extensions" % "0.5.1",
  "io.projectreactor.netty" % "reactor-netty" % "0.9.0.RELEASE",
  "io.projectreactor" % "reactor-core" % "3.3.3.RELEASE",
  "io.netty" % "netty-transport-native-kqueue" % "4.1.42.Final",
  "io.netty" % "netty-all" % "4.1.48.Final",
  "org.apache.spark" %% "spark-core" % "3.0.0-preview2",
  "org.apache.spark" %% "spark-sql" % "3.0.0-preview2"
)

libraryDependencies += "org.scalatest" %% "scalatest" % "3.1.1" % "test"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

test in assembly := {}