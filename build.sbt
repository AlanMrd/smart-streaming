name := "smartstreaming"
version := "0.1"
scalaVersion := "2.11.8"

val versaoSpark = "2.2.0.2.6.4.0-91"
val versaoKafka = "0.10.1.2.6.4.0-91"
val versaoHBase = "1.1.2.2.6.4.0-91"

mainClass in assembly := some("br.com.stream.main.StreamApp")
assemblyJarName := "stream_app.jar"

libraryDependencies += "org.apache.hbase" % "hbase-client" % versaoHBase exclude("org.apache.hadoop", "hadoop-aws")
libraryDependencies += "org.apache.hbase" % "hbase-common" % versaoHBase 
libraryDependencies += "org.apache.hbase" % "hbase" % versaoHBase
libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.7.3.2.6.4.0-91"
libraryDependencies += "net.liftweb" %% "lift-json" % "3.1.0-M2"
libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.9.9"
libraryDependencies += "org.mongodb.spark" %% "mongo-spark-connector" % "2.4.0"
libraryDependencies += "com.google.code.gson" % "gson" % "2.8.5"

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.3.3",
  "org.apache.spark" %% "spark-core" % versaoSpark,
  "org.apache.spark" %% "spark-sql" % "2.1.0" % "provided",
  "org.apache.spark" %% "spark-hive" % versaoSpark,
  "org.apache.kafka" %% "kafka" % "0.10.1.2.6.4.0-91",
  "org.apache.spark" %% "spark-streaming" % versaoSpark,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % versaoSpark
)

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test"

resolvers ++= Seq(
  "ViaVarejo" at "http://nexus.viavarejo.com.br/repository/hortonworks-group/",
  "HortonWorks" at "http://repo.hortonworks.com/content/repositories/releases/"
  )

val meta = """META.INF(.)*""".r
assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case n if n.startsWith("reference.conf") => MergeStrategy.concat
  case n if n.endsWith(".conf") => MergeStrategy.concat
  case meta(_) => MergeStrategy.discard
  case x => MergeStrategy.first
}
