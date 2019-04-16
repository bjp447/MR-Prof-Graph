name := "cs441_hw2"
version := "0.1"
scalaVersion := "2.11.0"
scalacOptions += "-target:jvm-1.7"
assemblyJarName in assembly := "hw2.jar"

mainClass in (Compile, run) := Some("hw2")         //sbt run
mainClass in (Compile, packageBin) := Some("hw2")  //sbt runmain

autoScalaLibrary := false  //runs a pure java program

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"         //unit testing

//https://github.com/scala/scala-xml
libraryDependencies += "org.scala-lang.modules" % "scala-xml_2.11" % "1.0.1"      //scala.xml

//libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.1"    //hadoop mapReduce core
libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "2.7.1"
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.1"