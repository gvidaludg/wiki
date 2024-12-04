scalaVersion := "2.13.1"

name := "Viquipedia Simil"
organization := "UdG-GEINF-PDA"
version := "1.0"

libraryDependencies += "org.scala-lang.modules" %% "scala-xml" % "1.3.0"
libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.6.10"
// per a poder convertir InputStreams a String
libraryDependencies += "commons-io" % "commons-io" % "2.8.0"