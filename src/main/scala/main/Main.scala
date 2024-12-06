package main

import main.Cosinesim.{freq, ngrames}
import mapreduce.MapReduce
import mapreduce.ViquipediaParse.{ResultViquipediaParsing, parseViquipediaFile}

import java.nio.file.{FileSystems, Files}
import scala.jdk.javaapi.CollectionConverters.asScala

object Main extends App {

    val wordcount = 1
    val dir = FileSystems.getDefault.getPath ("viqui_files")
    val docs = asScala(Files.list(dir).iterator()).to(Iterable).map(_.toString)

    val wiki = TimeMeasurement.timeMeasurement(_ => MapReduce.groupMapReduce (
        docs,
        (filename: String) => Iterable (parseViquipediaFile (filename) ),
        (file: ResultViquipediaParsing) => file.titol,
        (file: ResultViquipediaParsing) => WikiContents (
            freq (ngrames (Iterable (file.contingut), wordcount) ),
            file.refs.map (ref => ref.substring (2, ref.length - 2) ).toSet
        ),
        (_: WikiContents, _: WikiContents) => throw new Exception ("Non-unique values!"),
    ), "File fetching")

    val avgReferences = wiki.values.map (contents => contents.refs.size).sum / wiki.size
    println(s"$avgReferences references on average.")

    println(TimeMeasurement.timeMeasurement(_ => QueryDocument (wiki, wordcount).query ("Guerra"), "Query"))
    println(TimeMeasurement.timeMeasurement(_ => SimilarPages (wiki, wordcount).topNonReferenced ("Guerra"), "Top similar"))

}