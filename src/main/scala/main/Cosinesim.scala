package main

import scala.collection.View
import scala.collection.immutable.SortedMap
import scala.io.Source.fromFile

object Cosinesim {

    private val stopwords = fromFile("primeraPartPractica/catalan-stop.txt").getLines().toSet;

    def words(contents: String): View[String] =
        contents.view
            .map(char => if (char.isLetterOrDigit) {
                char
            } else {
                ' '
            }) // Substituïm per espais els caràcters que no són alfanumèrics
            .mkString // Construïm una string de la cadena de caràcters resultant
            .split("\\s").view // Dividim la string per espais
            .filter(str => str.nonEmpty) // Esborrem de la iteració les strings buides
            .map(str => str.toLowerCase) // Passem les strings que queden a minúscules

    def freq(sentences: Iterable[String]): Map[String, Int] =
        sentences.groupMapReduce(identity)(_ => 1)(_ + _) // Agrupem les frases resultants en un mapa d'aparicions

    def nonstopwords(words: Iterable[String], stopwords: Set[String]): Iterable[String] =
        words.filter(word => !stopwords.contains(word)) // Filtrem les paraules que siguin stopwords

    def nonstopfreq(words: Iterable[String], stopwords: Set[String]): Map[String, Int] =
        freq(nonstopwords(words, stopwords))

    def showFrequencies(list: List[(String, Int)]): Unit = {

        val freqs = list.sortBy(el => el._2)
        val total = freqs.map(el => el._2).sum
        val diff = freqs.size

        println(f"Num de Paraules:\t$total\tDiferents:\t$diff")
        println("Paraules\tOcurrències\tFreqüència")
        println("-------------------------------")

        freqs.view
            .drop(freqs.size - 10)
            .toList
            .reverse
            .foreach(el => {
                val word = el._1
                val count = el._2
                val freq = count.toFloat / total * 100
                println(f"$word\t$count\t$freq")
            })

    }

    def paraulafreqfreq(lines: Iterable[String]): Unit = {

        val list = nonstopfreq(lines.flatMap(words), stopwords)
        val freqsFreq = list.values
            .groupMapReduce(identity)(_ => 1)(_ + _)
            .to(SortedMap)

        println("Les 10 freqüències més freqüents:")
        val top = freqsFreq.view.take(10)
        for ((k, v) <- top) {
            println(f"$v paraules apareixen $k vegades")
        }

        println("Les 5 freqüències menys freqüents:")
        val bot = freqsFreq.view.drop(freqsFreq.size - 5).toList.reverse
        for ((k, v) <- bot) {
            println(f"$v paraules apareixen $k vegades")
        }

    }

    def ngrames(lines: Iterable[String], wordcount: Int): Iterable[String] =
        nonstopwords(lines.flatMap(words), stopwords) // Filtrem per stopwords
            .sliding(wordcount) // [0, 1, 2, 3].sliding(2) = [[0, 1], [1, 2], [2, 3]]
            .map(iter => iter.mkString(" ")) // Formem una string separada per espais
            .to(Iterable)

    def simil(fstFreq: Map[String, Double], secFreq: Map[String, Double]): Double = {

        val fstMax = fstFreq.values.max
        val secMax = secFreq.values.max

        val fstExtended = fstFreq.view.concat(
            secFreq.keys.flatMap(word => fstFreq.get(word) match {
                case Some(_) => None
                case None => Some((word, 0.0))
            })
        ).toList

        val paired = fstExtended.map(el => el._2).zip(fstExtended.map(el => secFreq.get(el._1) match {
            case Some(freq) => freq
            case None => 0.0
        }))

        var dot = 0.0
        var aSqrLen = 0.0
        var bSqrLen = 0.0

        paired.foreach(pair => {
            val (f, s) = pair
            val (a, b) = (f / fstMax, s / secMax)

            dot += a * b
            aSqrLen += a * a
            bSqrLen += b * b
        })

        dot / (Math.sqrt(aSqrLen) * Math.sqrt(bSqrLen))
    }

}