package main

import main.Cosinesim.ngrames
import mapreduce.MapReduce

import scala.annotation.tailrec
import scala.collection.mutable

case class WikiContents(freqs: Map[String, Int], refs: Set[String])
case class RankInformation(rank: Double, refs: Set[String])

case class QueryDocument(wiki: Map[String, WikiContents], wordcount: Int) {

    final val brakefactor = 0.85
    final val tolerance = 1e-3

    @tailrec
    final def pagerank(ranking: Map[String, RankInformation], references: String => Set[String]): Map[String, RankInformation] = {

        val newRanking = MapReduce.groupMapReduce(
            ranking.values,
            (info: RankInformation) => info.refs.toIterable.map(ref => (ref, info.rank / info.refs.size)),
            (pair: (String, Double)) => {
                val (title, _) = pair
                title
            },
            (pair: (String, Double)) => {
                val (_, rank) = pair
                rank
            },
            (lhs: Double, rhs: Double) => lhs + rhs
        )

        val head = (1 - brakefactor).toDouble / newRanking.size
        var diff = 0.0

        val dampedRanking = newRanking.map(keyval => {
            val (title, rank) = keyval
            val dampedRank = head + brakefactor * rank

            diff += Math.abs(ranking(title).rank - dampedRank)
            (title, RankInformation(dampedRank, references(title)))
        })

        if (diff < tolerance) {
            return dampedRanking
        }

        return pagerank(dampedRanking, references)
    }

    def query(query: String): List[String] = {

        val keywords = ngrames(Iterable(query), wordcount).toList

        // Filtrem pels documents que contenen la query.
        val references = wiki.view.filter(keyval => {
            val (_, content) = keyval
            keywords.exists(keyword => content.freqs.contains(keyword))
        }).map(keyval => {
            val (title, content) = keyval
            (title, content.refs)
        }).to(mutable.Map)

        // Limitem les referències, perquè es quedin dins del subset.
        for ((page, refs) <- references)
            references.put(page, refs.filter(references.contains))

        val shared = 1.0 / references.size
        val initialRanking = references.view.map(keyval => {
            val (title, refs) = keyval
            (title, RankInformation(shared, refs))
        }).toMap

        val finalRanking = pagerank(initialRanking, references)

        finalRanking.view.map(keyval => {
            val (title, info) = keyval
            (title, info.rank)
        }).toList.sortBy(keyval => {
            val (_, rank) = keyval
            rank
        }).map((keyval) => {
            val (title, _) = keyval
            title
        })
    }

}
