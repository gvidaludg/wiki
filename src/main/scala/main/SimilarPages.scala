package main

import mapreduce.MapReduce
import scala.collection.{BitSet, mutable}

case class SimilarPages(wiki: Map[String, WikiContents], wordcount: Int) {

    def computeAppearances(subset: Map[String, WikiContents]): Map[String, Int] = {

        val keys = subset.keys.toList
        val appearances = MapReduce.groupMapReduce(
            keys.indices,
            (i: Int) => {
                val words = subset(keys(i)).freqs.keys

                (i until keys.length).flatMap(j => {
                    val freqs = subset(keys(j)).freqs
                    words.filter(word => freqs.contains(word))
                        .map(word => (word, j))
                })
            },
            (keyval: (String, Int)) => {
                val (word, _) = keyval
                word
            },
            (keyval: (String, Int)) => {
                val (_, index) = keyval
                BitSet() + index
            },
            (lhs: BitSet, rhs: BitSet) => lhs.concat(rhs)
        )

        return appearances.view.mapValues(bits => bits.count(_ => true)).toMap
    }

    def topNonReferenced(query: String, limit: Int = 100): List[(String, String)] = {

        val documents = QueryDocument(wiki, wordcount).query(query)
            .slice(0, limit)

        val subset = documents.view
            .map(document => (document, wiki(document)))
            .to(mutable.Map)

        val appearances = computeAppearances(subset.toMap)

        val docs_idf = subset.view.map(keyval => {
            val (title, contents) = keyval
            val freqs = contents.freqs.map(keyval => {
                val (word, freq) = keyval
                val count = appearances(word)
                val idf = subset.size.toDouble / count
                (word, freq * idf)
            })
            (title, freqs)
        }).toMap

        val similarPages = MapReduce.groupMapReduce(
            docs_idf,
            (fst: (String, Map[String, Double])) => {
                val (fst_title, _) = fst
                val fst_refs = subset(fst_title).refs
                docs_idf.dropWhile(sec => {
                    val (sec_title, _) = sec
                    sec_title != fst_title
                }).drop(1)
                    .filter(sec => {
                        val (sec_title, _) = sec

                        if (fst_refs.contains(sec_title)) {
                            false
                        } else {
                            val sec_refs = subset(sec_title).refs
                            !sec_refs.contains(fst_title)
                        }
                    })
                    .map(sec => (fst, sec))
            },
            (pair: ((String, Map[String, Double]), (String, Map[String, Double]))) => {
                val (fst, sec) = pair
                val (fst_title, _) = fst
                val (sec_title, _) = sec
                (fst_title, sec_title)
            },
            (pair: ((String, Map[String, Double]), (String, Map[String, Double]))) => {
                val (fst, sec) = pair
                val (_, fst_freqs) = fst
                val (_, sec_freqs) = sec

                Cosinesim.simil(fst_freqs, sec_freqs)
            },
            (_: Double, _: Double) => throw new Exception()
        )

        val sorted = similarPages.toList
            .sortBy(keyval => {
                val (_, simil) = keyval
                -simil
            })

        return sorted.map(keyval => {
            val (docs, _) = keyval
            docs
        })
    }

}
