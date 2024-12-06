package main

object TimeMeasurement {

    def timeMeasurement[A](compute: Unit => A, name: String = "Function"): A = {

        val beg = System.currentTimeMillis()

        val ret = compute()

        val end = System.currentTimeMillis()

        val elapsed = (end - beg).toFloat / 1000;
        println(s"$name took $elapsed seconds!")

        return ret
    }

}
