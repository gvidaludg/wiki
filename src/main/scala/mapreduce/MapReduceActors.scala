package mapreduce

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration.{Duration, DurationInt}
import scala.language.postfixOps

case class executeMapReduce[A](input: Iterable[A])
case class executeMapping[A](input: A)
case class totalMappings(count: Int)
case class doneMapping[K, C](mappings: Iterable[(K, C)])
case class doneReducing[K, C](mappings: mutable.Map[K, C])
case class noMoreWork(workcount: Int)

class Mapper[K, A, B, C](compute: A => Iterable[B], keyMapper: B => K, valueMapper: B => C) extends Actor {
    def receive: Receive = {
        case executeMapping(input: A) =>
            var count = 0
            compute(input).foreach(value => { // foreach perquè volem que el processat el faci el Mapper, no el Reducer.
                sender ! doneMapping(Iterable((keyMapper(value), valueMapper(value))))
                count += 1
            })
            sender ! totalMappings(count)
    }
}

abstract class Reducer[K, A, B, C](reducer: (C, C) => C) extends Actor {

    protected var client: ActorRef = null
    protected var reduced: mutable.Map[K, C] = mutable.Map[K, C]()

    def processMapReduce(input: Iterable[A]): Unit
    def isFinished: Boolean
    def sendResult(reduced: mutable.Map[K, C]): Unit

    protected def checkFinished(): Unit = {

        if (isFinished) {
            sendResult(reduced)
        }

    }

    def done(mappings: Iterable[(K, C)]): Unit = {

        mappings.foreach(keyval => {
            val (key, mapped) = keyval

            val newValue = reduced.get(key) match {
                case Some(value) => reducer(value, mapped)
                case None => mapped
            }

            reduced.put(key, newValue)
        })

        checkFinished()

    }

    override def receive: Receive = {

        case executeMapReduce(input: Iterable[A]) =>
            client = sender()
            processMapReduce(input)

        case doneMapping(mappings: Iterable[(K, C)]) =>
            done(mappings)

    }

}

class NodeReducer[K, A, B, C](
                                 compute: A => Iterable[B], // Mapeig extremadament redundant: aquesta classe no té sentit sense un ecosistema d'Iteradors paral·lelitzables o amb spliterators.
                                 keyMapper: B => K,
                                 valueMapper: B => C,
                                 reducer: (C, C) => C,
                                 mappers: Seq[ActorRef]
                             ) extends Reducer[K, A, B, C](reducer) {

    var offset = 0

    var remainingProcesses = 0
    var totalMappings = 0
    var currentMappings = 0
    var remainingWork = 0
    var noMoreWork = false

    override def processMapReduce(input: Iterable[A]): Unit = {

        for (inp <- input) {
            mappers(offset % mappers.length) ! executeMapping(inp)
            remainingProcesses += 1
            offset += 1
        }

        remainingWork -= 1

    }

    override def isFinished: Boolean = noMoreWork && remainingWork == 0 && remainingProcesses == 0 && totalMappings == currentMappings

    override def done(mappings: Iterable[(K, C)]): Unit = {
        currentMappings += 1
        super.done(mappings)
    }

    override def receive: Receive = super.receive orElse {

        case totalMappings(count) =>

            remainingProcesses -= 1
            totalMappings += count

            checkFinished()

        case noMoreWork(workcount: Int) =>
            noMoreWork = true
            remainingWork += workcount
            checkFinished()

    }

    override def sendResult(reduced: mutable.Map[K, C]): Unit =
        client ! doneMapping(reduced)

}

class RootReducer[K, A, B, C](
                                 compute: A => Iterable[B], // Mapeig extremadament redundant: aquesta classe no té sentit sense un ecosistema d'Iteradors paral·lelitzables o amb spliterators.
                                 keyMapper: B => K,
                                 valueMapper: B => C,
                                 reducer: (C, C) => C,
                                 nmappers: Int,
                                 nreducers: Int
                             ) extends Reducer[K, A, B, C](reducer) {

    val mappers: Seq[ActorRef] = for (i <- 0 until nmappers) yield {
        context.actorOf(Props(new Mapper(compute, keyMapper, valueMapper)), "mapper" + i)
    }

    val nodeReducers: Seq[ActorRef] = for (i <- 0 until nreducers) yield {
        context.actorOf(Props(new NodeReducer(compute, keyMapper, valueMapper, reducer, mappers)), "reducer" + i)
    }

    var offset = 0

    var nodeReducersRemaining = 0

    override def processMapReduce(input: Iterable[A]): Unit = {

        val initial = offset

        val workcounts = nodeReducers.indices.map(_ => 0).to(mutable.Seq)

        for (inp <- input) {
            val index = offset % nodeReducers.size
            nodeReducers(index) ! executeMapReduce(Iterable(inp))
            workcounts(index) += 1
            offset += 1
        }

        nodeReducersRemaining = nodeReducers.size
        for ((workcount, reducer) <- workcounts.zip(nodeReducers)) {
            reducer ! noMoreWork(workcount)
        }

    }

    override def isFinished: Boolean = nodeReducersRemaining == 0

    override def done(mappings: Iterable[(K, C)]): Unit = {
        nodeReducersRemaining -= 1
        super.done(mappings)
    }

    override def sendResult(reduced: mutable.Map[K, C]): Unit =
        client ! doneReducing(reduced)

}

object MapReduce {

    var nmappers: Int = 16
    var nreducers: Int = 16

    def setCounters(nmappers: Int, nreducers: Int): Unit = {
        this.nmappers = nmappers
        this.nreducers = nreducers
    }

    def groupMapReduce[K, A, B, C](
                                   input: Iterable[A],
                                   compute: A => Iterable[B],
                                   key: B => K,
                                   mapper: B => C,
                                   reducer: (C, C) => C,
    ): Map[K, C] = {

        val system = ActorSystem("map-reduce-system")
        val mapReducer = system.actorOf(Props(new RootReducer(compute, key, mapper, reducer, nmappers, nreducers)))

        implicit val timeout: Timeout = Timeout(10000 seconds)
        val future = mapReducer ? executeMapReduce(input)

        val result = Await.result(future, Duration.Inf).asInstanceOf[doneReducing[K, C]]

        system.terminate()

        return result.mappings.toMap
    }

}
