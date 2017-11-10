package company.evo.elasticsearch

import java.util.Random
import java.util.concurrent.atomic.AtomicInteger

import kotlin.concurrent.thread

import io.searchbox.client.JestClient

import org.slf4j.LoggerFactory
import java.util.concurrent.FutureTask
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException


internal class Sink(
        private val esClient: JestClient,
        private val bulkSize: Int,
        queueSize: Int,
        maxInFlightRequests: Int,
        heartbeatInterval: Int,
        private val retryInterval: Int,
        private val maxRetryInterval: Int
)
{
    private val heartbeat = Heartbeat(esClient, heartbeatInterval)
    private val heartbeatThread: Thread
    private val sinkContexts: List<SinkWorker.Context>
    private val sinkThreads: Collection<Thread>
    private val retryingCount = AtomicInteger(0)
    private val tasks = ArrayList<FutureTask<Boolean>>(queueSize * maxInFlightRequests)

    private val randomHashes = Random().ints().iterator()

    companion object {
        private val logger = LoggerFactory.getLogger(Sink::class.java)
    }

    init {
        heartbeatThread = Thread(
                heartbeat,
                "elastic-heartbeat"
        )
        heartbeatThread.start()
        val sinks = (0 until maxInFlightRequests).map {
            val context = SinkWorker.Context(
                    heartbeat,
                    esClient,
                    bulkSize,
                    queueSize,
                    retryInterval,
                    maxRetryInterval,
                    retryingCount
            )
            context to thread(name = "elastic-sink-$it") {
                while (!Thread.interrupted()) {
                    try {
                        val task = context.takeTask()
                        task.run()
                    } catch (e: InterruptedException) {}
                }
            }
        }.unzip()
        sinkContexts = sinks.first
        sinkThreads = sinks.second
        logger.info("Started ${sinkThreads.size} sink threads")
    }

    fun close () {
        heartbeatThread.interrupt()
        sinkThreads.forEach { it.interrupt() }
        tasks.clear()
        retryingCount.set(0)
    }

    fun flush(timeoutMs: Int): Boolean {
        val pendingBulksCount = sinkContexts.map { it.pendingBulksCount() }.sum()
        val pendingTasksCount = tasks.count { !it.isDone }
        logger.debug("Flushing $pendingBulksCount pending bulks and $pendingTasksCount tasks")
        // TODO(Drift timeout)
        // try to flush all pending bulks
        sinkContexts.forEach { ctx ->
            val flushResult = ctx.flush(timeoutMs)
            tasks.addAll(flushResult.tasks)
            if (flushResult.isTimedOut) {
                return false
            }
        }
        try {
            // and wait all tasks finished
            tasks.forEach { task ->
                task.get(timeoutMs.toLong(), TimeUnit.MILLISECONDS)
            }
        } catch (e: TimeoutException) {
            return false
        }
        return true
    }

    fun waitingElastic(): Boolean {
        return heartbeat.isWaitingElastic() || retryingCount.get() > 0
    }

    fun put(action: AnyBulkableAction, hash: Int?, paused: Boolean, timeoutMs: Int): Boolean {
        val sinkIx = (hash ?: randomHashes.nextInt()) % sinkContexts.size
        val res = sinkContexts[sinkIx].addAction(action, paused, timeoutMs)
        return when (res) {
            is SinkWorker.Context.AddActionResult.Ok -> true
            is SinkWorker.Context.AddActionResult.Timeout -> false
            is SinkWorker.Context.AddActionResult.Task -> {
                tasks.add(res.task)
                true
            }
        }
    }
}
