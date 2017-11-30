package company.evo.elasticsearch

import java.util.concurrent.FutureTask
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicInteger

import kotlin.concurrent.thread

import io.searchbox.client.JestClient

import org.slf4j.LoggerFactory

import company.evo.kafka.Timeout


internal class Sink(
        private val esClient: JestClient,
        private val bulkSize: Int,
        queueSize: Int,
        maxInFlightRequests: Int,
        delayBeetweenRequests: Long,
        heartbeatIntervalMs: Long,
        private val retryIntervalMs: Long,
        private val maxRetryIntervalMs: Long
)
{
    private val heartbeat = Heartbeat(esClient, heartbeatIntervalMs)
    private val heartbeatThread: Thread
    private val sinkContexts: List<SinkWorker.Context>
    private val sinkThreads: Collection<Thread>
    private val retryingCount = AtomicInteger(0)
    private val tasks = ArrayList<FutureTask<Boolean>>(queueSize * maxInFlightRequests)

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
                    retryIntervalMs,
                    maxRetryIntervalMs,
                    retryingCount
            )
            context to thread(name = "elastic-sink-$it") {
                val timeout = Timeout(delayBeetweenRequests)
                while (!Thread.interrupted()) {
                    try {
                        val task = context.takeTask()
                        if (delayBeetweenRequests > 0) {
                            val sleepDelay = timeout.drift()
                            if (sleepDelay > 0) {
                                logger.trace("Falling asleep for {} ms ...", sleepDelay)
                                Thread.sleep(sleepDelay)
                            }
                        }
                        task.run()
                        timeout.reset()
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

    fun flush(timeout: Timeout): Boolean {
        val pendingBulksCount = sinkContexts.map { it.pendingBulksCount() }.sum()
        val pendingTasksCount = tasks.count { !it.isDone }
        if (pendingBulksCount > 0 || pendingTasksCount > 0) {
            logger.debug("Flushing $pendingBulksCount pending bulks and $pendingTasksCount tasks")
        }
        try {
            // try to flush all pending bulks
            sinkContexts.forEach { ctx ->
                val flushResult = ctx.flush(timeout)
                tasks.addAll(flushResult.tasks)
                if (flushResult.isTimedOut) {
                    return false
                }
            }
            // and wait all tasks finished
            tasks.forEach { task ->
                task.get(timeout.driftOrFail(), TimeUnit.MILLISECONDS)
            }
        } catch (e: TimeoutException) {
            return false
        }
        return true
    }

    fun waitingElastic(): Boolean {
        return heartbeat.isWaitingElastic() || retryingCount.get() > 0
    }

    fun put(action: AnyBulkableAction, hash: Int, paused: Boolean, timeout: Timeout): Boolean {
        val sinkIx = Math.abs(hash) % sinkContexts.size
        val res = sinkContexts[sinkIx].addAction(action, paused, timeout)
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
