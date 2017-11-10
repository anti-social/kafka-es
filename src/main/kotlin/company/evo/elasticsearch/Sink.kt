package company.evo.elasticsearch

import java.io.IOException
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import kotlin.concurrent.thread

import io.searchbox.client.JestClient
import io.searchbox.core.Bulk
import io.searchbox.core.BulkResult

import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock


internal class Sink(
        private val esClient: JestClient,
        private val bulkSize: Int = Config.BULK_SIZE_DEFAULT,
        queueSize: Int = Config.QUEUE_SIZE_DEFAULT,
        private val queueTimeout: Int = Config.REQUEST_TIMEOUT_DEFAULT,
        maxInFlightRequests: Int = Config.MAX_IN_FLIGHT_REQUESTS_DEFAULT,
        heartbeatInterval: Int = Config.HEARTBEAT_INTERVAL_DEFAULT,
        private val retryInterval: Int = Config.RETRY_INTERVAL_DEFAULT,
        private val maxRetryInterval: Int = Config.MAX_RETRY_INTERVAL_DEFAULT
)
{
    private val queue: BlockingQueue<Collection<AnyBulkableAction>>
    private val confirmationQueue: BlockingQueue<String>
    private val sinkThreads: Collection<Thread>
    private val heartbeatThread: Thread
    private val heartbeatLock = ReentrantLock()
    private val elasticUnavailable = heartbeatLock.newCondition()
    private val elasticAvailable = heartbeatLock.newCondition()
    private val waitingElastic = AtomicBoolean(false)
    private val retrying = AtomicInteger(0)

    private val actions = ArrayList<AnyBulkableAction>()
    private var requestCounter = 0


    companion object {
        val logger = LoggerFactory.getLogger(Sink::class.java)

        // TODO(Review all the exceptions)
        val NON_RETRIABLE_ES_ERRORS = setOf(
                "elasticsearch_parse_exception",
                "parsing_exception",
                "routing_missing_exception"
        )
    }

    init {
        heartbeatThread = Thread(
                Heartbeat(esClient, heartbeatInterval,
                        heartbeatLock, elasticUnavailable, elasticAvailable,
                        waitingElastic),
                "elastic-heartbeat"
        )
        heartbeatThread.start()
        queue = ArrayBlockingQueue(queueSize)
        confirmationQueue = ArrayBlockingQueue(queueSize)
        // TODO(Use ThreadPoolExecutor)
        sinkThreads = (0 until maxInFlightRequests).map {
            thread(name = "elastic-sink-$it") {
                while (!Thread.interrupted()) {
                    try {
                        val actions = queue.take()
                        endlessSend(actions)
                        confirmationQueue.put("ok")
                    } catch (e: InterruptedException) {}
                }
            }
        }
    }

    fun stop () {
        heartbeatThread.interrupt()
        sinkThreads.forEach { it.interrupt() }
        waitingElastic.set(false)
        retrying.set(0)
        actions.clear()
        requestCounter = 0
    }

    fun flush(timeoutMs: Int): Boolean {
        if (actions.isNotEmpty()) {
            send(actions)
            actions.clear()
        }
        logger.debug("Flushing $requestCounter bulk requests")
        while (requestCounter > 0) {
            val confirm = confirmationQueue.poll(timeoutMs.toLong(), TimeUnit.MILLISECONDS)
            if (confirm == null) {
                return false
            } else {
                requestCounter -= 1
            }
        }
        return true
    }

    fun waitingElastic(): Boolean = waitingElastic.get() || retrying.get() > 0

    fun put(action: AnyBulkableAction) {
        actions.add(action)
        if (actions.size >= bulkSize) {
            send(actions)
            actions.clear()
        }
    }

    private fun send(actions: Collection<AnyBulkableAction>) {
        if (queue.offer(actions.toList(), queueTimeout.toLong(), TimeUnit.MILLISECONDS)) {
            requestCounter += 1
            logger.debug("Put ${actions.size} actions into queue")
        }
    }

    private fun endlessSend(acts: Collection<AnyBulkableAction>) {
        var actions = acts
        var retries = 0
        var wasRetries = false
        try {
            while (actions.size > 0) {
                if (retries > 0) {
                    if (retries == 1) {
                        wasRetries = true
                        retrying.incrementAndGet()
                    }
                    val retryInterval = minOf(
                            (retryInterval * Math.pow(2.0, retries.toDouble() - 1)).toInt(),
                            maxRetryInterval
                    )
                    Thread.sleep(retryInterval * 1000L)
                }
                try {
                    actions = sendBulk(actions)
                    retries += 1
                } catch (e: IOException) {
                    heartbeatLock.withLock {
                        waitingElastic.set(true)
                        elasticUnavailable.signal()
                        logger.info("Waiting for elasticsearch ...")
                        while (waitingElastic.get()) {
                            elasticAvailable.await()
                        }
                        logger.info("Resumed indexing")
                    }
                    // Reset retries
                    retries = 0
                    if (wasRetries) {
                        wasRetries = false
                        retrying.decrementAndGet()
                    }
                }
            }
        } finally {
            if (wasRetries) {
                retrying.decrementAndGet()
            }
        }
    }

    private fun sendBulk(
            actions: Collection<AnyBulkableAction>
    ): Collection<AnyBulkableAction>
    {
        if (actions.isEmpty()) {
            return emptyList()
        }
        val bulkRequest = Bulk.Builder()
                .addAction(actions)
                .build() as Bulk
        val bulkResult = esClient.execute(bulkRequest) as BulkResult
        val retriableActions = ArrayList<AnyBulkableAction>()
        var successItems = 0
        if (bulkResult.isSucceeded) {
            successItems = bulkResult.items.size
        } else {
            val failedItems = ArrayList<BulkResult.BulkResultItem>()
            val retriableItems = ArrayList<BulkResult.BulkResultItem>()
            // TODO(Fix Jest to correctly process missing id (for create operation))
            bulkResult.items.zip(actions).forEach { (item, action) ->
                if (item.error == null) {
                    successItems += 1
                    return@forEach
                }
                if (item.errorType in NON_RETRIABLE_ES_ERRORS) {
                    failedItems.add(item)
                } else {
                    retriableItems.add(item)
                    retriableActions.add(action)
                }
            }
            if (failedItems.isNotEmpty()) {
                // TODO(Save non retriable documents into dedicated topic)
                logger.error(formatFailedItems(
                        "Some documents weren't indexed, skipping them",
                        failedItems
                ))
            }
            if (retriableItems.isNotEmpty()) {
                logger.error(formatFailedItems(
                        "Some documents weren't indexed, retrying them",
                        retriableItems))
            }
        }
        if (successItems > 0) {
            logger.info("Successfully processed ${successItems} actions")
        }
        return retriableActions
    }

    private fun formatFailedItems(
            message: String,
            items: Collection<BulkResult.BulkResultItem>
    ): String
    {
        return "$message:\n" +
                items.map { "\t${it.errorType}: ${it.errorReason}" }
                        .joinToString("\n")
    }
}
