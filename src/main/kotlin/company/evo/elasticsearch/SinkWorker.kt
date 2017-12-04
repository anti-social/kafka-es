package company.evo.elasticsearch

import java.io.IOException
import java.util.LinkedList
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.Callable
import java.util.concurrent.FutureTask
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import kotlin.collections.ArrayList

import io.searchbox.client.JestClient
import io.searchbox.core.Bulk
import io.searchbox.core.BulkResult

import org.slf4j.LoggerFactory

import company.evo.kafka.Timeout


internal class SinkWorker(
        private val heartbeat: Heartbeat,
        private val esClient: JestClient,
        private val retryIntervalMs: Long,
        private val maxRetryIntervalMs: Long,
        private var actions: Collection<AnyBulkableAction>,
        private val retryingCount: AtomicInteger
) : Callable<Boolean> {

    companion object {
        private val logger = LoggerFactory.getLogger(SinkWorker::class.java)

        // TODO(Review all the exceptions)
        val NON_RETRIABLE_ES_ERRORS = setOf(
                "elasticsearch_parse_exception",
                "parsing_exception",
                "routing_missing_exception"
        )
    }

    class Context(
            private val heartbeat: Heartbeat,
            private val esClient: JestClient,
            private val bulkSize: Int,
            queueSize: Int,
            private val retryIntervalMs: Long,
            private val maxRetryIntervalMs: Long,
            private val retryingCount: AtomicInteger
    ) {
        private val actionChunks = LinkedList<ArrayList<AnyBulkableAction>>()
        private val queue = ArrayBlockingQueue<FutureTask<Boolean>>(queueSize)

        sealed class AddActionResult {
            object Ok : AddActionResult()
            object Timeout : AddActionResult()
            class Task(val task: FutureTask<Boolean>) : AddActionResult()
        }

        class FlushResult(val tasks: Collection<FutureTask<Boolean>>, val isTimedOut: Boolean)

        private fun createTask(actions: List<AnyBulkableAction>): FutureTask<Boolean> {
            return FutureTask(
                    SinkWorker(
                            heartbeat,
                            esClient,
                            retryIntervalMs,
                            maxRetryIntervalMs,
                            actions,
                            retryingCount
                    )
            )
        }

        fun pendingBulksCount(): Int = actionChunks.size

        fun takeTask(): FutureTask<Boolean> {
            logger.trace("Waiting task ...")
            return queue.take()
        }

        internal fun addAction(action: AnyBulkableAction, paused: Boolean, timeout: Timeout): AddActionResult {
            val lastChunk = actionChunks.peekLast()
            if (lastChunk == null || lastChunk.size >= bulkSize) {
                actionChunks.add(ArrayList())
            }
            actionChunks.last.add(action)

            val actions = actionChunks.peekFirst()
            return if (!paused && actions.size >= bulkSize) {
                val task = createTask(actions)
                if (queue.offer(task, timeout.drift(), TimeUnit.MILLISECONDS)) {
                    logger.debug("Queued ${actions.size} actions")
                    actionChunks.pollFirst()
                    AddActionResult.Task(task)
                } else {
                    AddActionResult.Timeout
                }
            } else {
                AddActionResult.Ok
            }
        }

        fun flush(timeout: Timeout): FlushResult {
            val tasks = ArrayList<FutureTask<Boolean>>()
            val chunksIterator = actionChunks.iterator()
            for (actions in chunksIterator) {
                val task = createTask(actions)
                val timeoutMs = timeout.drift()
                if (timeoutMs <= 0) {
                    return FlushResult(tasks, true)
                }
                if (queue.offer(task, timeoutMs, TimeUnit.MILLISECONDS)) {
                    logger.debug("Queued ${actions.size} actions")
                    chunksIterator.remove()
                    tasks.add(task)
                } else {
                    return FlushResult(tasks, true)
                }
            }
            return FlushResult(tasks, false)
        }
    }

    override fun call(): Boolean {
        var retries = 0
        var wereRetries = false
        try {
            while (actions.isNotEmpty()) {
                if (retries > 0) {
                    if (retries == 1) {
                        wereRetries = true
                        retryingCount.incrementAndGet()
                    }
                    val retryIntervalMs = minOf(
                            (retryIntervalMs * Math.pow(2.0, retries.toDouble() - 1)).toLong(),
                            maxRetryIntervalMs
                    )
                    logger.debug("Sleeping for $retryIntervalMs ms")
                    Thread.sleep(retryIntervalMs)
                }
                try {
                    actions = sendBulk(actions)
                    retries += 1
                } catch (e: IOException) {
                    logger.info("Error when sending actions to elasticsearch", e)
                    heartbeat.start()
                    logger.info("Resumed indexing")
                    // Reset retry interval
                    retries = 0
                }
            }
        } finally {
            if (wereRetries) {
                retryingCount.decrementAndGet()
            }
        }
        return true
    }

    private fun sendBulk(
            actions: Collection<AnyBulkableAction>
    ): Collection<AnyBulkableAction> {
        if (actions.isEmpty()) {
            return emptyList()
        }
        val bulkRequest = Bulk.Builder()
                .addAction(actions)
                .build() as Bulk
        val bulkResult = esClient.execute(bulkRequest)
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
                        "Some documents weren't indexed, skipping",
                        bulkRequest.uri, failedItems
                ))
            }
            if (retriableItems.isNotEmpty()) {
                logger.error(formatFailedItems(
                        "Some documents weren't indexed, will retry",
                         bulkRequest.uri, retriableItems))
            }
        }
        if (successItems > 0) {
            logger.info("Successfully processed ${successItems} actions")
        }
        return retriableActions
    }

    private fun formatFailedItems(
            message: String, uri: String,
            items: Collection<BulkResult.BulkResultItem>
    ): String {
        return "$uri: $message:\n" +
                items.map { "\t[${it.index}/${it.type}/${it.id}] ${it.errorType}: ${it.errorReason}" }
                        .joinToString("\n")
    }
}
