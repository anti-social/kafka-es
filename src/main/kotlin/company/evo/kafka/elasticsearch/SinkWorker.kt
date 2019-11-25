package company.evo.kafka.elasticsearch

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
        private val esUrl: List<String>,
        private val esClient: JestClient,
        private val retryIntervalMs: Long,
        private val maxRetryIntervalMs: Long,
        private var actions: Collection<AnyBulkableAction>,
        private val retryingCount: AtomicInteger
) : Callable<Unit> {

    companion object {
        private val logger = LoggerFactory.getLogger(SinkWorker::class.java)

        // TODO Review all the exceptions
        val NON_RETRIABLE_ES_ERRORS = setOf(
                "elasticsearch_parse_exception",
                "parsing_exception",
                "routing_missing_exception"
        )
    }

    class Context(
            private val esUrl: List<String>,
            private val esClient: JestClient,
            private val bulkSize: Int,
            queueSize: Int,
            private val retryIntervalMs: Long,
            private val maxRetryIntervalMs: Long,
            private val retryingCount: AtomicInteger
    ) {
        private val actionChunks = LinkedList<ArrayList<AnyBulkableAction>>()
        private val queue = ArrayBlockingQueue<FutureTask<Unit>>(queueSize)

        sealed class AddActionResult {
            object Ok : AddActionResult()
            object Timeout : AddActionResult()
            class Task(val task: FutureTask<Unit>) : AddActionResult()
        }

        class FlushResult(val tasks: Collection<FutureTask<Unit>>, val isTimedOut: Boolean)

        private fun createTask(actions: List<AnyBulkableAction>): FutureTask<Unit> {
            return FutureTask(
                    SinkWorker(
                            esUrl,
                            esClient,
                            retryIntervalMs,
                            maxRetryIntervalMs,
                            actions,
                            retryingCount
                    )
            )
        }

        fun pendingBulksCount(): Int = actionChunks.size

        fun takeTask(timeoutMs: Long): FutureTask<Unit>? {
            logger.trace("Waiting for a task ...")
            return queue.poll(timeoutMs, TimeUnit.MILLISECONDS)
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
            val tasks = ArrayList<FutureTask<Unit>>()
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

    override fun call() {
        var retries = 0
        try {
            while (true) {
                if (retries > 0) {
                    if (retries == 1) {
                        retryingCount.incrementAndGet()
                    }
                    val retryIntervalMs = minOf(
                            (retryIntervalMs * Math.pow(2.0, retries.toDouble() - 1)).toLong(),
                            maxRetryIntervalMs
                    )
                    logger.debug("Sleeping for $retryIntervalMs ms before retry")
                    Thread.sleep(retryIntervalMs)
                }
                try {
                    actions = sendBulk(actions)
                    if (actions.isEmpty()) {
                        break
                    }
                } catch (e: IOException) {
                    logger.warn("Error when sending actions to elasticsearch: $e")
                }
                retries += 1
            }
        } finally {
            if (retries > 0) {
                retryingCount.decrementAndGet()
            }
        }
    }

    private fun sendBulk(
            actions: Collection<AnyBulkableAction>
    ): Collection<AnyBulkableAction> {
        if (actions.isEmpty()) {
            return emptyList()
        }
        val bulkRequest = Bulk.Builder()
                .addAction(actions)
                .build()
        val bulkResult = esClient.execute(bulkRequest)
        val retriableActions = ArrayList<AnyBulkableAction>()
        var successItems = 0
        if (bulkResult.isSucceeded) {
            successItems = bulkResult.items.size
        } else {
            val failedItems = ArrayList<BulkResult.BulkResultItem>()
            val retriableItems = ArrayList<BulkResult.BulkResultItem>()
            // TODO Fix Jest to correctly process missing id (for create operation)
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
                // TODO Save non retriable documents into dedicated topic
                logger.error(formatFailedItems(
                        "Some documents weren't indexed, skipping", failedItems
                ))
            }
            if (retriableItems.isNotEmpty()) {
                logger.error(formatFailedItems(
                        "Some documents weren't indexed, will retry", retriableItems
                ))
            }
        }
        if (successItems > 0) {
            logger.debug("Successfully processed $successItems actions")
        }
        return retriableActions
    }

    private fun formatFailedItems(
            message: String,
            items: Collection<BulkResult.BulkResultItem>
    ): String {
        return "<$esUrl> $message:\n" +
                items.joinToString("\n") {
                    "\t[${it.index}/${it.type}/${it.id}] ${it.errorType}: ${it.errorReason}"
                }
    }
}
