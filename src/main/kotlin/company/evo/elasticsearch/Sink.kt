package company.evo.elasticsearch

import java.util.concurrent.BlockingQueue

import io.searchbox.client.JestClient
import io.searchbox.core.Bulk
import io.searchbox.core.BulkResult

import java.io.IOException
import java.util.concurrent.atomic.AtomicBoolean

import org.slf4j.LoggerFactory


internal class Sink(
        private val esClient: JestClient,
        private val queue: BlockingQueue<Collection<AnyBulkableAction>>,
        private val confirmationQueue: BlockingQueue<String>,
        private val heartbeatMonitor: Object,
        private val waitingElastic: AtomicBoolean
) : Runnable
{
    companion object {
        val logger = LoggerFactory.getLogger(Sink::class.java)

        // TODO(Review all the exceptions)
        val NON_RETRIABLE_ES_ERRORS = setOf(
                "elasticsearch_parse_exception",
                "parsing_exception",
                "routing_missing_exception"
        )
    }

    override fun run() {
        while (!Thread.interrupted()) {
            try {
                val actions = queue.take()
                guaranteedSend(actions)
                confirmationQueue.put("ok")
            } catch (e: InterruptedException) {}
        }
    }

    private fun guaranteedSend(
            acts: Collection<AnyBulkableAction>
    ) = synchronized(heartbeatMonitor) {
        var actions = acts
        var retries = 0
        while(actions.size > 0 && !waitingElastic.get()) {
            if (retries > 0) {
//                val retryInterval = 30000L * retryNum
                Thread.sleep(30000L)
            }
            try {
                actions = sendBulk(actions)
                retries += 1
            } catch (e: IOException) {
                waitingElastic.set(true)
                heartbeatMonitor.notifyAll()
                heartbeatMonitor.wait()
                retries = 0
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
        logger.info("Sending ${actions.size} actions to elasticsearch")
        val bulkRequest = Bulk.Builder()
                .addAction(actions)
                .build()
        val bulkResult = esClient.execute(bulkRequest)
        val retriableActions = ArrayList<AnyBulkableAction>()
        if (!bulkResult.isSucceeded) {
            val failedItems = ArrayList<BulkResult.BulkResultItem>()
            val retriableItems = ArrayList<BulkResult.BulkResultItem>()
            // TODO(Fix Jest to correctly process missing id (for create operation))
            bulkResult.items.zip(actions).forEach { (item, action) ->
                if (item.error == null) return@forEach
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
