package dev.evo.kafka.elasticsearch

import dev.evo.elasticart.transport.ElasticsearchException
import dev.evo.elasticart.transport.ElasticsearchTransport
import dev.evo.elasticart.transport.Method

import java.io.IOException

import kotlin.time.Duration
import kotlin.time.measureTimedValue
import kotlin.time.TimeSource

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.channels.receiveOrNull

import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.boolean
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive

import org.slf4j.LoggerFactory


/**
 * Bulk writer actor takes actions from an input channel and sends them into Elasticsearch.
 *
 * @param scope a [CoroutineScope] to launch an actor
 * @param channel the input channel
 * @param esTransport an Elasticsearch transport
 * @param requestTimeoutMs the maximum time to wait bulk response
 * @param delayBetweenRequestsMs the delay between bulk requests
 * @param minRetryDelayMs the minimum delay time before retry
 * @param maxRetryDelayMs the maximum delay time before retry
 */
class ElasticsearchWriter(
    scope: CoroutineScope,
    private val connectorName: String,
    channel: ReceiveChannel<SinkMsg<BulkAction>>,
    private val esTransport: ElasticsearchTransport,
    private val requestTimeoutMs: Long,
    private val minRetryDelayMs: Long,
    private val maxRetryDelayMs: Long,
    delayBetweenRequestsMs: Long = 0,
    private val metrics: KafkaEsMetrics? = null,
    clock: TimeSource = TimeSource.Monotonic,
) {
    private val logger = LoggerFactory.getLogger(ElasticsearchWriter::class.java)
    val job = scope.launch {
        var lastProcessTimeMark = clock.markNow()
        while (true) {
            when (val msg = channel.receiveOrNull()) {
                is SinkMsg.Data -> {
                    delay(delayBetweenRequestsMs - lastProcessTimeMark.elapsedNow().toLongMilliseconds())
                    process(msg.data)
                    lastProcessTimeMark = clock.markNow()
                }
                is SinkMsg.Flush -> {
                    msg.flushed.countDown()
                }
                null -> break
            }
        }
    }

    companion object {
        private val NON_RETRIABLE_ES_ERRORS = setOf(
            "elasticsearch_parse_exception",
            "parsing_exception",
            "routing_missing_exception",
        )
    }

    private enum class SendStatus {
        SUCCESS, ERROR, TIMEOUT,
    }
    private sealed class SendResult {
        class Success(
            val time: Duration,
            val successActionsCount: Long,
            val retryActions: List<BulkAction>,
        ) : SendResult()
        object Error : SendResult()
        object Timeout : SendResult()
    }

    private class FailedItem(
        val index: String?,
        val type: String?,
        val id: String?,
        val errorType: String?,
        val errorReason: String?,
    ) {
        companion object {
            fun fromJsonItem(elem: JsonElement): FailedItem {
                val item = elem.jsonObject
                val index = item["_index"]?.jsonPrimitive?.content
                val type = item["_type"]?.jsonPrimitive?.content
                val id = item["_id"]?.jsonPrimitive?.content
                val error = item["error"]?.jsonObject
                val errorType = error?.get("type")?.jsonPrimitive?.content
                val errorReason = error?.get("reason")?.jsonPrimitive?.content
                return FailedItem(
                    index,
                    type,
                    id,
                    errorType,
                    errorReason,
                )
            }
        }
    }

    private fun setLabels(labels: KafkaEsLabels) = labels.apply {
        connectorName = this@ElasticsearchWriter.connectorName
    }

    private suspend fun process(bulk: List<BulkAction>) {
        var retryDelayMs = minRetryDelayMs
        while (true) {
            val retryActions = when (val sendResult = sendActions(bulk)) {
                is SendResult.Success -> {
                    metrics?.let { metrics ->
                        metrics.bulksCount.inc(::setLabels)
                        metrics.bulksTime.add(sendResult.time.toLongMilliseconds(), ::setLabels)
                        metrics.bulkActionsCount.add(bulk.size.toLong(), ::setLabels)
                    }
                    sendResult.retryActions
                }
                SendResult.Error -> {
                    metrics?.bulksErrorCount?.inc(::setLabels)
                    bulk
                }
                SendResult.Timeout -> {
                    metrics?.bulksTimeoutCount?.inc(::setLabels)
                    bulk
                }
            }
            if (retryActions.isEmpty()) {
                break
            }
            delay(retryDelayMs)
            retryDelayMs = (retryDelayMs * 2).coerceAtMost(maxRetryDelayMs)
        }
    }

    private suspend fun sendActions(bulk: List<BulkAction>): SendResult {
        try {
            val (response, time) = withTimeout(requestTimeoutMs) {
                logger.debug("Sending ${bulk.size} action ")
                measureTimedValue {
                    esTransport.request(
                        Method.POST,
                        "/_bulk",
                        parameters = null,
                        contentType = "application/x-ndjson",
                    ) {
                        for (action in bulk) {
                            action.write(this)
                        }
                    }
                }
            }
            val bulkResult = Json.decodeFromString<JsonElement>(response).jsonObject
            val hasErrors = requireNotNull(
                bulkResult["errors"]?.jsonPrimitive?.boolean
            )
            if (!hasErrors) {
                return SendResult.Success(time, bulk.size.toLong(), emptyList())
            }

            val itemsResult = requireNotNull(
                bulkResult["items"]?.jsonArray
            )
            val failedItems = mutableListOf<FailedItem>()
            val retryItems = mutableListOf<FailedItem>()
            val retryActions = mutableListOf<BulkAction>()
            for ((item, action) in itemsResult.zip(bulk)) {
                val failedItem = FailedItem.fromJsonItem(item)
                if (
                    failedItem.errorType != null &&
                    NON_RETRIABLE_ES_ERRORS.contains(failedItem.errorType)
                ) {
                    failedItems.add(failedItem)
                } else {
                    retryItems.add(failedItem)
                    retryActions.add(action)
                }
            }

            if (failedItems.isNotEmpty()) {
                throw IllegalStateException(formatFailedItems(
                    "Some documents weren't indexed",
                    failedItems,
                ))
            }
            if (retryItems.isNotEmpty()) {
                logger.error(formatFailedItems(
                    "Some documents weren't indexed, will retry",
                    retryItems
                ))
            }

            return SendResult.Success(
                time, (bulk.size - retryActions.size).toLong(), retryActions
            )
        } catch (ex: ElasticsearchException) {
            logger.error("Error when sending bulk actions", ex)
            return SendResult.Error
        } catch (ex: IOException) {
            logger.error("Error when sending bulk actions", ex)
            return SendResult.Error
        } catch (ex: TimeoutCancellationException) {
            logger.error("Error when sending bulk actions", ex)
            return SendResult.Timeout
        }
    }

    private suspend fun updateMetrics() {
        // TODO: also count failed actions
    }

    private fun formatFailedItems(
        message: String,
        items: List<FailedItem>,
    ): String {
        return "<${esTransport.baseUrl}/_bulk> $message:\n" +
            items.joinToString("\n") {
                "\t[${it.index}/${it.type}/${it.id}] ${it.errorType}: ${it.errorReason}"
            }
    }
}
