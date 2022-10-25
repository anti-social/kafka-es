package dev.evo.kafka.elasticsearch

import dev.evo.elasticmagic.transport.ElasticsearchException
import dev.evo.elasticmagic.transport.ElasticsearchTransport
import dev.evo.elasticmagic.transport.Method

import java.io.IOException
import java.nio.channels.UnresolvedAddressException

import kotlin.time.measureTimedValue
import kotlin.time.TimeSource

import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.withTimeout

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.boolean
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long

import org.slf4j.LoggerFactory

class ElasticsearchNonRetriableBulkError(msg: String) : IllegalStateException(msg)

data class BulkActionResult(
    val id: String?,
    val type: String?,
    val index: String?,
    val status: Int,
    val error: BulkActionError?,
) {
    companion object {
        fun fromJson(elem: JsonElement): BulkActionResult {
            val item = elem.jsonObject
            return BulkActionResult(
                id = item["_id"]?.jsonPrimitive?.content,
                type = item["_type"]?.jsonPrimitive?.content,
                index = item["_index"]?.jsonPrimitive?.content,
                status = requireNotNull(item["status"]).jsonPrimitive.int,
                error = item["error"]?.let(BulkActionError.Companion::fromJson)
            )
        }
    }
}

sealed class BulkActionError {
    abstract val isRetriable: Boolean

    data class WithType(val type: String, val reason: String?) : BulkActionError() {
        override val isRetriable: Boolean
            get() = !NON_RETRIABLE_ES_ERRORS.contains(type)

        override fun toString(): String {
            return "$type: $reason"
        }
    }

    data class Unknown(val reason: String) : BulkActionError() {
        override val isRetriable = false

        override fun toString(): String {
            return reason
        }
    }

    companion object {
        private val NON_RETRIABLE_ES_ERRORS = setOf(
            "elasticsearch_parse_exception",
            "parsing_exception",
            "routing_missing_exception",
        )

        fun fromJson(elem: JsonElement): BulkActionError {
            if (elem is JsonObject) {
                val errorType = elem["type"]?.jsonPrimitive?.content
                val errorReason = elem["reason"]?.jsonPrimitive?.content
                if (errorType != null) {
                    return WithType(
                        type = errorType,
                        reason = errorReason,
                    )
                }
            }
            return Unknown(elem.toString())
        }
    }
}

/**
 * Sends bulk actions to Elasticsearch and parses a response.
 *
 * @param esTransport an Elasticsearch transport
 * @param requestTimeoutMs the maximum time to wait bulk response
 */
class ElasticsearchBulkSender(
    private val esTransport: ElasticsearchTransport,
    private val requestTimeoutMs: Long,
    internal val clock: TimeSource = TimeSource.Monotonic,
) {
    private val logger = LoggerFactory.getLogger(ElasticsearchBulkSender::class.java)

    suspend fun sendBulk(
        bulk: List<BulkAction>,
        refresh: Boolean = false,
    ): SendBulkResult<BulkAction, BulkActionResult> {
        try {
            val (bulkResult, totalTime) = withTimeout(requestTimeoutMs) {
                logger.debug("Sending ${bulk.size} action ")
                val params = if (refresh) {
                    mapOf("refresh" to listOf("true"))
                } else {
                    emptyMap()
                }
                clock.measureTimedValue {
                    esTransport.request(
                        BulkRequest(
                            Method.POST,
                            "/_bulk",
                            parameters = params,
                            body = bulk,
                        )
                    )
                }
            }
            val itemsResult = requireNotNull(
                bulkResult["items"]?.jsonArray
            )
            val hasErrors = requireNotNull(bulkResult["errors"])
                .jsonPrimitive.boolean
            val tookTimeMs = requireNotNull(bulkResult["took"])
                .jsonPrimitive.long
            val items = itemsResult.map { itRes ->
                BulkActionResult.fromJson(
                    itRes.jsonObject.iterator().next().value
                )
            }
            if (!hasErrors) {
                return SendBulkResult.Success(
                    totalTime.inWholeMilliseconds,
                    tookTimeMs,
                    bulk.size.toLong(),
                    items,
                    emptyList(),
                )
            }

            val nonRetriableItems = mutableListOf<BulkActionResult>()
            val retryItems = mutableListOf<BulkActionResult>()
            val retryActions = mutableListOf<BulkAction>()
            for ((item, action) in items.zip(bulk)) {
                if (item.error == null) {
                    continue
                }

                if (item.error.isRetriable) {
                    retryItems.add(item)
                    retryActions.add(action)
                } else {
                    nonRetriableItems.add(item)
                }
            }

            if (nonRetriableItems.isNotEmpty()) {
                throw ElasticsearchNonRetriableBulkError(formatFailedItems(
                    "Some documents weren't indexed",
                    nonRetriableItems,
                ))
            }
            if (retryItems.isNotEmpty()) {
                logger.error(formatFailedItems(
                    "Some documents weren't indexed, will retry",
                    retryItems
                ))
            }

            return SendBulkResult.Success(
                totalTime.inWholeMilliseconds,
                tookTimeMs,
                (bulk.size - retryActions.size).toLong(),
                items,
                retryActions,
            )
        } catch (e: ElasticsearchException) {
            logger.error("Error when sending bulk actions", e)
            return SendBulkResult.IOError(e)
        } catch (e: IOException) {
            logger.error("Error when sending bulk actions", e)
            return SendBulkResult.IOError(e)
        } catch (e: UnresolvedAddressException) {
            return SendBulkResult.IOError(e)
        } catch (e: TimeoutCancellationException) {
            logger.error("Error when sending bulk actions", e)
            return SendBulkResult.Timeout
        }
    }

    private fun formatFailedItems(
        message: String,
        items: List<BulkActionResult>,
    ): String {
        return "<${esTransport.baseUrl}/_bulk> $message:\n" +
            items.joinToString("\n") {
                "\t[${it.index}/${it.type}/${it.id}] ${it.error}"
            }
    }
}