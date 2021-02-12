package dev.evo.kafka.elasticsearch

import dev.evo.elasticart.transport.ElasticsearchException
import dev.evo.elasticart.transport.ElasticsearchTransport
import dev.evo.elasticart.transport.Method

import java.io.IOException

import kotlin.time.measureTimedValue

import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.withTimeout

import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.boolean
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long

import org.slf4j.LoggerFactory
import kotlin.time.TimeSource

class ElasticsearchNonRetriableBulkError(msg: String) : IllegalStateException(msg)

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

    companion object {
        private val NON_RETRIABLE_ES_ERRORS = setOf(
            "elasticsearch_parse_exception",
            "parsing_exception",
            "routing_missing_exception",
        )
    }

    private data class Item(
        val id: String?,
        val type: String?,
        val index: String?,
        val error: ItemError?,
    ) {
        companion object {
            fun fromJson(elem: JsonElement): Item {
                val item = elem.jsonObject
                return Item(
                    id = item["_id"]?.jsonPrimitive?.content,
                    type = item["_type"]?.jsonPrimitive?.content,
                    index = item["_index"]?.jsonPrimitive?.content,
                    error = item["error"]?.let(ItemError.Companion::fromJson)
                )
            }
        }
    }

    private sealed class ItemError {
        data class WithType(val type: String, val reason: String?) : ItemError() {
            override fun toString(): String {
                return "$type: $reason"
            }
        }
        data class Unknown(val reason: String) : ItemError() {
            override fun toString(): String {
                return reason
            }
        }

        companion object {
            fun fromJson(elem: JsonElement): ItemError {
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

    suspend fun sendActions(bulk: List<BulkAction>): SendBulkResult<BulkAction> {
        try {
            val (response, totalTime) = withTimeout(requestTimeoutMs) {
                logger.debug("Sending ${bulk.size} action ")
                clock.measureTimedValue {
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
            val hasErrors = requireNotNull(bulkResult["errors"])
                .jsonPrimitive.boolean
            val tookTimeMs = requireNotNull(bulkResult["took"])
                .jsonPrimitive.long
            if (!hasErrors) {
                return SendBulkResult.Success(
                    totalTime.toLongMilliseconds(),
                    tookTimeMs,
                    bulk.size.toLong(),
                    emptyList(),
                )
            }

            val itemsResult = requireNotNull(
                bulkResult["items"]?.jsonArray
            )
            val failedItems = mutableListOf<Item>()
            val retryItems = mutableListOf<Item>()
            val retryActions = mutableListOf<BulkAction>()
            for ((resultItem, action) in itemsResult.zip(bulk)) {
                val item = Item.fromJson(
                    resultItem.jsonObject.iterator().next().value
                )
                if (item.error == null) {
                    continue
                }

                if (item.error is ItemError.WithType && !NON_RETRIABLE_ES_ERRORS.contains(item.error.type)) {
                    retryItems.add(item)
                    retryActions.add(action)
                } else {
                    failedItems.add(item)
                }
            }

            if (failedItems.isNotEmpty()) {
                throw ElasticsearchNonRetriableBulkError(formatFailedItems(
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

            return SendBulkResult.Success(
                totalTime.toLongMilliseconds(),
                tookTimeMs,
                (bulk.size - retryActions.size).toLong(),
                retryActions,
            )
        } catch (ex: ElasticsearchException) {
            logger.error("Error when sending bulk actions", ex)
            return SendBulkResult.IOError
        } catch (ex: IOException) {
            logger.error("Error when sending bulk actions", ex)
            return SendBulkResult.IOError
        } catch (ex: TimeoutCancellationException) {
            logger.error("Error when sending bulk actions", ex)
            return SendBulkResult.Timeout
        }
    }

    private fun formatFailedItems(
        message: String,
        items: List<Item>,
    ): String {
        return "<${esTransport.baseUrl}/_bulk> $message:\n" +
            items.joinToString("\n") {
                "\t[${it.index}/${it.type}/${it.id}] ${it.error}"
            }
    }
}