package company.evo.bulk.elasticsearch

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JsonMappingException
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue

import company.evo.bulk.BulkWriteException
import company.evo.bulk.BulkWriter

import java.io.ByteArrayOutputStream
import java.io.IOException

import kotlin.math.pow
import kotlinx.coroutines.delay
import kotlinx.coroutines.suspendCancellableCoroutine

import org.apache.http.HttpResponse
import org.apache.http.HttpStatus
import org.apache.http.client.methods.HttpPost
import org.apache.http.client.methods.HttpUriRequest
import org.apache.http.concurrent.FutureCallback
import org.apache.http.entity.ByteArrayEntity
import org.apache.http.entity.ContentType
import org.apache.http.nio.client.HttpAsyncClient

import org.slf4j.LoggerFactory


interface RetryPolicy {
    fun nextRetryDelay(retry: Int): Long
}

class ExponentialRetryPolicy(
        private val minRetryDelayMs: Long, private val maxRetryDelayMs: Long
) : RetryPolicy {
    override fun nextRetryDelay(retry: Int): Long {
        if (retry <= 0) {
            return 0
        }
        return minRetryDelayMs.toDouble().pow(retry).toLong().coerceAtMost(maxRetryDelayMs)
    }
}

class ElasticBulkWriter(
        private val httpClient: HttpAsyncClient,
        urls: List<String>,
        private val maxRetries: Int = 0,
        private val retryPolicy: RetryPolicy
) : BulkWriter<BulkAction> {

    companion object {
        val RETRYABLE_STATUS_CODES = setOf(
                HttpStatus.SC_REQUEST_TIMEOUT,
                429, // Too Many Requests
                HttpStatus.SC_INTERNAL_SERVER_ERROR,
                HttpStatus.SC_BAD_GATEWAY,
                HttpStatus.SC_SERVICE_UNAVAILABLE,
                HttpStatus.SC_GATEWAY_TIMEOUT
        )
        val NON_RETRYABLE_ES_ERRORS = setOf(
//                "version_conflict_engine_exception",
                "elasticsearch_parse_exception",
                "parsing_exception",
                "mapper_parsing_exception",
                "routing_missing_exception"
        )

        private val logger = LoggerFactory.getLogger(ElasticBulkWriter::class.java)
    }

    private val bulkUrls = urls.map { "$it/_bulk" }
    private var curUrlIx = 0L
    private var bulkUrl = bulkUrls[0]
    private val objectMapper = jacksonObjectMapper().apply {
        configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    }
    private val outStream = ByteArrayOutputStream()

    override suspend fun write(actions: List<BulkAction>): Boolean {
        var pendingActions = actions
        var retries = 0
        while(pendingActions.isNotEmpty()) {
            if (retries > maxRetries) {
                return false
            }
            val retryDelayMs = retryPolicy.nextRetryDelay(retries)
            if (retryDelayMs > 0) {
                delay(retryDelayMs)
            }

            try {
                val httpResponse = doWrite(pendingActions)
                pendingActions = processResponse(pendingActions, httpResponse)
            } catch (exc: JsonMappingException) {
                // We cannot map json, so this is an unrecoverable error
                throw BulkWriteException("Error mapping json", exc)
            } catch (exc: IOException) {
                logger.warn("Error happened when making request: $exc")
            }

            retries++
        }
        return true
    }

    private suspend fun doWrite(actions: List<BulkAction>): HttpResponse {
        nextBulkUrl()
        return httpClient.awaitResponse(HttpPost(bulkUrl).apply {
            outStream.reset()
            // TODO Serialize actions once
            actions.forEach {
                it.writeTo(objectMapper, outStream)
            }
            val body = outStream.toByteArray()
            entity = ByteArrayEntity(
                    body, ContentType.create("application/x-ndjson")
            )
        }).also {
            checkStatus(it)
        }
    }

    private fun nextBulkUrl() {
        bulkUrl = bulkUrls[(curUrlIx++ % bulkUrls.size).toInt()]
    }

    private fun checkStatus(resp: HttpResponse) {
        // Raises an exception on an unrecoverable error, possibly we sent an invalid request
        val statusCode = resp.statusLine.statusCode
        if (statusCode in (200..299))
            return
        if (statusCode in RETRYABLE_STATUS_CODES)
            return
        val content = resp.entity.content.readAllBytes()
        throw BulkWriteException(content.toString(Charsets.UTF_8))
    }

    private fun processResponse(
            actions: List<BulkAction>, httpResponse: HttpResponse
    ): List<BulkAction> {
        var successCount = 0
        val failedItems = mutableListOf<BulkResult.Item>()
        val retriableItems = mutableListOf<BulkResult.Item>()
        val retriableActions = mutableListOf<BulkAction>()
        val content = httpResponse.entity.content.readAllBytes()
        val bulkResult = objectMapper.readValue<BulkResult>(content)
        bulkResult.items.zip(actions).forEach { (item, action) ->
            val error = item.value.error
            if (error == null) {
                successCount++
            } else {
                if (error.type in NON_RETRYABLE_ES_ERRORS) {
                    failedItems.add(item)
                } else {
                    retriableItems.add(item)
                    retriableActions.add(action)
                }
            }
        }
        if (failedItems.isNotEmpty()) {
            throw BulkWriteException(formatFailedItems(
                    "Some documents weren't indexed", failedItems
            ))
        }
        if (retriableItems.isNotEmpty()) {
            logger.warn(formatFailedItems(
                    "Some documents weren't indexed, will retry", retriableItems
            ))
        }
        return retriableActions
    }

    private fun formatFailedItems(
            message: String, items: Collection<BulkResult.Item>
    ): String {
        return "$bulkUrl: $message:\n" +
                items.joinToString("\n") {
                    val error = it.error
                    val reason = if (error != null) {
                        "${error.type}: ${error.reason}".let { msg ->
                            val causedBy = error.causedBy?.reason
                            if (error.causedBy != null) {
                                "$msg, caused by $causedBy"
                            } else {
                                msg
                            }
                        }
                    } else {
                        "Unknown reason"
                    }
                    "\t[${it.index}/${it.type}/${it.id}] $reason"
                }
    }
}

suspend fun HttpAsyncClient.awaitResponse(
        request: HttpUriRequest
): HttpResponse = suspendCancellableCoroutine { cont ->
    val future = execute(request, object : FutureCallback<HttpResponse> {
        override fun completed(result: HttpResponse) {
            cont.resumeWith(Result.success(result))
        }
        override fun failed(ex: Exception) {
            cont.resumeWith(Result.failure(ex))
        }
        override fun cancelled() {
        }
    })

    cont.invokeOnCancellation { future.cancel(false) }
}
