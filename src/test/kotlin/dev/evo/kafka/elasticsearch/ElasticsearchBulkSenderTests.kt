package dev.evo.kafka.elasticsearch

import dev.evo.elasticart.transport.ElasticsearchTransport
import dev.evo.elasticart.transport.Method
import dev.evo.elasticart.transport.RequestBodyBuilder
import dev.evo.elasticart.transport.StringEncoder
import io.kotest.assertions.throwables.shouldThrow

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf

import java.io.IOException

import kotlin.time.TestTimeSource
import kotlin.time.milliseconds

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runBlockingTest

import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put

class ElasticsearchMockTransport(
    private val check: suspend RequestContext.() -> Unit,
) : ElasticsearchTransport("http://example.com:9200", Config()) {
    class RequestContext(
        val method: Method,
        val path: String,
        val parameters: Map<String, List<String>>?,
        val contentType: String?,
        val body: String?,
    ) {
        var response: String = """{"errors": false, "took": 1, "items": []}"""

        fun respond(response: String) {
            this.response = response
        }
    }

    override suspend fun request(
        method: Method,
        path: String,
        parameters: Map<String, List<String>>?,
        contentType: String?,
        bodyBuilder: RequestBodyBuilder?,
    ): String {
        val bodyEncoder = StringEncoder()
        bodyBuilder?.invoke(bodyEncoder)
        val ctx = RequestContext(
            method,
            path,
            parameters,
            contentType,
            bodyEncoder.toByteArray().toString(Charsets.UTF_8),
        )
        ctx.check()
        return ctx.response
    }
}

class ElasticsearchBulkSenderTests : StringSpec({
    val jsonIndexAction = BulkAction.Index(
        id = "1",
        type = "_doc",
        index = "test",
        routing = "2",
        source = JsonSource(
            buildJsonObject {
                put("name", "Test json")
                put("keyword", JsonNull)
            }
        )
    )

    "test json" {
        val clock = TestTimeSource()
        val sender = ElasticsearchBulkSender(
            ElasticsearchMockTransport {
                method shouldBe Method.POST
                parameters shouldBe null
                path shouldBe "/_bulk"
                contentType shouldBe "application/x-ndjson"
                body shouldBe """
                    |{"index":{"_id":"1","_type":"_doc","_index":"test","routing":"2"}}
                    |{"name":"Test json","keyword":null}
                    |""".trimMargin()

                clock += 2.milliseconds
            },
            requestTimeoutMs = 10_000,
            clock = clock,
        )

        val result = sender.sendBulk(listOf(jsonIndexAction))
        result.shouldBeInstanceOf<SendBulkResult.Success<BulkAction, BulkActionResult>>()
        result.totalTimeMs shouldBe 2
        result.tookTimeMs shouldBe 1
        result.successActionsCount shouldBe 1
        result.items shouldBe emptyList()
        result.retryActions shouldBe emptyList()
    }

    "test retriable failed actions" {
        runBlockingTest {
            val clock = TestTimeSource()
            val sender = ElasticsearchBulkSender(
                ElasticsearchMockTransport {
                    method shouldBe Method.POST
                    parameters shouldBe null
                    path shouldBe "/_bulk"
                    contentType shouldBe "application/x-ndjson"
                    body shouldBe """
                        |{"index":{"_id":"1","_type":"_doc","_index":"test","routing":"2"}}
                        |{"name":"Test json","keyword":null}
                        |{"delete":{"_id":"2","_type":"_doc","_index":"test"}}
                        |""".trimMargin()

                    respond("""
                        |{"took": 99, "errors": true, "items": [
                        |    {"index": {"_id": "1", "_type": "_doc", "_index": "test", "result": "created", "status": 201}},    
                        |    {"delete": {"_id": "2", "_type": "_doc", "_index": "test", "status": 404, "error": {
                        |        "type": "document_missing_exception",
                        |        "reason": "[_doc][6]: document missing"
                        |    }}}
                        |]}""".trimMargin())
                },
                requestTimeoutMs = 10_000,
                clock = clock,
            )

            val failedAction = BulkAction.Delete(
                id = "2",
                type = "_doc",
                index = "test",
            )
            val result = sender.sendBulk(listOf(
                jsonIndexAction,
                failedAction,
            ))
            result.shouldBeInstanceOf<SendBulkResult.Success<BulkAction, BulkActionResult>>()
            result.tookTimeMs shouldBe 99
            result.successActionsCount shouldBe 1
            result.items shouldBe listOf(
                BulkActionResult(
                    id = "1",
                    type = "_doc",
                    index = "test",
                    status = 201,
                    error = null,
                ),
                BulkActionResult(
                    id = "2",
                    type = "_doc",
                    index = "test",
                    status = 404,
                    error = BulkActionError.WithType(
                        type = "document_missing_exception",
                        reason = "[_doc][6]: document missing",
                    ),
                )
            )
            result.retryActions shouldBe listOf(failedAction)
        }
    }

    "test non-retriable failed actions" {
        runBlockingTest {
            val clock = TestTimeSource()
            val sender = ElasticsearchBulkSender(
                ElasticsearchMockTransport {
                    method shouldBe Method.POST
                    parameters shouldBe null
                    path shouldBe "/_bulk"
                    contentType shouldBe "application/x-ndjson"
                    body shouldBe """
                        |{"index":{"_id":"1","_type":"_doc","_index":"test","routing":"2"}}
                        |{"name":"Test json","keyword":null}
                        |{"delete":{"_id":"2","_type":"_doc","_index":"test"}}
                        |""".trimMargin()

                    respond("""
                        |{"took": 99, "errors": true, "items": [
                        |    {"index": {"_id": "1", "result": "created", "status": 201}},    
                        |    {"delete": {"_id": "2", "status": 404, "error": {
                        |        "type": "routing_missing_exception"
                        |    }}}
                        |]}""".trimMargin())
                },
                requestTimeoutMs = 10_000,
                clock = clock,
            )

            val failedAction = BulkAction.Delete(
                id = "2",
                type = "_doc",
                index = "test",
            )
            shouldThrow<ElasticsearchNonRetriableBulkError> {
                sender.sendBulk(listOf(
                    jsonIndexAction,
                    failedAction,
                ))
            }
        }
    }

    "test protobuf" {
        val clock = TestTimeSource()
        val sender = ElasticsearchBulkSender(
            ElasticsearchMockTransport {
                method shouldBe Method.POST
                parameters shouldBe null
                path shouldBe "/_bulk"
                contentType shouldBe "application/x-ndjson"
                body shouldBe """
                    |{"index":{"_id":"1","_type":"_doc","_index":"test"}}
                    |{"id":0,"name":"Test protobuf","counter":"0"}
                    |""".trimMargin()

                clock += 3.milliseconds
            },
            requestTimeoutMs = 10_000,
            clock = clock,
        )

        val result = sender.sendBulk(
            listOf(
                BulkAction.Index(
                    id = "1",
                    type = "_doc",
                    index = "test",
                    source = ProtobufSource(
                        TestProto.TestDocument.newBuilder().apply {
                            name = "Test protobuf"
                        }
                            .build()
                    )
                )
            )
        ) as SendBulkResult.Success
        result.totalTimeMs shouldBe 3
        result.tookTimeMs shouldBe 1
        result.successActionsCount shouldBe 1
        result.retryActions shouldBe emptyList()
    }

    "test timeout" {
        runBlockingTest {
            val clock = TestTimeSource()
            val sender = ElasticsearchBulkSender(
                ElasticsearchMockTransport {
                    // Waiting response forever
                    CompletableDeferred<Unit>().await()
                },
                requestTimeoutMs = 10_000,
                clock = clock,
            )

            launch {
                val result = sender.sendBulk(listOf(jsonIndexAction))
                result.shouldBeInstanceOf<SendBulkResult.Timeout>()
            }

            advanceTimeBy(10_000)
        }
    }

    "test io error" {
        runBlockingTest {
            val clock = TestTimeSource()
            val sender = ElasticsearchBulkSender(
                ElasticsearchMockTransport {
                    throw IOException()
                },
                requestTimeoutMs = 10_000,
                clock = clock,
            )

            val result = sender.sendBulk(listOf(jsonIndexAction))
            result.shouldBeInstanceOf<SendBulkResult.IOError>()
        }
    }
})