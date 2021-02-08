package dev.evo.kafka.elasticsearch

import dev.evo.elasticart.transport.ElasticsearchTransport
import dev.evo.elasticart.transport.Method
import dev.evo.elasticart.transport.RequestBodyBuilder
import dev.evo.elasticart.transport.StringEncoder

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe

import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.JsonNull

import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put

class ElasticsearchMockTransport(
    private val check: RequestContext.() -> Unit,
) : ElasticsearchTransport("http://example.com:9200", Config()) {
    class RequestContext(
        val method: Method,
        val path: String,
        val parameters: Map<String, List<String>>?,
        val contentType: String?,
        val body: String?,
    )

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
        return """{"errors": false}"""
    }
}

class ElasticsearchWriterTests : StringSpec({
    "test json" {
        val channel = Channel<SinkMsg<BulkAction>>()
        val writer = ElasticsearchWriter(
            this,
            "<test>",
            channel,
            ElasticsearchMockTransport {
                method shouldBe Method.POST
                parameters shouldBe null
                path shouldBe "/_bulk"
                contentType shouldBe "application/x-ndjson"
                body shouldBe """
                    |{"index":{"_id":"1","_type":"_doc","_index":"test"}}
                    |{"name":"Indexed again","keyword":null}
                    |""".trimMargin()
            },
            requestTimeoutMs = 10_000,
            minRetryDelayMs = 15_000,
            maxRetryDelayMs = 600_000,
        )

        try {
            channel.send(SinkMsg.Data(
                listOf(
                    BulkAction(
                        BulkMeta.Index(
                            id = "1",
                            type = "_doc",
                            index = "test",
                        ),
                        JsonSource(
                            buildJsonObject {
                                put("name", "Indexed again")
                                put("keyword", JsonNull)
                            }
                        )
                    )
                )
            ))
            val flushed = Latch(1)
            withTimeout(1_000) {
                channel.send(SinkMsg.Flush(flushed))
                flushed.await()
            }
        } finally {
            channel.close()
        }
    }

    "test protobuf" {
        val channel = Channel<SinkMsg<BulkAction>>()
        val writer = ElasticsearchWriter(
            this,
            "<test>",
            channel,
            ElasticsearchMockTransport {
                method shouldBe Method.POST
                parameters shouldBe null
                path shouldBe "/_bulk"
                contentType shouldBe "application/x-ndjson"
                body shouldBe """
                    |{"index":{"_id":"1","_type":"_doc","_index":"test"}}
                    |{"id":0,"name":"Test protobuf","counter":"0"}
                    |""".trimMargin()
            },
            requestTimeoutMs = 10_000,
            minRetryDelayMs = 15_000,
            maxRetryDelayMs = 600_000,
        )

        try {
            channel.send(SinkMsg.Data(
                listOf(
                    BulkAction(
                        BulkMeta.Index(
                            id = "1",
                            type = "_doc",
                            index = "test",
                        ),
                        ProtobufSource(
                            TestProto.TestDocument.newBuilder().apply {
                                name = "Test protobuf"
                            }
                                .build()
                        )
                    )
                )
            ))
            val flushed = Latch(1)
            withTimeout(1_000) {
                channel.send(SinkMsg.Flush(flushed))
                flushed.await()
            }
        } finally {
            channel.close()
        }
    }
})
