package company.evo.kafka.elasticsearch

import io.kotlintest.*
import io.kotlintest.matchers.instanceOf
import io.kotlintest.matchers.string.contain
import io.kotlintest.specs.StringSpec

import io.mockk.*

import java.net.URI

import org.apache.http.HttpEntity
import org.apache.http.HttpResponse
import org.apache.http.ProtocolVersion
import org.apache.http.client.methods.HttpPost
import org.apache.http.client.methods.HttpUriRequest
import org.apache.http.concurrent.FutureCallback
import org.apache.http.entity.StringEntity
import org.apache.http.message.BasicStatusLine
import org.apache.http.nio.client.HttpAsyncClient

import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask

private fun SinkTask.startingWith(props: Map<String, String>, block: SinkTask.() -> Unit) {
    start(props)
    try {
        block()
    } finally {
        stop()
    }
}

class ElasticsearchSinkTaskTests : StringSpec() {

    companion object {
        val TOPIC = "test"
        val TOPIC_INDEX_MAP_TASK_PROPS = mutableMapOf(
                "name" to "test-connector",
                "connection.url" to "http://localhost:9200",
                "topic.index.map" to "test:test_index"
        )
        val JUST_INDEX_TASK_PROPS = mutableMapOf(
                "name" to "test-connector",
                "connection.url" to "http://localhost:9200",
                "index" to "just_index"
        )
        val DELETE_VALUE = mapOf(
                "action" to mapOf(
                        "delete" to mapOf(
                                "type" to "test_type",
                                "id" to "1"
                        )
                )
        )
        val DELETE_RECORD = SinkRecord(
                TOPIC, 0,
                null, Any(),
                null, DELETE_VALUE,
                0L
        )
        const val DUMMY_BULK_RESULT = """{"took":1,"errors":false,"items":[]}"""
    }

    private class MockedHttpClientContext(
            val httpClient: HttpAsyncClient,
            val httpRequest: CapturingSlot<HttpUriRequest>,
            val httpResponse: HttpResponse
    )

    private fun withHttpClient(responseContent: String, statusCode: Int = 200, block: MockedHttpClientContext.() -> Unit) {
        val ctx = MockedHttpClientContext(
                mockk(), slot(), mockk()
        )

        val future = slot<FutureCallback<HttpResponse>>()
        every {
            ctx.httpClient.execute(capture(ctx.httpRequest), capture(future))
        } answers {
            future.captured.completed(ctx.httpResponse)
            mockk()
        }
        every {
            ctx.httpResponse.statusLine
        } answers {
            BasicStatusLine(ProtocolVersion("1.1", 1, 1), 200, "OK")
        }
        every {
            ctx.httpResponse.entity
        } answers {
            StringEntity(responseContent)
        }

        ctx.block()
    }

    private fun HttpEntity.readString() = content.readBytes().toString(Charsets.UTF_8)

    init {
        "required configuration" {
            val task = ElasticsearchSinkTask()
            shouldThrow<ConnectException> {
                task.start(mutableMapOf())
            }.let { exc ->
                val cause = exc.cause
                cause shouldBe instanceOf(ConfigException::class)
                cause!!.message should contain("\"connection.url\"")
            }
        }

        "empty sink records" {
            withHttpClient(DUMMY_BULK_RESULT) {
                ElasticsearchSinkTask(httpClient).startingWith(JUST_INDEX_TASK_PROPS) {
                    put(mutableListOf())
                    flush(null)
                }

                verify {
                    httpClient wasNot Called
                }
            }
        }

        "topic to index map setting" {
            withHttpClient(DUMMY_BULK_RESULT) {
                ElasticsearchSinkTask(httpClient).startingWith(JUST_INDEX_TASK_PROPS) {
                    put(mutableListOf(DELETE_RECORD))
                    preCommit(HashMap())
                }

                httpRequest.captured shouldBe instanceOf(HttpPost::class)
                val capturedRequest = httpRequest.captured as HttpPost
                capturedRequest.uri shouldBe URI("http://localhost:9200/_bulk")
                capturedRequest.entity.readString() shouldBe """
                    |{"delete":{"_index":"just_index","_type":"test_type","_id":"1"}}
                    |""".trimMargin()
            }
        }

        "index setting" {
            withHttpClient(DUMMY_BULK_RESULT) {
                ElasticsearchSinkTask(httpClient).startingWith(JUST_INDEX_TASK_PROPS) {
                    put(mutableListOf(DELETE_RECORD))
                    preCommit(HashMap())
                }

                httpRequest.captured shouldBe instanceOf(HttpPost::class)
                val capturedRequest = httpRequest.captured as HttpPost
                capturedRequest.uri shouldBe URI("http://localhost:9200/_bulk")
                capturedRequest.entity.readString() shouldBe """
                    |{"delete":{"_index":"just_index","_type":"test_type","_id":"1"}}
                    |""".trimMargin()
            }
        }
    }
}
