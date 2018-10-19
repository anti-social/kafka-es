package company.evo.kafka.elasticsearch

import company.evo.bulk.BulkWriteException
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
import org.apache.kafka.connect.sink.SinkTaskContext

private fun SinkTask.startingWith(props: Map<String, String>, block: SinkTask.() -> Unit) {
    start(props)
    try {
        block()
    } finally {
        stop()
    }
}

class ElasticsearchSinkTaskTests : StringSpec() {

    override fun tags(): Set<Tag> = setOf(company.evo.Task)

    companion object {
        private val TOPIC = "test"
        private val TOPIC_INDEX_MAP_TASK_PROPS = mutableMapOf(
                "name" to "test-connector",
                "connection.url" to "http://localhost:9200",
                "topic.index.map" to "test:test_index"
        )
        private val INDEX_TASK_PROPS = mutableMapOf(
                "name" to "test-connector",
                "connection.url" to "http://localhost:9200",
                "index" to "just_index"
        )
        private val INDEX_VALUE = mapOf(
                "action" to mapOf(
                        "index" to mapOf(
                                "type" to "test_type",
                                "id" to "1"
                        )
                ),
                "source" to mapOf(
                        "name" to "Test document",
                        "status" to 0
                )
        )
        private val INDEX_RECORD = SinkRecord(
                TOPIC, 0,
                null, Any(),
                null, INDEX_VALUE,
                0L
        )
        private val DELETE_VALUE = mapOf(
                "action" to mapOf(
                        "delete" to mapOf(
                                "type" to "test_type",
                                "id" to "1"
                        )
                )
        )
        private val DELETE_RECORD = SinkRecord(
                TOPIC, 0,
                null, Any(),
                null, DELETE_VALUE,
                0L
        )

        private const val OK_BULK_RESULT = """{"took":1,"errors":false,"items":[]}"""
        private const val MAPPING_PARSING_ERROR_RESULT = """{"took":1,"errors":true,"items":[""" +
                """{"index":{"_index":"just_index","_type":"test_type","_id":"1","status":400,"error":""" +
                """{"type":"mapper_parsing_exception","reason":"failed to parse","caused_by":""" +
                """{"type":"not_x_content_exception",""" +
                """"reason":"Compressor detection can only be called on some xcontent bytes or compressed xcontent bytes"}}}}""" +
                """]}"""
        private const val REJECTED_ERROR_RESULT = """{"took":1,"errors":true,"items":[""" +
                """{"index":{"_index":"just_index","_type":"test_type","_id":"1","status":503,"error":""" +
                """{"type":"es_rejected_execution_exception","reason":"rejected execution of ..."}}}""" +
                """]}"""
    }

    private class MockedTaskContext(
            val task: ElasticsearchSinkTask,
            val taskContext: SinkTaskContext,
            val httpClient: HttpAsyncClient,
            val httpRequest: CapturingSlot<HttpUriRequest>,
            val httpResponse: HttpResponse
    )

    private data class FakeResponse(val responseContent: String, val statusCode: Int = 200)

    private fun withMockedTask(responses: List<FakeResponse>, block: MockedTaskContext.() -> Unit) {
        val taskContext = mockk<SinkTaskContext>()
        every {
            taskContext.assignment()
        } returns emptySet()
        every {
            taskContext.pause()
        } just Runs
        every {
            taskContext.resume()
        } just Runs

        val httpClient = mockk<HttpAsyncClient>()
        val httpRequest = slot<HttpUriRequest>()
        val httpResponse = mockk<HttpResponse>()
        val future = slot<FutureCallback<HttpResponse>>()
        every {
            httpClient.execute(capture(httpRequest), capture(future))
        } answers {
            future.captured.completed(httpResponse)
            mockk()
        }
        every {
            httpResponse.statusLine
        } returnsMany responses.map {
            BasicStatusLine(ProtocolVersion("1.1", 1, 1), it.statusCode, "OK")
        }
        every {
            httpResponse.entity
        } returnsMany responses.map {
            StringEntity(it.responseContent)
        }

        val task = ElasticsearchSinkTask(httpClient)
                .apply { initialize(taskContext) }
        val ctx = MockedTaskContext(
                task, taskContext, httpClient, httpRequest, httpResponse
        )
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
            withMockedTask(listOf(FakeResponse(OK_BULK_RESULT))) {
                task.startingWith(INDEX_TASK_PROPS) {
                    put(mutableListOf())
                    flush(null)
                }

                verify {
                    httpClient wasNot Called
                }
            }
        }

        "topic to index map setting" {
            withMockedTask(listOf(FakeResponse(OK_BULK_RESULT))) {
                task.startingWith(TOPIC_INDEX_MAP_TASK_PROPS) {
                    put(mutableListOf(DELETE_RECORD))
                    preCommit(HashMap())
                }

                httpRequest.captured shouldBe instanceOf(HttpPost::class)
                val capturedRequest = httpRequest.captured as HttpPost
                capturedRequest.uri shouldBe URI("http://localhost:9200/_bulk")
                capturedRequest.entity.readString() shouldBe """
                    |{"delete":{"_index":"test_index","_type":"test_type","_id":"1"}}
                    |""".trimMargin()
            }
        }

        "index setting" {
            withMockedTask(listOf(FakeResponse(OK_BULK_RESULT))) {
                task.startingWith(INDEX_TASK_PROPS) {
                    put(mutableListOf(DELETE_RECORD))
                    preCommit(HashMap())
                }

                verify(exactly = 0) { taskContext.pause() }

                httpRequest.captured shouldBe instanceOf(HttpPost::class)
                val capturedRequest = httpRequest.captured as HttpPost
                capturedRequest.uri shouldBe URI("http://localhost:9200/_bulk")
                capturedRequest.entity.readString() shouldBe """
                    |{"delete":{"_index":"just_index","_type":"test_type","_id":"1"}}
                    |""".trimMargin()
            }
        }

        "mapping parsing exception" {
            withMockedTask(listOf(FakeResponse(MAPPING_PARSING_ERROR_RESULT))) {
                task.startingWith(INDEX_TASK_PROPS) {
                    put(mutableListOf(INDEX_RECORD))
                    shouldThrow<ConnectException> {
                        preCommit(HashMap())
                    }.also {
                        it.cause shouldBe instanceOf(BulkWriteException::class)
                    }
                }

                httpRequest.captured shouldBe instanceOf(HttpPost::class)
                val capturedRequest = httpRequest.captured as HttpPost
                capturedRequest.uri shouldBe URI("http://localhost:9200/_bulk")
                capturedRequest.entity.readString() shouldBe """
                    |{"index":{"_index":"just_index","_type":"test_type","_id":"1"}}
                    |{"name":"Test document","status":0}
                    |""".trimMargin()

                verify(exactly = 0) { taskContext.pause() }
            }
        }

        "pausing and resuming task" {
            withMockedTask(listOf(
                    FakeResponse(REJECTED_ERROR_RESULT),
                    FakeResponse(OK_BULK_RESULT)
            )) {
                task.startingWith(INDEX_TASK_PROPS) {
                    put(listOf(INDEX_RECORD))

                    preCommit(emptyMap())
                    verify(exactly = 1) { taskContext.pause() }
                    verify(exactly = 0) { taskContext.resume() }

                    put(emptyList())
                    verify(exactly = 1) { taskContext.pause() }
                    verify(exactly = 1) { taskContext.resume() }

                    preCommit(emptyMap())
                    verify(exactly = 1) { taskContext.pause() }
                    verify(exactly = 1) { taskContext.resume() }
                }
            }
        }
    }
}
