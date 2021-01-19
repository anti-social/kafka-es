package dev.evo.kafka.elasticsearch

import com.google.gson.Gson

import io.searchbox.action.Action
import io.searchbox.client.JestClient
import io.searchbox.client.JestResult
import io.searchbox.client.JestResultHandler
import io.searchbox.core.Bulk

import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask

import org.assertj.core.api.Assertions.*

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test

private fun SinkTask.startingWith(props: Map<String, String>, block: SinkTask.() -> Unit) {
    start(props)
    try {
        block()
    } finally {
        stop()
    }
}

class ElasticsearchSinkTaskTests {
    class MockJestClient : JestClient {
        var requests = ArrayList<Action<out JestResult>>()
        val gson = Gson()

        override fun setServers(servers: MutableSet<String>?) {}

        override fun <T : JestResult> execute(clientRequest: Action<T>): T? {
            requests.add(clientRequest)
            return clientRequest.createNewElasticSearchResult(
                    "",
                    200,
                    "200 OK",
                    gson
            )
        }

        override fun <T : JestResult> executeAsync(clientRequest: Action<T>, jestResultHandler: JestResultHandler<in T>) {}

        override fun close() {}

        override fun shutdownClient() = close()
    }

    companion object {
        val gson = Gson()
        val esClient = MockJestClient()
        val TOPIC = "test"
        val TOPIC_INDEX_MAP_TASK_PROPS = mutableMapOf(
                "name" to "test-connector",
                "connection.url" to "localhost:9200",
                "topic.index.map" to "test:test_index"
        )
        val JUST_INDEX_TASK_PROPS = mutableMapOf(
                "name" to "test-connector",
                "connection.url" to "localhost:9200",
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
    }

    @AfterEach
    fun clearEsClient() {
        esClient.requests.clear()
    }

    @Test fun `test required configuration`() {
        val task = ElasticsearchSinkTask()
        try {
            assertThatThrownBy {
                task.start(mutableMapOf())
            }
                    .isInstanceOf(ConnectException::class.java)
                    .hasCauseInstanceOf(ConfigException::class.java)
                    .hasStackTraceContaining("\"connection.url\"")
        } finally {
            task.stop()
        }
    }

    @Test fun `test empty sink records`() {
        ElasticsearchSinkTask(esClient).startingWith(JUST_INDEX_TASK_PROPS) {
            put(mutableListOf())
            flush(null)
            assertThat(esClient.requests).isEmpty()
        }
    }

    @Test fun `test topic to index map setting`() {
        ElasticsearchSinkTask(esClient).startingWith(TOPIC_INDEX_MAP_TASK_PROPS) {
            put(mutableListOf(DELETE_RECORD))
            preCommit(HashMap())
            assertThat(esClient.requests)
                    .hasSize(1)
                    .first()
                    .isInstanceOf(Bulk::class.java)
                    .returns(
                            """{"delete":{"_id":"1","_index":"test_index","_type":"test_type"}}
                                |
                            """.trimMargin(),
                            { it.getData(gson) }
                    )
        }
    }

    @Test fun `test index setting`() {
        ElasticsearchSinkTask(esClient).startingWith(JUST_INDEX_TASK_PROPS) {
            put(mutableListOf(DELETE_RECORD))
            preCommit(HashMap())
            assertThat(esClient.requests)
                    .hasSize(1)
                    .first()
                    .isInstanceOf(Bulk::class.java)
                    .returns(
                            """{"delete":{"_id":"1","_index":"just_index","_type":"test_type"}}
                                |
                            """.trimMargin(),
                            { it.getData(gson) }
                    )
        }
    }
}
