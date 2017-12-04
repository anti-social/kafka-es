package company.evo.elasticsearch

import com.google.gson.Gson

import io.searchbox.action.Action
import io.searchbox.client.JestClient
import io.searchbox.client.JestResult
import io.searchbox.client.JestResultHandler
import io.searchbox.core.Bulk

import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkRecord

import org.assertj.core.api.Assertions.*

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test


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
        val task = ElasticsearchSinkTask(esClient)
        val TOPIC = "test"
        val TASK_PROPS = mutableMapOf(
                "connection.url" to "localhost:9200",
                "topic.index.map" to "test:test_index"
        )
    }

    @BeforeEach
    fun startTask() {
        task.start(TASK_PROPS)
    }

    @AfterEach
    fun stopTask() {
        task.stop()
    }

    @AfterEach
    fun clearEsClient() {
        esClient.requests.clear()
    }

    @Test fun testRequiredConfiguration() {
        val task = ElasticsearchSinkTask()
        assertThatThrownBy {
            task.start(mutableMapOf())
        }
                .isInstanceOf(ConnectException::class.java)
                .hasCauseInstanceOf(ConfigException::class.java)
                .hasStackTraceContaining("\"connection.url\"")
    }

    @Test fun testPutEmptyList() {
        task.put(mutableListOf())
        task.flush(null)
        assertThat(esClient.requests).isEmpty()
    }

    @Test fun testPutAndPreCommit() {
        val value = mapOf(
                "action" to mapOf(
                        "delete" to mapOf(
                                "type" to "test_type",
                                "id" to "1"
                        )
                )
        )
        task.put(mutableListOf(
                SinkRecord(TOPIC, 0,
                        null, Any(),
                        null, value,
                        0L)
        ))
        task.preCommit(HashMap())
        assertThat(esClient.requests)
                .hasSize(1)
                .first()
                .isInstanceOf(Bulk::class.java)
                .returns(
                        """{"delete":{"_id":"1","_index":"test_index","_type":"test_type"}}
                            |""".trimMargin(),
                        { it.getData(gson) }
                )
    }
}
