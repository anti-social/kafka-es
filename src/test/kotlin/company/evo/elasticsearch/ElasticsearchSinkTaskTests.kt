package company.evo.elasticsearch

import org.apache.kafka.common.config.ConfigException

import org.assertj.core.api.Assertions.*

import org.junit.jupiter.api.Test

class ElasticsearchSinkTaskTests {
    @Test fun testSinkTask() {
        val task = ElasticsearchSinkTask()
        task.start(mutableMapOf(
                "connection.url" to "localhost:9200",
                "topic.index.map" to "test:test"))
        task.put(mutableListOf())
    }

    @Test fun testRequiredConfiguration() {
        val task = ElasticsearchSinkTask()
        assertThatThrownBy {
            task.start(mutableMapOf())
        }
                .isInstanceOf(ConfigException::class.java)
                .hasMessageContaining("\"connection.url\"")
        assertThatThrownBy {
            task.start(mutableMapOf("connection.url" to "localhost:9200"))
        }
                .isInstanceOf(ConfigException::class.java)
                .hasMessageContaining("\"topic.index.map\"")
    }
}
