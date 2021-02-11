package dev.evo.kafka.elasticsearch

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe

import kotlinx.coroutines.test.runBlockingTest

import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask

private fun SinkTask.startWith(props: Map<String, String>, block: SinkTask.() -> Unit) {
    start(props)
    val assignment = listOf(TopicPartition(ElasticsearchSinkTaskTests.TOPIC, 0))
    try {
        open(assignment)
        block()
    } finally {
        close(assignment)
        stop()
    }
}

class ElasticsearchSinkTaskTests : StringSpec({
    "basic test" {
        runBlockingTest {
            val esTransport = ElasticsearchMockTransport {
                body shouldBe """
                    |{"delete":{"_id":"1","_index":"test"}}
                    |
                """.trimMargin()
            }
            ElasticsearchSinkTask(esTransport).startWith(
                mutableMapOf(
                    "name" to "test-connector",
                    "connection.url" to "http://example.com:9200",
                    "topic" to TOPIC,
                    "index" to "test",
                )
            ) {
                put(listOf(
                    newRecord(
                        TOPIC, 0, 0L,
                        null,
                        BulkAction.Delete(BulkMeta.Delete("1")),
                    )
                ))
                val commitBall = mapOf(TopicPartition(TOPIC, 0) to OffsetAndMetadata(1L))
                preCommit(commitBall) shouldBe commitBall
            }
        }
    }
}) {
    companion object {
        const val TOPIC = "test-topic.json"

        fun newRecord(topic: String, partition: Int, offset: Long, key: Any?, value: Any?): SinkRecord {
            return SinkRecord(
                topic, partition,
                null, key,
                null, value,
                offset,
                null, TimestampType.NO_TIMESTAMP_TYPE,
                null,
            )
        }
    }
}
