package dev.evo.kafka.elasticsearch

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

import kotlinx.coroutines.test.runBlockingTest

import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put

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
                        BulkAction.Delete("1"),
                    )
                ))
                val commitBall = mapOf(TopicPartition(TOPIC, 0) to OffsetAndMetadata(1L))
                preCommit(commitBall) shouldBe commitBall
            }
        }
    }

    "multiple concurrent requests".config(invocations = 1000) {
        runBlockingTest {
            val sentBodies = mutableSetOf<String>()
            val lock = Mutex()
            val esTransport = ElasticsearchMockTransport {
                lock.withLock {
                    sentBodies.add(body!!)
                }
            }
            ElasticsearchSinkTask(esTransport).startWith(
                mutableMapOf(
                    "name" to "test-connector",
                    "connection.url" to "http://example.com:9200",
                    "topic" to TOPIC,
                    "index" to "test",
                    "max.in.flight.requests" to "2",
                )
            ) {
                put(listOf(
                    newRecord(
                        TOPIC, 0, 0L,
                        null,
                        BulkAction.Delete("1"),
                    ),
                    newRecord(
                        TOPIC, 0, 1L,
                        null,
                        BulkAction.Delete("2"),
                    ),
                ))
                val commitBall = mapOf(TopicPartition(TOPIC, 0) to OffsetAndMetadata(2L))
                preCommit(commitBall) shouldBe commitBall

                sentBodies shouldBe setOf("""
                        |{"delete":{"_id":"1","_index":"test"}}
                        |
                    """.trimMargin(),
                    """
                        |{"delete":{"_id":"2","_index":"test"}}
                        |
                    """.trimMargin(),
                )
            }
        }
    }

    "multiple actions in single record" {
        runBlockingTest {
            val esTransport = ElasticsearchMockTransport {
                body shouldBe """
                    |{"index":{"_id":"1","_index":"test_0"}}
                    |{"name":"Fan out me!"}
                    |{"index":{"_id":"1","_index":"test_1"}}
                    |{"name":"Fan out me!"}
                    |
                """.trimMargin()
            }
            ElasticsearchSinkTask(esTransport).startWith(
                mutableMapOf(
                    "name" to "test-connector",
                    "connection.url" to "http://example.com:9200",
                    "topic" to TOPIC,
                )
            ) {
                val source = JsonSource(buildJsonObject {
                    put("name", "Fan out me!")
                })
                put(listOf(
                    newRecord(
                        TOPIC, 0, 0L,
                        null,
                        listOf(
                            BulkAction.Index("1", index = "test_0", source = source),
                            BulkAction.Index("1", index = "test_1", source = source),
                        )
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
