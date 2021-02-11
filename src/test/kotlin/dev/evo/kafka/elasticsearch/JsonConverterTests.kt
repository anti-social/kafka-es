package dev.evo.kafka.elasticsearch

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe

import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonObject

import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.connect.errors.DataException

class JsonConverterTests : StringSpec({
    val converter = JsonConverter().apply {
        configure(mutableMapOf<String, Any>(), false)
    }

    "not configured" {
        val notConfiguredConverter = JsonConverter()
        shouldThrow<UninitializedPropertyAccessException> {
            notConfiguredConverter.toConnectData("<test>", null, null)
        }
    }

    "deserialize missing action" {
        shouldThrow<DataException> {
            converter.toConnectData("<test>", RecordHeaders(), null)
        }
    }

    "deserialize index action" {
        val headers = RecordHeaders().apply {
            add(
                "action",
                """{"index": {"_id": "123", "_type": "_doc", "_index": "test", "routing": "456"}}""".toByteArray()
            )
        }
        val connectData = converter.toConnectData(
            "<test>", headers, """{"name": "Test"}""".toByteArray()
        )
        connectData.schema() shouldBe null
        val action = connectData.value() as BulkAction
        action shouldBe BulkAction.Index(
            id = "123",
            type = "_doc",
            index = "test",
            routing = "456",
            source = JsonSource(buildJsonObject {
                put("name", "Test")
            })
        )
    }

    "deserialize index action without source" {
        val headers = RecordHeaders().apply {
            add(
                "action",
                """{"index": {"_id": "123", "_type": "_doc"}}""".toByteArray()
            )
        }
        shouldThrow<DataException> {
            converter.toConnectData("<test>", headers, null)
        }
    }

    "deserialize delete action" {
        val headers = RecordHeaders().apply {
            add(
                "action",
                """{"delete": {"_id": "123", "_type": "_doc", "_index": "test", "routing": "456"}}""".toByteArray()
            )
        }
        val connectData = converter.toConnectData("<test>", headers, null)
        connectData.schema() shouldBe null
        val action = connectData.value() as BulkAction
        action shouldBe BulkAction.Delete(
            id = "123",
            type = "_doc",
            index = "test",
            routing = "456",
        )
    }

    "deserialize update action" {
        val headers = RecordHeaders().apply {
            add(
                "action",
                """
                    {
                        "update": {
                            "_id": "123", "_type": "_doc", "_index": "test", 
                            "routing": "456", "retry_on_conflict": 3
                        }
                    }
                """.trimIndent().toByteArray()
            )
        }
        val connectData = converter.toConnectData(
            "<test>", headers, """{"doc": {"name": "Test"}}""".toByteArray()
        )
        connectData.schema() shouldBe null
        val action = connectData.value() as BulkAction
        action shouldBe BulkAction.Update(
            id = "123",
            type = "_doc",
            index = "test",
            routing = "456",
            retryOnConflict = 3,
            source = JsonSource(buildJsonObject {
                putJsonObject("doc") {
                    put("name", "Test")
                }
            })
        )
    }
})