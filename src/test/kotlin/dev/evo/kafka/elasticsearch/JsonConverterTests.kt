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
            add("action", """{"index": {"_id": "123", "_type": "_doc", "routing": "456"}}""".toByteArray())
        }
        val connectData = converter.toConnectData("<test>", headers, """{"name": "Test"}""".toByteArray())
        connectData.schema() shouldBe null
        val action = connectData.value() as BulkAction
        action.meta shouldBe BulkMeta.Index(
            index = null,
            id = "123",
            type = "_doc",
            routing = "456",
        )
        action.source shouldBe JsonSource(buildJsonObject { put("name", "Test") })
    }

    "deserialize index action without source" {
        val headers = RecordHeaders().apply {
            add("action", """{"index": {"_id": "123", "_type": "_doc", "routing": "456"}}""".toByteArray())
        }
        shouldThrow<DataException> {
            converter.toConnectData("<test>", headers, null)
        }
    }

    "deserialize delete action" {
        val headers = RecordHeaders().apply {
            add("action", """{"delete": {"_id": "123", "_type": "_doc", "routing": "456"}}""".toByteArray())
        }
        val connectData = converter.toConnectData("<test>", headers, null)
        connectData.schema() shouldBe null
        val action = connectData.value() as BulkAction
        action.meta shouldBe BulkMeta.Delete(
            index = null,
            id = "123",
            type = "_doc",
            routing = "456",
        )
        action.source shouldBe null
    }

    "deserialize update action" {
        val headers = RecordHeaders().apply {
            add("action", """{"update": {"_id": "123", "_type": "_doc", "routing": "456", "retry_on_conflict": 3}}""".toByteArray())
        }
        val connectData = converter.toConnectData("<test>", headers, """{"doc": {"name": "Test"}}""".toByteArray())
        connectData.schema() shouldBe null
        val action = connectData.value() as BulkAction
        action.meta shouldBe BulkMeta.Update(
            index = null,
            id = "123",
            type = "_doc",
            routing = "456",
            retryOnConflict = 3,
        )
        action.source shouldBe JsonSource(buildJsonObject {
            putJsonObject("doc") {
                put("name", "Test")
            }
        })
    }
})