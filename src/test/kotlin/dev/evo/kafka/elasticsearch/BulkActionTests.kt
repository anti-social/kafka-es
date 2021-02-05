package dev.evo.kafka.elasticsearch

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.SerializationException

import kotlinx.serialization.json.Json

class BulkMetaTests : StringSpec({
    fun serializeMeta(meta: BulkMeta): String {
        return Json.encodeToString(BulkMetaSerializer, meta)
    }

    fun deserializeMeta(data: String): BulkMeta {
        return Json.decodeFromString(BulkMetaSerializer, data)
    }

    "serialize index" {
        shouldThrow<IllegalArgumentException> {
            deserializeMeta("""
                {"index":{"id":"1"}}
            """.trimIndent())
        }

        serializeMeta(BulkMeta.Index()) shouldBe """
            {"index":{}}
        """.trimIndent()

        serializeMeta(BulkMeta.Index("111")) shouldBe """
            {"index":{"_id":"111"}}
        """.trimIndent()

        serializeMeta(BulkMeta.Index("111", "_doc")) shouldBe """
            {"index":{"_id":"111","_type":"_doc"}}
        """.trimIndent()

        serializeMeta(BulkMeta.Index("111", "_doc", "test", routing = "222")) shouldBe """
            {"index":{"_id":"111","_type":"_doc","_index":"test","routing":"222"}}
        """.trimIndent()
    }

    "serialize delete" {
        serializeMeta(BulkMeta.Delete("321")) shouldBe """
            {"delete":{"_id":"321"}}
        """.trimIndent()

        serializeMeta(BulkMeta.Delete("321", "product", "catalog", routing = "564")) shouldBe """
            {"delete":{"_id":"321","_type":"product","_index":"catalog","routing":"564"}}
        """.trimIndent()
    }

    "serialize update" {
        serializeMeta(BulkMeta.Update("123")) shouldBe """
            {"update":{"_id":"123"}}
        """.trimIndent()

        serializeMeta(BulkMeta.Update("123", routing = "456")) shouldBe """
            {"update":{"_id":"123","routing":"456"}}
        """.trimIndent()

        serializeMeta(BulkMeta.Update("123", "order", "test", routing = "456", retryOnConflict = 5)) shouldBe """
            {"update":{"_id":"123","_type":"order","_index":"test","routing":"456","retry_on_conflict":5}}
        """.trimIndent()
    }

    "serialize create" {
        serializeMeta(BulkMeta.Create("987")) shouldBe """
            {"create":{"_id":"987"}}
        """.trimIndent()

        serializeMeta(BulkMeta.Create("987", "company", "test", routing = "654")) shouldBe """
            {"create":{"_id":"987","_type":"company","_index":"test","routing":"654"}}
        """.trimIndent()
    }

    "deserialize index" {
        deserializeMeta("""
            {"index":{}}
        """.trimIndent()) shouldBe BulkMeta.Index()

        deserializeMeta("""
            {"index":{"_id":"111"}}
        """.trimIndent()) shouldBe BulkMeta.Index("111")

        deserializeMeta("""
            {"index":{"_id":"111","_type":"_doc","_index":"test","routing":"234"}}
        """.trimIndent()) shouldBe BulkMeta.Index("111", "_doc", "test", routing = "234")

        shouldThrow<IllegalArgumentException> {
            deserializeMeta("""{"index":"test"}""")
        }
    }

    "deserialize delete" {
        shouldThrow<IllegalArgumentException> {
            deserializeMeta("""
                {"delete":{"_type":"_doc","routing":"234"}}
            """.trimIndent())
        }

        deserializeMeta("""
            {"delete":{"_id":"111","_type":"_doc","routing":"234"}}
        """.trimIndent()) shouldBe BulkMeta.Delete("111", "_doc", routing = "234")

        deserializeMeta("""
            {"delete":{"_id":"111","_type":"_doc","_index":"products","routing":"234"}}
        """.trimIndent()) shouldBe BulkMeta.Delete("111", "_doc", "products", routing = "234")
    }

    "deserialize update" {
        shouldThrow<IllegalArgumentException> {
            deserializeMeta("""
                {"update":{}}
            """.trimIndent())
        }

        deserializeMeta("""
            {"update":{"_id":"321"}}
        """.trimIndent()) shouldBe BulkMeta.Update("321")

        deserializeMeta("""
            {"update":{"_id":"999","_type":"_doc","_index":"products","routing":"234","retry_on_conflict":2}}
        """.trimIndent()) shouldBe BulkMeta.Update("999", "_doc", "products", routing = "234", retryOnConflict = 2)
    }


    "deserialize create" {
        deserializeMeta("""
            {"create":{}}
        """.trimIndent()) shouldBe BulkMeta.Create()

        deserializeMeta("""
            {"create":{"_id":"999","_type":"_doc","_index":"products","routing":"234"}}
        """.trimIndent()) shouldBe BulkMeta.Create("999", "_doc", "products", routing = "234")
    }

    "deserialize unknown action" {
        shouldThrow<IllegalStateException> {
            deserializeMeta("{}")
        }
        shouldThrow<SerializationException> {
            deserializeMeta("""{"unknown":{}}""")
        }
    }
})