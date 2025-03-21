package dev.evo.kafka.elasticsearch

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.kotest.matchers.string.shouldStartWith
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put

import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.connect.errors.DataException

class ProtobufConverterTests : StringSpec({
    val converter = ProtobufConverter()
    val indexAction = BulkActionProto.BulkAction.newBuilder().apply {
        opType = BulkActionProto.BulkAction.OpType.INDEX
        id = "123"
        routing = "456"
    }
        .build()
    val deleteAction = BulkActionProto.BulkAction.newBuilder().apply {
        opType = BulkActionProto.BulkAction.OpType.DELETE
        id = "123"
        routing = "456"
    }
        .build()
    val testMsg = TestProto.TestDocument.newBuilder().apply {
        id = 123
        name = "Teo"
    }
        .build()

    beforeEach {
        converter.configure(
            mutableMapOf("protobuf.class" to "dev.evo.kafka.elasticsearch.TestProto\$TestDocument"),
            false
        )
    }

    "required configuration" {
        val exc = shouldThrow<ConfigException> {
            converter.configure(mutableMapOf<String, Any>(), false)
        }
        exc.message shouldStartWith "Missing required configuration \"protobuf.class\""
    }

    "missing protobuf class" {
        val exc = shouldThrow<ConfigException> {
            converter.configure(
                mutableMapOf("protobuf.class" to "dev.evo.kafka.elasticsearch.UnknownMessage"),
                false
            )
        }
        exc.message shouldContain "Class dev.evo.kafka.elasticsearch.UnknownMessage could not be found"
    }

    "invalid protobuf class" {
        val exc = shouldThrow<ConfigException> {
            converter.configure(
                mutableMapOf("protobuf.class" to converter::class.java.canonicalName),
                false
            )
        }
        exc.message shouldContain "dev.evo.kafka.elasticsearch.ProtobufConverter class has no method parser"
    }

    "deserialize without headers" {
        shouldThrow<NotImplementedError> {
            converter.toConnectData("<test>", testMsg.toByteArray())
        }
    }

    "deserialize index action" {
        val headers = RecordHeaders().apply {
            add("action", indexAction.toByteArray())
        }
        val connectData =  converter.toConnectData("<test>", headers, testMsg.toByteArray())
        connectData.schema() shouldBe null
        val action = connectData.value() as BulkAction
        action shouldBe BulkAction.Index(
            id = "123",
            routing = "456",
            source = ProtobufSource(testMsg)
        )
    }

    "deserialize invalid data" {
        val headers = RecordHeaders().apply {
            add("action", indexAction.toByteArray())
        }
        val exc = shouldThrow<DataException> {
            converter.toConnectData("<test>", headers, "invalid data".toByteArray())
        }
        exc.message shouldStartWith "Error when parsing protobuf data"
    }

    "deserialize delete action" {
        val headers = RecordHeaders().apply {
            add("action", deleteAction.toByteArray())
        }
        val connectData =  converter.toConnectData("<test>", headers, null)
        connectData.schema() shouldBe null
        val action = connectData.value() as BulkAction
        action shouldBe BulkAction.Delete(
            id = "123",
            routing = "456",
        )
    }

    "serialize message" {
        val data =  converter.fromConnectData("<test>", null, testMsg)
        data shouldBe testMsg.toByteArray()
    }

    "serialize null message" {
        val data =  converter.fromConnectData("<test>", null, null)
        data shouldBe null
    }

    "convert message if tag header present and same" {
        val converter = ProtobufConverter().apply {
            configure(mutableMapOf<String, Any>(
                "protobuf.class" to "dev.evo.kafka.elasticsearch.TestProto\$TestDocument",
                "value.converter.tag" to "foo",
                "tag.header.key" to "tag",
            ), false)
        }

        val headers = RecordHeaders().apply {
            add("action", deleteAction.toByteArray())
            add("tag", "foo".toByteArray())
        }
        val connectData =  converter.toConnectData("<test>", headers, null)
        connectData.schema() shouldBe null
        val action = connectData.value() as BulkAction
        action shouldBe BulkAction.Delete(
            id = "123",
            routing = "456",
        )
    }
     "convert message if no tag header" {
        val converter = ProtobufConverter().apply {
             configure(mutableMapOf<String, Any>(
                 "protobuf.class" to "dev.evo.kafka.elasticsearch.TestProto\$TestDocument",
                 "value.converter.tag" to "foo",
                 "tag.header.key" to "tag",
             ), false)
        }
         val headers = RecordHeaders().apply {
             add("action", deleteAction.toByteArray())
         }
         val connectData =  converter.toConnectData("<test>", headers, null)
         connectData.schema() shouldBe null
         val action = connectData.value() as BulkAction
         action shouldBe BulkAction.Delete(
             id = "123",
             routing = "456",
         )
    }
})
