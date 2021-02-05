package dev.evo.kafka.elasticsearch

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.kotest.matchers.string.shouldStartWith

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
        action.meta shouldBe BulkMeta.Index(
            index = null,
            id = "123",
            routing = "456",
        )
        action.source shouldBe ProtobufSource(testMsg)
    }

    "deserialize invalid data" {
        val headers = RecordHeaders().apply {
            add("action", indexAction.toByteArray())
        }
        val exc = shouldThrow<DataException> {
            converter.toConnectData("<test>", headers, "invalid data".toByteArray())
        }
        exc.message shouldStartWith "Cannot deserialize data"
    }

    "deserialize delete action" {
        val headers = RecordHeaders().apply {
            add("action", deleteAction.toByteArray())
        }
        val connectData =  converter.toConnectData("<test>", headers, null)
        connectData.schema() shouldBe null
        val action = connectData.value() as BulkAction
        action.meta shouldBe BulkMeta.Delete(
            index = null,
            id = "123",
            routing = "456",
        )
        action.source shouldBe null
    }

    "serialize message" {
        val data =  converter.fromConnectData("<test>", null, testMsg)
        data shouldBe testMsg.toByteArray()
    }

    "serialize null message" {
        val data =  converter.fromConnectData("<test>", null, null)
        data shouldBe null
    }
})
