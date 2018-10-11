package company.evo.kafka

import company.evo.kafka.TestProto.TestDocument

import io.kotlintest.Description
import io.kotlintest.Spec
import io.kotlintest.should
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kotlintest.matchers.startWith
import io.kotlintest.matchers.string.contain
import io.kotlintest.specs.StringSpec

import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.data.SchemaAndValue
import org.apache.kafka.connect.errors.DataException


class ProtobufConverterTests : StringSpec() {
    private val converter = ProtobufConverter()
    private val testMsg = TestDocument.newBuilder()
            .setId(123)
            .setName("Teo")
            .build()

    override fun beforeSpec(description: Description, spec: Spec) {
        converter.configure(
                mutableMapOf("protobuf.class" to "company.evo.kafka.TestProto\$TestDocument"),
                false
        )
    }

    init {
        "required configuration" {
            shouldThrow<ConfigException> {
                converter.configure(mutableMapOf<String, Any>(), false)
            }.also {
                it.message should startWith(
                        "Missing required configuration \"protobuf.class\""
                )
            }
        }

        "missing protobuf class" {
            shouldThrow<ConfigException> {
                converter.configure(
                        mutableMapOf("protobuf.class" to "company.evo.kafka.UnknownMessage"),
                        false
                )
            }.also {
                it.message should contain(
                        "Class company.evo.kafka.UnknownMessage could not be found"
                )
            }
        }

        "invalid protobuf class" {
            shouldThrow<ConfigException> {
                converter.configure(
                        mutableMapOf("protobuf.class" to "company.evo.kafka.ProtobufConverter"),
                        false
                )
            }.also {
                it.message should contain(
                        "company.evo.kafka.ProtobufConverter class has no method parseFrom"
                )
            }
        }

        "convert to connect data" {
            val data = testMsg.toByteArray()
            val schemaAndValue = converter.toConnectData("<test>", data)
            schemaAndValue.schema() shouldBe null
            val value = schemaAndValue.value()
            if (value !is TestDocument) {
                throw AssertionError("$value must be an instance of ${TestDocument::class}")
            }
            value.id shouldBe 123
            value.name shouldBe "Teo"
        }

        "convert null value to connect data" {
            converter.toConnectData("<test>", null) shouldBe SchemaAndValue.NULL
        }

        "convert invalid data to connect data" {
            shouldThrow<DataException> {
                converter.toConnectData("<test>", "invalid data".toByteArray())
            }.also {
                it.message should startWith("Cannot deserialize data")
            }
        }

        "convert from connect data" {
            val data = converter.fromConnectData("<test>", null, testMsg)
            val value = TestDocument.parseFrom(data)
            value.id shouldBe 123
            value.name shouldBe "Teo"
        }

        "convert null value from connect data" {
            converter.fromConnectData("<test>", null, null) shouldBe null
        }

        "convert invalid message from connect data" {
            shouldThrow<DataException> {
                converter.fromConnectData("<test>", null, Object())
            }.also {
                it.message should startWith(
                        "Value must be instance of com.google.protobuf.MessageLite"
                )
            }
        }
    }
}