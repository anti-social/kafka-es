package company.evo.kafka

import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.errors.DataException

import org.assertj.core.api.Assertions.*

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.BeforeEach

import company.evo.kafka.TestProto.TestDocument


class ProtobufConverterTests {
    private val converter = ProtobufConverter()
    private val testMsg = TestDocument.newBuilder()
            .setId(123)
            .setName("Teo")
            .build()

    @BeforeEach
    fun configureConverter() {
        converter.configure(
                mutableMapOf("protobuf.class" to "company.evo.kafka.TestProto\$TestDocument"),
                false
        )
    }

    @Test
    fun testRequiredConfiguration() {
        assertThatThrownBy {
            converter.configure(mutableMapOf<String, Any>(), false)
        }
                .isInstanceOf(ConfigException::class.java)
                .hasMessageStartingWith(
                        "Missing required configuration \"protobuf.class\"")
    }

    @Test
    fun testMissingProtobufClass() {
        assertThatThrownBy {
            converter.configure(
                    mutableMapOf("protobuf.class" to "company.evo.kafka.UnknownMessage"),
                    false)
        }
                .isInstanceOf(ConfigException::class.java)
                .hasMessageContaining(
                        "Class company.evo.kafka.UnknownMessage could not be found")
    }

    @Test
    fun testInvalidProtobufClass() {
        assertThatThrownBy {
            converter.configure(
                    mutableMapOf("protobuf.class" to "company.evo.kafka.ProtobufConverter"),
                    false)
        }
                .isInstanceOf(ConfigException::class.java)
                .hasMessageContaining(
                        "company.evo.kafka.ProtobufConverter class has no method parseFrom")
    }

    @Test
    fun testToConnectData() {
        val data = testMsg.toByteArray()
        val schemaAndValue = converter.toConnectData("<test>", data)
        assertThat(schemaAndValue.schema()).isNull()
        val value = schemaAndValue.value()
        if (value !is TestDocument) {
            throw AssertionError("$value must be an instance of ${TestDocument::class.java}")
        }
        assertThat(value.id).isEqualTo(123)
        assertThat(value.name).isEqualTo("Teo")
    }

    @Test
    fun testToConnectDataInvalidData() {
        assertThatThrownBy {
            converter.toConnectData("<test>", "invalid data".toByteArray())
        }
                .isInstanceOf(DataException::class.java)
                .hasMessageStartingWith("Cannot deserialize data")
    }

    @Test
    fun testFromConnectData() {
        val data = converter.fromConnectData("<test>", null, testMsg)
        val value = TestDocument.parseFrom(data)
        assertThat(value.id).isEqualTo(123)
        assertThat(value.name).isEqualTo("Teo")
    }

    @Test
    fun testFromConnectDataInvalidMessage() {
        assertThatThrownBy {
            converter.fromConnectData("<test>", null, Object())
        }
                .isInstanceOf(DataException::class.java)
                .hasMessageStartingWith(
                        "Value must be instance of com.google.protobuf.MessageLite"
                )
    }
}