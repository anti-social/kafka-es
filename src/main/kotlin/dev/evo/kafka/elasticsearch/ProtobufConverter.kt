package dev.evo.kafka.elasticsearch

import com.google.protobuf.InvalidProtocolBufferException
import com.google.protobuf.Message
import com.google.protobuf.MessageLite

import dev.evo.kafka.serde.ProtobufDeserializer
import dev.evo.kafka.serde.ProtobufSerializer

import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.header.Headers
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaAndValue
import org.apache.kafka.connect.errors.DataException

class ProtobufConverter : BaseConverter() {
    private val serializer = ProtobufSerializer()
    private val deserializer = ProtobufDeserializer()

    override lateinit var actionHeaderKey: String
    override lateinit var tagHeaderKey: String
    override lateinit var tagConfigValue: String

    class Config(props: MutableMap<String, *>) : AbstractConfig(CONFIG, props) {
        companion object {
            val PROTOBUF_CLASS = "protobuf.class"

            val CONFIG = baseConfigDef().apply {
                define(
                    PROTOBUF_CLASS,
                    ConfigDef.Type.CLASS,
                    ConfigDef.Importance.HIGH,
                    "The full path to the protobuf class."
                )
            }
        }
    }

    override fun configure(configs: MutableMap<String, *>, isKey: Boolean) {
        val config = Config(configs)

        actionHeaderKey = config.getString(ACTION_HEADER_KEY)
        tagHeaderKey = config.getString(TAG_HEADER_KEY)
        tagConfigValue = config.getString(VALUE_CONVERTER_TAG)

        serializer.configure(configs, isKey)
        deserializer.configure(configs, isKey)
    }

    override fun fromConnectData(topic: String, schema: Schema?, value: Any?): ByteArray? {
        if (value == null) {
            return null
        }
        if (value !is MessageLite) {
            throw DataException("Value must be an instance of com.google.protobuf.MessageLite")
        }

        return serializer.serialize(null, value)
    }

    override fun toConnectData(topic: String, headers: Headers?, value: ByteArray?): SchemaAndValue {
        val actionHeader = headers?.lastHeader(actionHeaderKey)
            ?: throw DataException("Headers must contain [$actionHeaderKey] key")
        val actionMeta = BulkActionProto.BulkAction.parseFrom(actionHeader.value())

        if (shouldSkipMessage(headers)) {
            return SchemaAndValue(null, null)
        }

        val index = actionMeta.index.ifEmpty { null }
        val type = actionMeta.type.ifEmpty { null }
        val routing = actionMeta.routing.ifEmpty { null }
        val parent = actionMeta.parent.ifEmpty { null }
        val bulkAction = when (actionMeta.opType) {
            BulkActionProto.BulkAction.OpType.INDEX -> {
                BulkAction.Index(
                    index = index,
                    id = actionMeta.id.ifEmpty { null },
                    type = type,
                    routing = routing,
                    parent = parent,
                    source = parseSource(value)
                )
            }
            BulkActionProto.BulkAction.OpType.DELETE -> {
                BulkAction.Delete(
                    index = index,
                    id = actionMeta.id,
                    type = type,
                    routing = routing,
                    parent = parent,
                )
            }
            BulkActionProto.BulkAction.OpType.UNRECOGNIZED, null -> {
                throw IllegalArgumentException("Unrecognized operation type for bulk action")
            }
        }
        return SchemaAndValue(null, bulkAction)
    }

    override fun toConnectData(topic: String, value: ByteArray): SchemaAndValue {
        throw NotImplementedError("Headers are required")
    }

    private fun parseSource(value: ByteArray?): ProtobufSource {
        requireNotNull(value) {
            "Message value must be present"
        }

        try {
            return ProtobufSource(deserializer.deserialize(null, value) as Message)
        } catch (e: InvalidProtocolBufferException) {
            throw DataException("Cannot deserialize data", e)
        }
    }
}
