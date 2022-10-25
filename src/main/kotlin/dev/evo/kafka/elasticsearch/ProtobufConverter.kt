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
import org.apache.kafka.connect.storage.Converter

class ProtobufConverter : Converter {
    private var actionHeaderKey: String = Config.DEFAULT_ACTION_HEADER
    private val serializer = ProtobufSerializer()
    private val deserializer = ProtobufDeserializer()

    class Config(props: MutableMap<String, *>) :
            AbstractConfig(CONFIG, props)
    {
        companion object {
            val PROTOBUF_CLASS = "protobuf.class"
            val ACTION_HEADER_KEY = "action.header.key"
            val DEFAULT_ACTION_HEADER = "action"

            val CONFIG = ConfigDef().apply {
                define(
                    PROTOBUF_CLASS,
                    ConfigDef.Type.CLASS,
                    ConfigDef.Importance.HIGH,
                    "The full path to the protobuf class."
                )
                define(
                    ACTION_HEADER_KEY,
                    ConfigDef.Type.STRING,
                    DEFAULT_ACTION_HEADER,
                    ConfigDef.Importance.LOW,
                    "Header key where action meta information will be stored."
                )

            }
        }
    }

    override fun configure(configs: MutableMap<String, *>, isKey: Boolean) {
        val config = Config(configs)

        actionHeaderKey = config.getString(Config.ACTION_HEADER_KEY)

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
