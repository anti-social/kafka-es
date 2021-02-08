package dev.evo.kafka.elasticsearch

import com.google.protobuf.InvalidProtocolBufferException
import com.google.protobuf.Message
import com.google.protobuf.MessageLite
import com.google.protobuf.Parser

import java.lang.invoke.MethodHandles
import java.lang.invoke.MethodType

import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.header.Headers
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaAndValue
import org.apache.kafka.connect.errors.DataException
import org.apache.kafka.connect.storage.Converter

class ProtobufConverter : Converter {
    private lateinit var parser: Parser<*>
    private lateinit var actionHeaderKey: String

    class Config(props: MutableMap<String, *>) :
            AbstractConfig(CONFIG, props)
    {
        companion object {
            val PROTOBUF_CLASS = "protobuf.class"
            val ACTION_HEADER_KEY = "action.header.key"

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
                    "action",
                    ConfigDef.Importance.LOW,
                    "Header key where action meta information will be stored."
                )

            }
        }
    }

    override fun configure(configs: MutableMap<String, *>, isKey: Boolean) {
        val config = Config(configs)

        actionHeaderKey = config.getString(Config.ACTION_HEADER_KEY)

        val protoClass = config.getClass(Config.PROTOBUF_CLASS)
        val lookup = MethodHandles.lookup()
        val parserGetterName = "parser"
        val parserForTypeMethod = try {
            lookup.findStatic(
                protoClass,
                parserGetterName,
                MethodType.methodType(Parser::class.java)
            )
        } catch (e: NoSuchMethodException) {
            throw ConfigException(
                "${protoClass.canonicalName} class has no method $parserGetterName", e
            )
        }
        parser = parserForTypeMethod.invoke() as Parser<*>

    }

    override fun fromConnectData(topic: String, schema: Schema?, value: Any?): ByteArray? {
        if (value == null) {
            return null
        }
        if (value !is MessageLite) {
            throw DataException("Value must be an instance of com.google.protobuf.MessageLite")
        }
        return value.toByteArray()
    }

    override fun toConnectData(topic: String, headers: Headers?, value: ByteArray?): SchemaAndValue {
        val actionHeader = headers?.lastHeader(actionHeaderKey)
            ?: throw DataException("Headers must contain [$actionHeaderKey] key")
        val actionMeta = BulkActionProto.BulkAction.parseFrom(actionHeader.value())

        val index = actionMeta.index.ifEmpty { null }
        val type = actionMeta.type.ifEmpty { null }
        val routing = actionMeta.routing.ifEmpty { null }
        val bulkMeta = when (actionMeta.opType) {
            BulkActionProto.BulkAction.OpType.INDEX -> {
                BulkMeta.Index(
                    index = index,
                    id = actionMeta.id.ifEmpty { null },
                    type = type,
                    routing = routing,
                )
            }
            BulkActionProto.BulkAction.OpType.DELETE -> {
                BulkMeta.Delete(
                    index = index,
                    id = actionMeta.id,
                    type = type,
                    routing = routing,
                )
            }
            BulkActionProto.BulkAction.OpType.UNRECOGNIZED, null -> {
                throw IllegalArgumentException("Unrecognized operation type for bulk action")
            }
        }
        val source = if (actionMeta.opType == BulkActionProto.BulkAction.OpType.DELETE) {
            null
        } else {
            parseSource(requireNotNull(value))
        }
        return SchemaAndValue(null, BulkAction(bulkMeta, source))
    }

    override fun toConnectData(topic: String, value: ByteArray): SchemaAndValue {
        throw NotImplementedError("Headers are required")
    }

    private fun parseSource(value: ByteArray): ProtobufSource {
        try {
            return ProtobufSource(parser.parseFrom(value) as Message)
        } catch (e: InvalidProtocolBufferException) {
            throw DataException("Cannot deserialize data", e)
        }
    }
}
