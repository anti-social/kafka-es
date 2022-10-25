package dev.evo.kafka.serde

import com.google.protobuf.InvalidProtocolBufferException
import com.google.protobuf.MessageLite
import com.google.protobuf.Parser

import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.connect.errors.DataException

import java.lang.invoke.MethodHandles
import java.lang.invoke.MethodType

class ProtobufSerde : Serdes.WrapperSerde<MessageLite>(ProtobufSerializer(), ProtobufDeserializer())

class ProtobufSerializer : Serializer<MessageLite> {
    override fun serialize(topic: String?, data: MessageLite?): ByteArray? {
        return data?.toByteArray()
    }
}

class ProtobufDeserializer : Deserializer<MessageLite> {
    private lateinit var parser: Parser<*>

    class Config(props: MutableMap<String, *>) : AbstractConfig(CONFIG, props) {
        companion object {
            val PROTOBUF_CLASS = "protobuf.class"

            val CONFIG = ConfigDef().apply {
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

    override fun deserialize(topic: String?, data: ByteArray?): MessageLite? {
        if (data == null) return null

        return try {
            parser.parseFrom(data) as MessageLite
        } catch (e: InvalidProtocolBufferException) {
            throw DataException("Error when parsing protobuf data", e)
        }
    }
}