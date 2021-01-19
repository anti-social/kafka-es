package dev.evo.kafka

import java.lang.reflect.Method

import com.google.protobuf.InvalidProtocolBufferException
import com.google.protobuf.MessageLite

import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaAndValue
import org.apache.kafka.connect.errors.DataException
import org.apache.kafka.connect.storage.Converter
import java.lang.reflect.InvocationTargetException


class ProtobufConverter : Converter {
    lateinit private var config: Config
    lateinit private var parser: Method

    class Config(props: MutableMap<String, *>) :
            AbstractConfig(CONFIG, props)
    {
        companion object {
            val PROTOBUF_CLASS = "protobuf.class"

            val CONFIG = ConfigDef()
            init {
                CONFIG.define(
                        PROTOBUF_CLASS,
                        ConfigDef.Type.CLASS,
                        ConfigDef.Importance.HIGH,
                        "The full path to the protobuf class."
                )
            }
        }
    }

    override fun configure(configs: MutableMap<String, *>, isKey: Boolean) {
        config = Config(configs)
        val protoClass = config.getClass(Config.PROTOBUF_CLASS)
        parser = try {
            protoClass.getMethod("parseFrom", ByteArray::class.java)
        } catch (e: NoSuchMethodException) {
            throw ConfigException(
                    "${protoClass.canonicalName} class has no method parseFrom", e)
        }
    }

    override fun fromConnectData(topic: String, schema: Schema?, value: Any?): ByteArray? {
        if (value == null) {
            return null
        }
        if (value !is MessageLite) {
            throw DataException("Value must be instance of com.google.protobuf.MessageLite")
        }
        return value.toByteArray()
    }

    override fun toConnectData(topic: String, value: ByteArray?): SchemaAndValue {
        if (value == null) {
            return SchemaAndValue.NULL
        }
        try {
            val msg = parser.invoke(null, value)
            return SchemaAndValue(null, msg)
        } catch (e: InvocationTargetException) {
            throw DataException("Cannot deserialize data: ${e.targetException}",
                    e.targetException)
        }
    }
}
