package dev.evo.kafka.elasticsearch

import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement

import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.header.Headers
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaAndValue
import org.apache.kafka.connect.errors.DataException
import org.apache.kafka.connect.storage.Converter

class JsonConverter : Converter {
    private val json = Json.Default
    private lateinit var actionHeaderKey: String

    class Config(props: MutableMap<String, *>) : AbstractConfig(CONFIG, props) {
        companion object {
            val ACTION_HEADER_KEY = "action.header.key"

            val CONFIG = ConfigDef().apply {
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
    }

    override fun fromConnectData(topic: String, schema: Schema?, value: Any?): ByteArray? {
        if (value == null) {
            return null
        }
        return json.encodeToString(value as JsonElement).toByteArray(Charsets.UTF_8)
    }

    override fun toConnectData(topic: String, headers: Headers?, value: ByteArray?): SchemaAndValue {
        val actionHeader = headers?.lastHeader(actionHeaderKey)
            ?: throw DataException("Headers must contain [$actionHeaderKey] key")
        val bulkAction = try {
            val meta = json.decodeFromString(
                BulkMetaSerializer,
                actionHeader.value().toString(Charsets.UTF_8)
            )
            when (meta) {
                is BulkMeta.Index -> BulkAction.Index(meta, parseSource(value))
                is BulkMeta.Delete -> BulkAction.Delete(meta)
                is BulkMeta.Update -> BulkAction.Update(meta, parseSource(value))
                is BulkMeta.Create -> BulkAction.Create(meta, parseSource(value))
            }
        } catch (e: Throwable) {
            throw DataException("Unable to deserialize Elasticsearch action", e)
        }
        return SchemaAndValue(null, bulkAction)
    }

    override fun toConnectData(topic: String, value: ByteArray): SchemaAndValue {
        throw NotImplementedError("Headers are required")
    }

    private fun parseSource(value: ByteArray?): JsonSource {
        requireNotNull(value) {
            "Message value must be present"
        }
        return JsonSource(json.parseToJsonElement(value.toString(Charsets.UTF_8)))
    }
}
