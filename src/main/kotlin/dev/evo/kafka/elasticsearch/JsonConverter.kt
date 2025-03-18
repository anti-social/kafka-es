package dev.evo.kafka.elasticsearch

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
    private lateinit var tagHeaderKey: String
    private lateinit var tagConfigValue: String;

    class Config(props: MutableMap<String, *>) : AbstractConfig(CONFIG, props) {
        companion object {
            val ACTION_HEADER_KEY = "action.header.key"
            val TAG_HEADER_KEY = "tag.header.key"
            val VALUE_CONVERTER_TAG = "value.converter.tag"

            val CONFIG = ConfigDef().apply {
                define(
                    ACTION_HEADER_KEY,
                    ConfigDef.Type.STRING,
                    "action",
                    ConfigDef.Importance.LOW,
                    "Header key where action meta information will be stored."
                )
                define(
                    TAG_HEADER_KEY,
                    ConfigDef.Type.STRING,
                    "tag",
                    ConfigDef.Importance.LOW,
                    "Header key where message tag will be stored."
                )
                define(
                    VALUE_CONVERTER_TAG,
                    ConfigDef.Type.STRING,
                    "",
                    ConfigDef.Importance.LOW,
                    "Tag for the value converter. If specified, only actions with the same tag header will be processed by this converter. " +
                            "You can specify different tag header name in 'tag.header.key' property."
                )
            }

        }
    }

    override fun configure(configs: MutableMap<String, *>, isKey: Boolean) {
        val config = Config(configs)
        actionHeaderKey = config.getString(Config.ACTION_HEADER_KEY)
        tagHeaderKey = config.getString(Config.TAG_HEADER_KEY)
        tagConfigValue = config.getString(Config.VALUE_CONVERTER_TAG)
    }

    override fun fromConnectData(topic: String, schema: Schema?, value: Any?): ByteArray? {
        if (value == null) {
            return null
        }
        return json
            .encodeToString(JsonElement.serializer(), value as JsonElement)
            .toByteArray(Charsets.UTF_8)
    }

    /**
     * Skip message if value.converter.tag is configured and either
     * - tag header does not present
     * - one or more tag headers present and none of them match value.converter.tag
     */
    private fun shouldSkipMessage(headers: Headers): Boolean {
        if (tagConfigValue.isEmpty()) {
            return false
        }
        val tags = headers.headers(tagHeaderKey).map { it.value().toString(Charsets.UTF_8) }.toSet()
        return !tags.contains(tagConfigValue)
    }

    override fun toConnectData(topic: String, headers: Headers?, value: ByteArray?): SchemaAndValue {
        val actionHeader = headers?.lastHeader(actionHeaderKey)
            ?: throw DataException("Headers must contain [$actionHeaderKey] key")

        if (shouldSkipMessage(headers)) {
            return SchemaAndValue(null, null)
        }

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
