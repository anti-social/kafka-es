package dev.evo.kafka.elasticsearch

import org.apache.kafka.common.header.Headers
import org.apache.kafka.connect.storage.Converter
import org.apache.kafka.common.config.ConfigDef

abstract class BaseConverter : Converter {
    protected lateinit var actionHeaderKey: String
    protected lateinit var tagHeaderKey: String
    protected lateinit var tagConfigValue: String;

    companion object {
        val ACTION_HEADER_KEY = "action.header.key"
        val DEFAULT_ACTION_HEADER = "action"
        val TAG_HEADER_KEY = "tag.header.key"
        val VALUE_CONVERTER_TAG = "value.converter.tag"

        fun baseConfigDef(): ConfigDef {
            return ConfigDef().apply {
                define(
                    ACTION_HEADER_KEY,
                    ConfigDef.Type.STRING,
                    DEFAULT_ACTION_HEADER,
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

    /**
     * Skip message if value.converter.tag is set in config, tag header is present and does not match config value.
     *
     * If not "tag" header present, message will be processed as usual. Such default behavior is safe and won't break
     * existing messages.
     */
    protected fun shouldSkipMessage(headers: Headers): Boolean {
        if (tagConfigValue.isEmpty()) {
            return false
        }
        val tags = headers.headers(tagHeaderKey).map { it.value().toString(Charsets.UTF_8) }.toSet()
        if (tags.isEmpty()) {
            return false
        }
        return !tags.contains(tagConfigValue)
    }
}