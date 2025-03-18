package dev.evo.kafka.elasticsearch

import org.apache.kafka.common.header.Headers
import org.apache.kafka.connect.storage.Converter

abstract class BaseConverter : Converter {
    companion object {
        val TAG_HEADER_KEY = "tag.header.key"
        val VALUE_CONVERTER_TAG = "value.converter.tag"
    }

    protected lateinit var tagHeaderKey: String
    protected lateinit var tagConfigValue: String;

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