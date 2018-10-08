package company.evo.bulk.elasticsearch

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.jsontype.impl.TypeIdResolverBase

import java.io.OutputStream

import kotlin.sequences.sequence

@JsonTypeInfo(use = JsonTypeInfo.Id.CUSTOM, include = JsonTypeInfo.As.WRAPPER_OBJECT)
@JsonTypeIdResolver(BulkActionOperationResolver::class)
data class BulkAction(
        @JsonIgnore val operation: Operation,
        @JsonProperty("_index") val index: String,
        @JsonProperty("_type") val type: String,
        @JsonProperty("_id") val id: String,
        @JsonInclude(value = JsonInclude.Include.NON_EMPTY) val routing: String? = null,
        @JsonInclude(value = JsonInclude.Include.NON_EMPTY) val parent: String? = null,
        @JsonInclude(value = JsonInclude.Include.NON_EMPTY) val version: String? = null,
        @JsonInclude(value = JsonInclude.Include.NON_EMPTY) val versionType: String? = null,
        @JsonInclude(value = JsonInclude.Include.NON_EMPTY) val retryOnConflict: Int? = null,
        @JsonIgnore val source: Any? = null
) {
    companion object {
        private val NEW_LINE = byteArrayOf('\n'.toByte())
    }

    enum class Operation(val value: String) {
        INDEX("index"), CREATE("create"), UPDATE("update"), DELETE("delete")
    }

    init {
        if (operation == Operation.DELETE && source != null) {
            throw IllegalArgumentException("delete operation doesn't support source")
        }
        if (operation != Operation.DELETE && source == null) {
            throw IllegalArgumentException("$operation requires source")
        }
    }

    fun writeTo(objectMapper: ObjectMapper, out: OutputStream) {
        objectMapper.writeValue(out, this)
        if (source != null) {
            out.write(NEW_LINE)
            objectMapper.writeValue(out, source)
        }
        out.write(NEW_LINE)
    }
}

private class BulkActionOperationResolver : TypeIdResolverBase() {
    override fun idFromValue(value: Any?): String {
        val bulkAction = value as? BulkAction ?:
        throw IllegalArgumentException(
                "value must be an instance of ${BulkAction::class}, but was ${value?.javaClass}"
        )
        return bulkAction.operation.value
    }

    override fun getMechanism(): JsonTypeInfo.Id {
        TODO("not supported")
    }

    override fun idFromValueAndType(value: Any?, suggestedType: Class<*>?): String {
        TODO("not supported")
    }
}

data class BulkResult(
        @JsonProperty(required = true) val took: Int,
        @JsonProperty(required = true) val errors: Boolean,
        @JsonProperty(required = true) val items: List<Item>
) {
    @JsonDeserialize(using = BulkItemDeserializer::class)
    data class Item(
            val operation: String,
            val value: ItemValue
    ) {
        val index get() = value.index
        val type get() = value.type
        val id get() = value.id
        val version get() = value.version
        val status get() = value.status
        val result get() = value.result
        val error get() = value.error
    }

    class BulkItemDeserializer : StdDeserializer<Item>(Item::class.java) {
        override fun deserialize(p: JsonParser, ctx: DeserializationContext): Item {
            val operation = when (val fieldName = p.nextFieldName()) {
                "index", "delete", "update", "create" -> fieldName
                else -> throw ctx.weirdKeyException(
                        String::class.java, fieldName,
                        "Operation must be on of [index, delete, update, create]"
                )
            }

            checkToken(p, ctx, JsonToken.START_OBJECT)
            val value = p.readValueAs(ItemValue::class.java)
            checkToken(p, ctx, JsonToken.END_OBJECT)

            return Item(operation, value)
        }

        private fun checkToken(p: JsonParser, ctx: DeserializationContext, token: JsonToken) {
            if (p.nextToken() != token) {
                throw ctx.wrongTokenException(p, ItemValue::class.java, token, null)
            }
        }
    }

    data class ItemValue (
            @JsonProperty("_index") val index: String,
            @JsonProperty("_type") val type: String,
            @JsonProperty("_id") val id: String,
            @JsonProperty("_version") val version: Int,
            val status: Int,
            val result: String?,
            val error: ItemError?
    )

    data class ItemError(
            val type: String,
            val reason: String,
            val index: String?,
            @JsonProperty("index_uuid") val indexUuid: String?,
            @JsonProperty("caused_by") val causedBy: CausedBy?
    )

    data class CausedBy(
            val type: String,
            val reason: String
    )

    fun getFailedItems(): Sequence<Item> = sequence {
        items.filter { it.value.error != null }.forEach {
            yield(it)
        }
    }
}
