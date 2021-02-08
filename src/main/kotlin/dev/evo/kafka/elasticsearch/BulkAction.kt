package dev.evo.kafka.elasticsearch

import com.google.protobuf.Message
import com.google.protobuf.util.JsonFormat

import kotlinx.serialization.encodeToString
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.buildClassSerialDescriptor
import kotlinx.serialization.descriptors.element
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.encoding.decodeStructure
import kotlinx.serialization.encoding.encodeStructure
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement

/**
 * Represents Elasticsearch's bulk action:
 * https://www.elastic.co/guide/en/elasticsearch/reference/7.10/docs-bulk.html
 *
 * @param meta a meta data of a bulk action
 * @param source an optional source
 */
class BulkAction(
    val meta: BulkMeta,
    val source: BulkSource?,
) {
    init {
        if (
            source == null && (
                meta is BulkMeta.Index ||
                    meta is BulkMeta.Update ||
                    meta is BulkMeta.Create
                )
        ) {
            throw IllegalArgumentException(
                "Source is required for ${meta::class.simpleName?.toLowerCase()} action"
            )
        }

    }

    fun write(writer: Appendable) {
        writer.append(Json.encodeToString(BulkMetaSerializer, meta))
        writer.append("\n")
        if (source != null) {
            source.write(writer)
            writer.append("\n")
        }
    }
}

/**
 * Contains a meta data of a bulk action.
 *
 * @param index a name of an index; it will be overridden by the sink connector
 * @param id a unique document identifier, can be `null` to automatically generate id
 * @param type a document type
 * @param routing a routing key specifies a shard for a document
 */
sealed class BulkMeta {
    abstract val id: String?
    abstract val type: String?
    abstract var index: String?
    abstract val routing: String?

    @Serializable
    data class Index(
        @SerialName("_id")
        override val id: String? = null,
        @SerialName("_type")
        override val type: String? = null,
        @SerialName("_index")
        override var index: String? = null,
        override val routing: String? = null,
    ) : BulkMeta()

    @Serializable
    data class Delete(
        @SerialName("_id")
        override val id: String,
        @SerialName("_type")
        override val type: String? = null,
        @SerialName("_index")
        override var index: String? = null,
        override val routing: String? = null,
    ) : BulkMeta()

    @Serializable
    data class Update(
        @SerialName("_id")
        override val id: String,
        @SerialName("_type")
        override val type: String? = null,
        @SerialName("_index")
        override var index: String? = null,
        override val routing: String? = null,
        @SerialName("retry_on_conflict")
        val retryOnConflict: Int? = null,
    ) : BulkMeta()

    @Serializable
    data class Create(
        @SerialName("_id")
        override val id: String? = null,
        @SerialName("_type")
        override val type: String? = null,
        @SerialName("_index")
        override var index: String? = null,
        override val routing: String? = null,
    ) : BulkMeta()
}

/**
 * A serializer for a [BulkMeta].
 * The serializer is polymorphic, it wraps content by an operation type.
 */
object BulkMetaSerializer : KSerializer<BulkMeta> {
    override val descriptor: SerialDescriptor = buildClassSerialDescriptor(BulkMeta::class.qualifiedName!!) {
        element<BulkMeta.Index>("index")
        element<BulkMeta.Delete>("delete")
        element<BulkMeta.Update>("update")
        element<BulkMeta.Create>("create")
    }
    private val indexSerializer = BulkMeta.Index.serializer()
    private val deleteSerializer = BulkMeta.Delete.serializer()
    private val updateSerializer = BulkMeta.Update.serializer()
    private val createSerializer = BulkMeta.Create.serializer()

    override fun serialize(encoder: Encoder, value: BulkMeta) {
        encoder.encodeStructure(descriptor) {
            when (value) {
                is BulkMeta.Index -> encodeSerializableElement(
                    descriptor, 0, indexSerializer, value
                )
                is BulkMeta.Delete -> encodeSerializableElement(
                    descriptor, 1, deleteSerializer, value
                )
                is BulkMeta.Update -> encodeSerializableElement(
                    descriptor, 2, updateSerializer, value
                )
                is BulkMeta.Create -> encodeSerializableElement(
                    descriptor, 3, createSerializer, value
                )
            }
        }
    }

    override fun deserialize(decoder: Decoder): BulkMeta {
        return decoder.decodeStructure(descriptor) {
            val bulkTypeIx = decodeElementIndex(descriptor)
            val subSerializer = when (bulkTypeIx) {
                0 -> indexSerializer
                1 -> deleteSerializer
                2 -> updateSerializer
                3 -> createSerializer
                else -> error("Unexpected element index: $bulkTypeIx")
            }
            decodeSerializableElement(subSerializer.descriptor, bulkTypeIx, subSerializer)
        }
    }
}

/**
 * A source of a bulk action.
 */
interface BulkSource {
    fun write(writer: Appendable)
}

/**
 * Represents JSON source.
 */
data class JsonSource(private val source: JsonElement) : BulkSource {
    private val json = Json.Default

    override fun write(writer: Appendable) {
        writer.append(json.encodeToString(source))
    }
}

/**
 * Represents protobuf source.
 *
 * @param source a protobuf message that contains action source
 * @param includeDefaultValues also serializes default field values
 */
data class ProtobufSource(
    private val source: Message,
    private val includeDefaultValues: Boolean = true,
) : BulkSource {
    private val jsonPrinter = JsonFormat.printer()
        .omittingInsignificantWhitespace()
        .preservingProtoFieldNames()
        .run {
            if (includeDefaultValues) {
                includingDefaultValueFields()
            } else {
                this
            }
        }

    override fun write(writer: Appendable) {
        jsonPrinter.appendTo(source, writer)
    }
}
