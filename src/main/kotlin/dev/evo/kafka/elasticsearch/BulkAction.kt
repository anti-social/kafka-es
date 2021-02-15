package dev.evo.kafka.elasticsearch

import com.google.protobuf.MessageOrBuilder
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
 */
sealed class BulkAction {
    /**
     * A meta data of the bulk action
     */
    abstract val meta: BulkMeta

    abstract class BulkActionWithSource : BulkAction() {
        /**
         * A source of the bulk action
         */
        abstract var source: BulkSource
    }

    data class Index (
        override val meta: BulkMeta.Index,
        override var source: BulkSource,
    ) : BulkActionWithSource() {
        constructor(
            id: String? = null,
            type: String? = null,
            index: String? = null,
            routing: String? = null,
            parent: String? = null,
            source: BulkSource,
        ) : this(
            BulkMeta.Index(id, type, index, routing, parent),
            source,
        )
    }

    data class Delete(
        override val meta: BulkMeta.Delete,
    ) : BulkAction() {
        constructor(
            id: String,
            type: String? = null,
            index: String? = null,
            routing: String? = null,
            parent: String? = null,
        ) : this(
            BulkMeta.Delete(id, type, index, routing, parent)
        )
    }

    data class Update (
        override val meta: BulkMeta.Update,
        override var source: BulkSource,
    ) : BulkActionWithSource() {
        constructor(
            id: String,
            type: String? = null,
            index: String? = null,
            routing: String? = null,
            parent: String? = null,
            retryOnConflict: Int? = null,
            source: BulkSource,
        ) : this(
            BulkMeta.Update(id, type, index, routing, parent, retryOnConflict),
            source,
        )
    }

    data class Create (
        override val meta: BulkMeta.Create,
        override var source: BulkSource,
    ) : BulkActionWithSource() {
        constructor(
            id: String? = null,
            type: String? = null,
            index: String? = null,
            routing: String? = null,
            parent: String? = null,
            source: BulkSource,
        ) : this(
            BulkMeta.Create(id, type, index, routing, parent),
            source,
        )
    }

    fun write(writer: Appendable) {
        writer.append(Json.encodeToString(BulkMetaSerializer, meta))
        writer.append("\n")
        if (this is BulkActionWithSource) {
            source.write(writer)
            writer.append("\n")
        }
    }
}

/**
 * Contains a meta data of a bulk action.
 *
 * @param id a unique document identifier, can be `null` to automatically generate id
 * @param type a document type
 * @param index a name of an index; it will be overridden by the sink connector
 * @param routing a routing key specifies a shard for a document
 */
sealed class BulkMeta(
) {
    abstract var type: String?
    abstract var index: String?
    abstract var routing: String?
    abstract var parent: String?

    abstract fun id(): String?

    @Serializable
    data class Index(
        @SerialName("_id")
        var id: String? = null,
        @SerialName("_type")
        override var type: String? = null,
        @SerialName("_index")
        override var index: String? = null,
        override var routing: String? = null,
        override var parent: String? = null,
    ) : BulkMeta() {
        override fun id() = id
    }

    @Serializable
    data class Delete(
        @SerialName("_id")
        var id: String,
        @SerialName("_type")
        override var type: String? = null,
        @SerialName("_index")
        override var index: String? = null,
        override var routing: String? = null,
        override var parent: String? = null,
    ) : BulkMeta() {
        override fun id() = id
    }

    @Serializable
    data class Update(
        @SerialName("_id")
        var id: String,
        @SerialName("_type")
        override var type: String? = null,
        @SerialName("_index")
        override var index: String? = null,
        override var routing: String? = null,
        override var parent: String? = null,
        @SerialName("retry_on_conflict")
        val retryOnConflict: Int? = null,
    ) : BulkMeta() {
        override fun id() = id
    }

    @Serializable
    data class Create(
        @SerialName("_id")
        var id: String? = null,
        @SerialName("_type")
        override var type: String? = null,
        @SerialName("_index")
        override var index: String? = null,
        override var routing: String? = null,
        override var parent: String? = null,
    ) : BulkMeta() {
        override fun id() = id
    }
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
data class JsonSource(val source: JsonElement) : BulkSource {
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
    val source: MessageOrBuilder,
    val includeDefaultValues: Boolean = true,
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
