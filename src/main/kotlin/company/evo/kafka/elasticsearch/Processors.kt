package company.evo.kafka.elasticsearch

import com.google.protobuf.Message
import com.google.protobuf.MessageOrBuilder
import com.google.protobuf.util.JsonFormat

import company.evo.bulk.elasticsearch.BulkAction
import company.evo.castOrFail
import company.evo.kafka.elasticsearch.BulkActionProto


interface Processor {
    fun process(value: Any, index: String?): BulkAction
}

class JsonProcessor : Processor {
    override fun process(value: Any, index: String?): BulkAction {
        val valueOrPayload = castOrFail<Map<*, *>>(value)
        val payload: Map<*, *> = if (valueOrPayload.containsKey("payload")) {
            castOrFail(valueOrPayload["payload"], "payload")
        } else {
            valueOrPayload
        }
        val actionData: Map<*, *> = castOrFail(payload["action"], "action")
        val actionEntry = actionData.iterator().next()
        val opType = castOrFail<String>(actionEntry.key)
        val actionMeta = castOrFail<Map<*, *>>(actionEntry.value)
        val indexName = if (index?.isNotEmpty() == true) {
            index
        } else {
            castOrFail(actionMeta["_index"])
        }
        val source = when (opType) {
            "index", "create", "update" -> {
                val sourceData: Map<*, *> = castOrFail(payload["source"], "source")
                sourceData
            }
            "delete" -> {
                null
            }
            else -> {
                throw IllegalArgumentException(
                        "Expected one of the action [index, create, update, delete] " +
                                "but was [${actionEntry.key}]")
            }
        }
        return BulkAction(
                BulkAction.Operation.valueOf(opType),
                index = indexName,
                type = castOrFail(actionMeta["_type"]),
                id = castOrFail(actionMeta["_id"]),
                source = source
        )
    }
 }

class ProtobufProcessor(
        includeDefaultValues: Boolean = true
) : Processor {
    private val jsonPrinter: JsonFormat.Printer
    init {
        var jsonPrinter = JsonFormat.printer()
                .omittingInsignificantWhitespace()
                .preservingProtoFieldNames()
        if (includeDefaultValues) {
            jsonPrinter = jsonPrinter.includingDefaultValueFields()
        }
        this.jsonPrinter = jsonPrinter
    }

    override fun process(value: Any, index: String?): BulkAction {
        val message = castOrFail<MessageOrBuilder>(value)
        val descriptor = message.descriptorForType
        val actionField = descriptor.findFieldByName("action") ?:
                throw IllegalArgumentException("Message must contain [action] field")
        val action = message.getField(actionField) as? BulkActionProto.BulkAction ?:
                throw IllegalArgumentException(
                        "[action] field must be an instance of the ${BulkAction::class.java}")
        val indexName = if (!index.isNullOrEmpty()) {
            index
        } else {
            action.index
        }
        val source = when (action.opType) {
            BulkActionProto.BulkAction.OpType.INDEX,
            BulkActionProto.BulkAction.OpType.UPDATE,
            BulkActionProto.BulkAction.OpType.CREATE -> {
                val sourceField = descriptor.findFieldByName("source") ?:
                        throw IllegalArgumentException("Message must contain [source] field")
                val source = message.getField(sourceField) as? Message ?:
                        throw IllegalArgumentException(
                                "[source] field must be an instance of the ${Message::class.java}")
                jsonPrinter.print(source)
            }
            BulkActionProto.BulkAction.OpType.DELETE -> {
                null
            }
            BulkActionProto.BulkAction.OpType.UNRECOGNIZED,
            null -> {
                throw IllegalArgumentException("Unrecognized operation type for bulk action")
            }
        }
        return BulkAction(
                BulkAction.Operation.valueOf(action.opType.name),
                index = indexName,
                type = action.type,
                id = action.id,
                source = source
        )
    }
}
