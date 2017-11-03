package company.evo.elasticsearch

import com.google.gson.Gson
import com.google.gson.GsonBuilder
import com.google.gson.stream.JsonWriter
import com.google.protobuf.*
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType
import com.google.protobuf.util.Durations
import com.google.protobuf.util.JsonFormat
import com.google.protobuf.util.Timestamps
import company.evo.kafka.castOrFail

import company.evo.kafka.elasticsearch.BulkActionProto
import java.io.StringWriter
import java.util.*


internal fun processJsonMessage(value: Map<*, *>, index: String?): AnyBulkableAction {
    val payload: Map<*, *> = if (value.containsKey("payload")) {
        castOrFail(value["payload"], "payload")
    } else {
        value
    }
    val actionData: Map<*, *> = castOrFail(payload["action"], "action")
    val actionBuilder = AnyBulkableAction.Builder(actionData)
    val actionEntry = actionData.iterator().next()
    when (actionEntry.key) {
        "index", "create", "update" -> {
            val sourceData: Map<*, *> = castOrFail(payload["source"], "source")
            actionBuilder.setSource(sourceData)
        }
        "delete" -> {}
        else -> {
            throw IllegalArgumentException(
                    "Expected one of the action [index, create, update, delete] " +
                            "but was [${actionEntry.key}]")
        }
    }
    if (index != null && index.isNotEmpty()) {
        actionBuilder.index(index)
    }
    return actionBuilder.build()
 }

internal fun processProtobufMessage(
        message: Message, index: String?, includeDefaultValues: Boolean = false
): AnyBulkableAction
{
    val descriptor = message.descriptorForType
    val actionField = descriptor.findFieldByName("action") ?:
            throw IllegalArgumentException("Message has no action field")
    val sourceField = descriptor.findFieldByName("source") ?:
            throw IllegalArgumentException("Message has no source field")
    val action = message.getField(actionField) as? BulkActionProto.BulkAction ?:
            throw IllegalArgumentException(
                    "Action must be an instance of the " +
                            "company.evo.kafka.elasticsearch.BulkActionProto\$BulkAction class")
    val source = message.getField(sourceField) as? Message ?:
            throw IllegalArgumentException(
                    "Source must be an instance of the com.google.protobuf.Message class")
    val actionBuilder = AnyBulkableAction.Builder(action)
    if (index != null && index.isNotEmpty()) {
        actionBuilder.index(index)
    }
    var jsonPrinter = JsonFormat.printer()
            .omittingInsignificantWhitespace()
            .preservingProtoFieldNames()
    if (includeDefaultValues) {
        jsonPrinter = jsonPrinter.includingDefaultValueFields()
    }
    actionBuilder.setSource(jsonPrinter.print(source))
    return actionBuilder.build()
}
