package dev.evo.kafka.elasticsearch

import dev.evo.elasticmagic.serde.Serde
import dev.evo.elasticmagic.serde.kotlinx.JsonSerde
import dev.evo.elasticmagic.transport.Method
import dev.evo.elasticmagic.transport.Parameters
import dev.evo.elasticmagic.transport.PlainResponse
import dev.evo.elasticmagic.transport.Request
import dev.evo.elasticmagic.transport.ContentEncoder

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonObject

class BulkRequest<R>(
    override val method: Method,
    override val path: String,
    override val parameters: Parameters = emptyMap(),
    override val body: List<BulkAction>,
    override val processResponse: (JsonObject) -> R,
) : Request<List<BulkAction>, JsonObject, R>() {
    companion object {
        operator fun invoke(
            method: Method,
            path: String,
            parameters: Parameters = emptyMap(),
            body: List<BulkAction>,
        ): BulkRequest<JsonObject> {
            return BulkRequest(method, path, parameters = parameters, body = body) { res -> res}
        }
    }

    override val contentType = "application/x-ndjson"
    override val errorSerde = JsonSerde

    override fun serializeRequest(encoder: ContentEncoder) {
        for (action in body) {
            action.write(encoder)
        }
    }

    override fun deserializeResponse(response: PlainResponse): JsonObject {
        // HEAD requests return empty response body
        return Json.decodeFromString(JsonElement.serializer(), response.content.ifEmpty { "{}" }).jsonObject
    }
}
