package dev.evo.kafka.elasticsearch

import dev.evo.elasticmagic.serde.Serde
import dev.evo.elasticmagic.transport.Method
import dev.evo.elasticmagic.transport.Parameters
import dev.evo.elasticmagic.transport.Request
import dev.evo.elasticmagic.transport.RequestEncoder

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonObject

class BulkRequest<R>(
    method: Method,
    path: String,
    parameters: Parameters = emptyMap(),
    body: List<BulkAction>,
    processResult: (JsonObject) -> R
) : Request<List<BulkAction>, JsonObject, R>(
    method,
    path,
    parameters = parameters,
    body = body,
    processResult = processResult
) {
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

    override fun serializeRequest(encoder: RequestEncoder) {
        val body = body
        if (body != null) {
            for (action in body) {
                action.write(encoder)
            }
        }
    }

    override fun deserializeResponse(response: String, serde: Serde): JsonObject {
        // HEAD requests return empty response body
        return Json.decodeFromString(JsonElement.serializer(), response.ifEmpty { "{}" }).jsonObject
    }
}
