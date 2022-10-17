package dev.evo.kafka.elasticsearch

interface MetricsUpdater {
    suspend fun onSuccess(connectorName: String, sendBulkResult: SendBulkResult.Success<*, *>)

    suspend fun onError(connectorName: String)

    suspend fun onTimeout(connectorName: String)
}
