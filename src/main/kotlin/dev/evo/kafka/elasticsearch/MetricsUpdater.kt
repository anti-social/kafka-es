package dev.evo.kafka.elasticsearch

interface MetricsUpdater {
    suspend fun onSuccess(
        connectorName: String, taskId: Int, sendBulkResult: SendBulkResult.Success<*, *>
    )

    suspend fun onError(connectorName: String, taskId: Int)

    suspend fun onTimeout(connectorName: String, taskId: Int)
}
