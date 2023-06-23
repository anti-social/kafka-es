package dev.evo.kafka.elasticsearch

interface MetricsUpdater {
    suspend fun onSend(
        connectorName: String, taskId: Int, bytesSent: Long
    )

    suspend fun onSuccess(
        connectorName: String, taskId: Int, sendBulkResult: SendBulkResult.Success<*, *>
    )

    suspend fun onError(connectorName: String, taskId: Int)

    suspend fun onTimeout(connectorName: String, taskId: Int)
}
