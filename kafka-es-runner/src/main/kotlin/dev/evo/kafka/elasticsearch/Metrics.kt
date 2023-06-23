package dev.evo.kafka.elasticsearch

import dev.evo.prometheus.LabelSet
import dev.evo.prometheus.PrometheusMetrics
import dev.evo.prometheus.jvm.DefaultJvmMetrics

object Metrics : PrometheusMetrics() {
    val jvm by submetrics(DefaultJvmMetrics())
    val kafkaEsMetrics by submetrics("kafka_es", KafkaEsMetrics())
}

class KafkaEsLabels : LabelSet() {
    var connectorName by label("connector_name")
    var taskId by label("task_id")

    fun populate(connectorName: String, taskId: Int) {
        this.connectorName = connectorName
        this.taskId = taskId.toString()
    }
}

class KafkaEsMetrics : PrometheusMetrics(), MetricsUpdater {
    val bulksCount by counterLong(
        "bulk_requests_count",
        "Number of bulk requests successfully sent to Elasticsearch",
        labelsFactory = ::KafkaEsLabels,
    )
    val bulksTotalTime by counterLong(
        "bulk_requests_total_time_ms",
        "Total time of bulk requests successfully sent to Elasticsearch",
        labelsFactory = ::KafkaEsLabels,
    )
    val bulksTookTime by counterLong(
        "bulk_requests_took_time_ms",
        "Time of bulk requests measured by Elasticsearch itself",
        labelsFactory = ::KafkaEsLabels,
    )
    val bulkActionsCount by counterLong(
        "bulk_actions_count",
        "Number of bulk actions successfully sent to Elasticsearch",
        labelsFactory = ::KafkaEsLabels,
    )
    val bulkBytesSent by counterLong(
        "bulk_bytes_sent",
        "Number of bytes sent to Elasticsearch",
        labelsFactory = ::KafkaEsLabels,
    )
    val bulksErrorCount by counterLong(
        "bulk_requests_error_count",
        "Number of failed bulk requests",
        labelsFactory = ::KafkaEsLabels,
    )
    val bulksTimeoutCount by counterLong(
        "bulk_requests_timeout_count",
        "Number of timed out bulk requests",
        labelsFactory = ::KafkaEsLabels,
    )

    override suspend fun onSend(
        connectorName: String, taskId: Int, bytesSent: Long
    ) {
        bulkBytesSent.add(bytesSent)
    }

    override suspend fun onSuccess(
        connectorName: String, taskId: Int, sendBulkResult: SendBulkResult.Success<*, *>
    ) {
        bulksCount.inc { populate(connectorName, taskId) }
        bulksTotalTime.add(sendBulkResult.totalTimeMs) { populate(connectorName, taskId) }
        bulksTookTime.add(sendBulkResult.tookTimeMs) { populate(connectorName, taskId) }
        bulkActionsCount.add(sendBulkResult.successActionsCount) { populate(connectorName, taskId) }
    }

    override suspend fun onError(connectorName: String, taskId: Int) {
        bulksErrorCount.inc { populate(connectorName, taskId) }
    }

    override suspend fun onTimeout(connectorName: String, taskId: Int) {
        bulksTimeoutCount.inc { populate(connectorName, taskId) }
    }
}
