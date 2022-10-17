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

    fun populate(connectorName: String) {
        this.connectorName = connectorName
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

    override suspend fun onSuccess(connectorName: String, sendBulkResult: SendBulkResult.Success<*, *>) {
        bulksCount.inc { populate(connectorName) }
        bulksTotalTime.add(sendBulkResult.totalTimeMs) { populate(connectorName) }
        bulksTookTime.add(sendBulkResult.tookTimeMs) { populate(connectorName) }
        bulkActionsCount.add(sendBulkResult.successActionsCount) { populate(connectorName) }
    }

    override suspend fun onError(connectorName: String) {
        bulksErrorCount.inc { populate(connectorName) }
    }

    override suspend fun onTimeout(connectorName: String) {
        bulksTimeoutCount.inc { populate(connectorName) }
    }
}
