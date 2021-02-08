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
}

class KafkaEsMetrics : PrometheusMetrics() {
    val bulksCount by counterLong(
        "bulk_requests_count",
        "Number of bulk requests successfully sent to Elasticsearch",
        labelsFactory = ::KafkaEsLabels,
    )
    val bulksTime by counterLong(
        "bulk_requests_time_ms",
        "Total time of bulk requests successfully sent to Elasticsearch",
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
}
