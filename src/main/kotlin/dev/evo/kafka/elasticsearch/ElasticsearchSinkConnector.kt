package dev.evo.kafka.elasticsearch

import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.runtime.ConnectorConfig
import org.apache.kafka.connect.runtime.WorkerConfig
import org.apache.kafka.connect.sink.SinkConnector


class ElasticsearchSinkConnector : SinkConnector() {
    private lateinit var configProps: Map<String, String>

    override fun config(): ConfigDef {
        return Config.CONFIG
    }

    override fun taskConfigs(maxTasks: Int): MutableList<MutableMap<String, String>> {
        return (1..maxTasks).map { configProps.toMutableMap() }.toMutableList()
    }

    override fun taskClass(): Class<out Task> {
        return ElasticsearchSinkTask::class.java
    }

    override fun version(): String {
        return "unknown"
    }

    override fun start(props: MutableMap<String, String>) {
        try {
            Config(props)
            this.configProps = props.toMap()
        } catch (e: ConfigException) {
            throw ConnectException(
                "Couldn't start ${this::class.java} due to configuration error", e
            )
        }
    }

    override fun stop() {}
}

class Config(props: MutableMap<String, String>) : AbstractConfig(CONFIG, props) {
    companion object {
        val CONNECTION_URL = "connection.url"
        val COMPRESSION_ENABLED = "compression.enabled"
        val COMPRESSION_ENABLED_DEFAULT = false
        val INDEX = "index"
        val TOPIC_INDEX_MAP = "topic.index.map"
        val BULK_SIZE = "bulk.size"
        val BULK_SIZE_DEFAULT = 100
        val BULK_DELAY = "bulk.delay.ms"
        val BULK_DELAY_DEFAULT = 10_000L
        val REQUEST_TIMEOUT = "request.timeout.ms"
        val REQUEST_TIMEOUT_DEFAULT = 10_000L
        val MAX_IN_FLIGHT_REQUESTS = "max.in.flight.requests"
        val MAX_IN_FLIGHT_REQUESTS_DEFAULT = 1
        val DELAY_BEETWEEN_REQUESTS = "delay.between.requests.ms"
        val DELAY_BEETWEEN_REQUESTS_DEFAULT = 0L
        val QUEUE_SIZE = "queue.size"
        val QUEUE_SIZE_DEFAULT = 2
        val RETRY_INTERVAL = "retry.interval"
        val RETRY_INTERVAL_DEFAULT = 30_000L
        val MAX_RETRY_INTERVAL = "max.retry.interval"
        val MAX_RETRY_INTERVAL_DEFAULT = 300_000L
        val PROTOBUF_INCLUDE_DEFAULT_VALUES = "protobuf.include.default.values"
        val PROTOBUF_INCLUDE_DEFAULT_VALUES_DEFAULT = true

        val CONFIG = ConfigDef()
        init {
            CONFIG.define(
                CONNECTION_URL,
                ConfigDef.Type.LIST,
                ConfigDef.Importance.HIGH,
                "List of Elasticsearch HTTP connection URLs " +
                    "e.g. ``http://es1:9200,http://es2:9200``"
            )
            CONFIG.define(
                COMPRESSION_ENABLED,
                ConfigDef.Type.BOOLEAN,
                COMPRESSION_ENABLED_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                "Enables gzip compression"
            )
            CONFIG.define(
                INDEX,
                ConfigDef.Type.STRING,
                "",
                ConfigDef.Importance.HIGH,
                "An index into which send actions"
            )
            CONFIG.define(
                TOPIC_INDEX_MAP,
                ConfigDef.Type.LIST,
                emptyList<String>(),
                ConfigDef.Importance.HIGH,
                "A map from Kafka topic name to the destination Elasticsearch index, " +
                        "represented as a list of `topic:index` pairs."
            )
            CONFIG.define(
                BULK_SIZE,
                ConfigDef.Type.INT,
                BULK_SIZE_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                "The number of actions in the bulk request."
            )
            CONFIG.define(
                BULK_DELAY,
                ConfigDef.Type.LONG,
                BULK_DELAY_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                "Maximum delay to group actions in the bulk request."
            )
            CONFIG.define(
                REQUEST_TIMEOUT,
                ConfigDef.Type.LONG,
                REQUEST_TIMEOUT_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                "Timeout for Elasticsearch requests."
            )
            CONFIG.define(
                MAX_IN_FLIGHT_REQUESTS,
                ConfigDef.Type.INT,
                MAX_IN_FLIGHT_REQUESTS_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                "Maximum number of the concurrent requests to Elasticsearch."
            )
            CONFIG.define(
                DELAY_BEETWEEN_REQUESTS,
                ConfigDef.Type.LONG,
                DELAY_BEETWEEN_REQUESTS_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                "Delay before next bulk request to Elasticsearch"
            )
            CONFIG.define(
                QUEUE_SIZE,
                ConfigDef.Type.INT,
                QUEUE_SIZE_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                "Queue size for bulk requests. Total pending bulk requests can be" +
                    "equal to `$MAX_IN_FLIGHT_REQUESTS` * `$QUEUE_SIZE`"
            )
            CONFIG.define(
                RETRY_INTERVAL,
                ConfigDef.Type.LONG,
                RETRY_INTERVAL_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                "Interval between retries when some actions was rejected." +
                        "These retries are exponentially increased."
            )
            CONFIG.define(
                MAX_RETRY_INTERVAL,
                ConfigDef.Type.LONG,
                MAX_RETRY_INTERVAL_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                "Maximum interval between retries in seconds."
            )
            CONFIG.define(
                PROTOBUF_INCLUDE_DEFAULT_VALUES,
                ConfigDef.Type.BOOLEAN,
                PROTOBUF_INCLUDE_DEFAULT_VALUES_DEFAULT,
                ConfigDef.Importance.LOW,
                "When ``false`` does not include fields with default value into " +
                    "json document."
            )
            CONFIG.define(
                WorkerConfig.OFFSET_COMMIT_TIMEOUT_MS_CONFIG,
                ConfigDef.Type.LONG,
                WorkerConfig.OFFSET_COMMIT_TIMEOUT_MS_DEFAULT,
                ConfigDef.Importance.LOW,
                "Maximum timeout to flush pending actions."
            )
            CONFIG.define(
                ConnectorConfig.NAME_CONFIG,
                ConfigDef.Type.STRING,
                ConfigDef.Importance.HIGH,
                "Connector name. Used in log messages."
            )
        }
    }

    fun getMap(name: String): Map<String, String> {
        return getList(name)
            .map { it.split(':', limit = 2) }
            .associate { it[0].trim() to it[1].trim() }
    }
}