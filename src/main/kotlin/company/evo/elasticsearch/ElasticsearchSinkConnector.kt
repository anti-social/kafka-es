package company.evo.elasticsearch

import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.sink.SinkConnector
import java.net.ConnectException


class ElasticsearchSinkConnector : SinkConnector() {
    lateinit var configProps: Map<String, String>

    override fun config(): ConfigDef {
        return ConfigDef()
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
                    "Couldn't start ElasticsearchSinkConnector due to configuration error: $e")
        }
    }

    override fun stop() {}
}

class Config(props: MutableMap<String, String>) : AbstractConfig(CONFIG, props) {
    companion object {
        val CONNECTION_URL = "connection.url"
        val TOPIC_INDEX_MAP = "topic.index.map"
        val REQUEST_TIMEOUT = "request.timeout.ms"
        val REQUEST_TIMEOUT_DEFAULT = 10000
        val PROTOBUF_INCLUDE_DEFAULT_VALUES = "protobuf.include_default_values"
        val PROTOBUF_INCLUDE_DEFAULT_VALUES_DEFAULT = false
        // TODO(Make configurable timeout policy)
        val RETRY_TIMEOUT_DEFAULT = 30000L

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
                    TOPIC_INDEX_MAP,
                    ConfigDef.Type.LIST,
                    ConfigDef.Importance.HIGH,
                    "A map from Kafka topic name to the destination Elasticsearch index, " +
                            "represented as a list of ``topic:index`` pairs."
            )
            CONFIG.define(
                    REQUEST_TIMEOUT,
                    ConfigDef.Type.INT,
                    REQUEST_TIMEOUT_DEFAULT,
                    ConfigDef.Importance.MEDIUM,
                    """Timeout for Elasticsearch requests.
                        | Default is ``${REQUEST_TIMEOUT_DEFAULT}``.
                        |""".trimMargin()
            )
            CONFIG.define(
                    PROTOBUF_INCLUDE_DEFAULT_VALUES,
                    ConfigDef.Type.BOOLEAN,
                    PROTOBUF_INCLUDE_DEFAULT_VALUES_DEFAULT,
                    ConfigDef.Importance.LOW,
                    """When option is ``true`` includes all the message fields into json.
                        | Default is ``${PROTOBUF_INCLUDE_DEFAULT_VALUES_DEFAULT}``.
                        |""".trimMargin()
            )
        }
    }

    fun getMap(name: String): Map<String, String> {
        return getList(name).map {
            val (key, value) = it.split(':', limit = 2)
            key to value
        }.toMap()
    }
}