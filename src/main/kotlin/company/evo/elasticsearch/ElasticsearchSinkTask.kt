package company.evo.elasticsearch

import com.google.protobuf.Message
import io.searchbox.client.JestClient
import io.searchbox.client.JestClientFactory
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.core.Bulk
import org.apache.http.conn.HttpHostConnectException

import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.consumer.RetriableCommitFailedException
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask

import org.slf4j.LoggerFactory


class ElasticsearchSinkTask : SinkTask() {
    lateinit private var esClient: JestClient
    lateinit private var topicToIndexMap: Map<String, String>
    private var protobufIncludeDefaultValues: Boolean =
            Config.PROTOBUF_INCLUDE_DEFAULT_VALUES_DEFAULT
    private val buffer = ArrayList<AnyBulkableAction>()

    companion object {
        val logger = LoggerFactory.getLogger(ElasticsearchSinkTask::class.java)
    }

    override fun start(props: MutableMap<String, String>) {
        logger.debug("Starting ElasticsearchSinkTask")
        val config = Config(props)
        this.topicToIndexMap = config.getMap(Config.TOPIC_INDEX_MAP)
        this.protobufIncludeDefaultValues = config.getBoolean(
                Config.PROTOBUF_INCLUDE_DEFAULT_VALUES)
        val esClientFactory = JestClientFactory()
        esClientFactory.setHttpClientConfig(
                HttpClientConfig.Builder(config.getList(Config.CONNECTION_URL))
                        .build())
        this.esClient = esClientFactory.`object`
    }

    override fun stop() {
        logger.debug("Stopping ElasticsearchSinkTask")
        esClient.close()
    }

    override fun version(): String {
        return "unknown"
    }

    override fun put(records: MutableCollection<SinkRecord>) {
        records.forEach { record ->
            val index = topicToIndexMap[record.topic()]
            val value = record.value()
            try {
                val bulkAction = when (value) {
                    is Map<*,*> -> {
                        processJsonMessage(value, index)
                    }
                    is Message -> {
                        processProtobufMessage(value, index,
                                includeDefaultValues = protobufIncludeDefaultValues)
                    }
                    else -> {
                        throw IllegalArgumentException("Expected Map but was: ${value.javaClass}")
                    }
                }
            } catch (e: IllegalArgumentException) {
                logger.error("Malformed message: $e")
            }
        }
    }

    override fun flush(currentOffsets: MutableMap<TopicPartition, OffsetAndMetadata>?) {
        if (buffer.isEmpty()) {
            super.flush(currentOffsets)
            return
        }
        val bulkRequest = Bulk.Builder()
                .defaultIndex("test")
                .addAction(buffer)
                .build()
        try {
            val bulkResult = esClient.execute(bulkRequest)
            if (!bulkResult.isSucceeded) {
                var errorMsg = ""
                // TODO(Fix Jest to correctly process missing id)
                bulkResult.failedItems.forEach {
                    if (it.error != null) {
                        errorMsg += "\t${it.id}: ${it.error}\n"
                        // TODO(Parse error message and determine possibility of retry)
                    }
                }
                buffer.clear()
                throw RetriableCommitFailedException(
                        "Some documents weren't indexed:\n${errorMsg}"
                )
            }
            buffer.clear()
            super.flush(currentOffsets)
        } catch (e: HttpHostConnectException) {
            buffer.clear()
            throw RetriableCommitFailedException("$e")
        }
    }
}
