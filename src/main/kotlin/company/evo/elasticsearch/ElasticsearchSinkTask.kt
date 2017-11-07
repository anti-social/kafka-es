package company.evo.elasticsearch

import com.google.protobuf.Message
import io.searchbox.client.JestClient
import io.searchbox.client.JestClientFactory
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.core.Bulk

import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.consumer.RetriableCommitFailedException
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask

import org.slf4j.LoggerFactory
import java.io.IOException


class ElasticsearchSinkTask() : SinkTask() {
    private var testEsClient: JestClient? = null
    lateinit private var esClient: JestClient
    lateinit private var topicToIndexMap: Map<String, String>
    private var protobufIncludeDefaultValues: Boolean =
            Config.PROTOBUF_INCLUDE_DEFAULT_VALUES_DEFAULT
    private val actions = ArrayList<AnyBulkableAction>()

    companion object {
        val logger = LoggerFactory.getLogger(ElasticsearchSinkTask::class.java)
        val esClientFactory = JestClientFactory()

        // TODO(Review all the exceptions)
        val UNRECOVERABLE_ES_ERRORS = setOf(
                "index_closed_exception",
                "index_not_found_exception"
        )
        val NON_RETRIABLE_ES_ERRORS = setOf(
                "elasticsearch_parse_exception",
                "parsing_exception",
                "routing_missing_exception"
        )
    }

    internal constructor(esClient: JestClient) : this() {
        this.testEsClient = esClient
    }

    override fun start(props: MutableMap<String, String>) {
        logger.debug("Starting ElasticsearchSinkTask")
        val config = Config(props)
        this.topicToIndexMap = config.getMap(Config.TOPIC_INDEX_MAP)
        this.protobufIncludeDefaultValues = config.getBoolean(
                Config.PROTOBUF_INCLUDE_DEFAULT_VALUES)
        val testEsClient = this.testEsClient
        if (testEsClient != null) {
            this.esClient = testEsClient
        } else {
            val requestTimeout = config.getInt(Config.REQUEST_TIMEOUT)
            esClientFactory
                    .setHttpClientConfig(
                            HttpClientConfig.Builder(config.getList(Config.CONNECTION_URL))
                                    .connTimeout(requestTimeout)
                                    .readTimeout(requestTimeout)
                                    .build())
            this.esClient = esClientFactory.`object`
        }
    }

    override fun stop() {
        logger.debug("Stopping ElasticsearchSinkTask")
        actions.clear()
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
                        throw IllegalArgumentException(
                                "Expected one of [${Map::class.java}, ${Message::class.java}] " +
                                        "but was: ${value.javaClass}"
                        )
                    }
                }
                actions.add(bulkAction)
            } catch (e: IllegalArgumentException) {
                logger.error("Malformed message: $e")
            }
        }
    }

    override fun flush(currentOffsets: MutableMap<TopicPartition, OffsetAndMetadata>?) {
        if (actions.isEmpty()) {
            super.flush(currentOffsets)
            return
        }
        val bulkRequest = Bulk.Builder()
                .addAction(actions)
                .build()
        try {
            val bulkResult = esClient.execute(bulkRequest)
            if (!bulkResult.isSucceeded) {
                var retriableErrors = false
                var errorMsg = ""
                // TODO(Fix Jest to correctly process missing id (for create operation))
                bulkResult.failedItems.forEach {
                    if (it.errorType in NON_RETRIABLE_ES_ERRORS) {
                        errorMsg += "\t${it.id}: ${it.error}\n"
                    } else {
                        retriableErrors = true
                    }
                }
                if (retriableErrors) {
                    context.timeout(Config.RETRY_TIMEOUT_DEFAULT)
                    throw RetriableCommitFailedException(
                            "Some documents weren't indexed:\n${errorMsg}"
                    )
                }
            }
            super.flush(currentOffsets)
        } catch (e: IOException) {
            handleException(e)
        } finally {
            actions.clear()
        }
    }

    private fun handleException(exc: IOException) {
        logger.error("Error when sending request to Elasticsearch: $exc")
        // TODO(Pause records consuming when elasticsearch is unavailable)
        // context.pause()
        // TODO(Start a heartbeat thread to resume consuming when elasticsearch will be available)
        // context.resume()
        throw RetriableCommitFailedException("Could not connect to elasticsearch", exc)
    }
}
