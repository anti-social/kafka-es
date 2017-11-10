package company.evo.elasticsearch

import com.google.protobuf.Message

import io.searchbox.client.JestClient
import io.searchbox.client.JestClientFactory
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.params.Parameters

import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask

import org.slf4j.LoggerFactory


class ElasticsearchSinkTask() : SinkTask() {
    private var testEsClient: JestClient? = null

    lateinit private var esClient: JestClient
    lateinit private var topicToIndexMap: Map<String, String>
    private var requestTimeout = Config.REQUEST_TIMEOUT_DEFAULT
    private var protobufIncludeDefaultValues = Config.PROTOBUF_INCLUDE_DEFAULT_VALUES_DEFAULT

    lateinit private var sink: Sink
    private var isPaused = false

    companion object {
        private val logger = LoggerFactory.getLogger(ElasticsearchSinkTask::class.java)
        private val esClientFactory = JestClientFactory()

        private val EMPTY_OFFSETS: MutableMap<TopicPartition, OffsetAndMetadata> = HashMap()
    }

    internal class ActionAndHash(val action: AnyBulkableAction, val hash: Int?)

    internal constructor(esClient: JestClient) : this() {
        this.testEsClient = esClient
    }

    override fun start(props: MutableMap<String, String>) {
        logger.debug("Starting ElasticsearchSinkTask")
        val config = try {
            Config(props)
        } catch (e: ConfigException) {
            throw ConnectException(
                    "Couldn't start ${this::class.java} due to configuration error", e
            )
        }
        this.topicToIndexMap = config.getMap(Config.TOPIC_INDEX_MAP)
        this.protobufIncludeDefaultValues = config.getBoolean(
                Config.PROTOBUF_INCLUDE_DEFAULT_VALUES
        )
        val esUrl = config.getList(Config.CONNECTION_URL)
        val testEsClient = this.testEsClient
        requestTimeout = config.getInt(Config.REQUEST_TIMEOUT)
        if (testEsClient != null) {
            this.esClient = testEsClient
        } else {
            esClientFactory
                    .setHttpClientConfig(
                            HttpClientConfig.Builder(esUrl)
                                    .multiThreaded(true)
                                    .connTimeout(requestTimeout)
                                    .readTimeout(requestTimeout)
                                    .build()
                    )
            this.esClient = esClientFactory.`object`
        }
        sink = Sink(
                esClient,
                bulkSize = config.getInt(Config.BULK_SIZE),
                queueSize = config.getInt(Config.QUEUE_SIZE),
                maxInFlightRequests = config.getInt(Config.MAX_IN_FLIGHT_REQUESTS),
                heartbeatInterval = config.getInt(Config.HEARTBEAT_INTERVAL),
                retryInterval = config.getInt(Config.RETRY_INTERVAL),
                maxRetryInterval = config.getInt(Config.MAX_RETRY_INTERVAL)
        )
    }

    override fun stop() {
        logger.debug("Stopping ElasticsearchSinkTask")
        sink.close()
        isPaused = false
        esClient.close()
    }

    override fun version(): String {
        return "unknown"
    }

    override fun put(records: MutableCollection<SinkRecord>) {
        logger.debug("Recieved ${records.size} records")
        if (isPaused) {
            if (sink.waitingElastic()) {
                return
            } else {
                resume()
            }
        }

        records.forEach {
            val action = processRecord(it)
            try {
                if (!sink.put(action.action, action.hash, isPaused, requestTimeout)) {
                    pause()
                }
            } catch (e: IllegalArgumentException) {
                logger.error("Malformed message", e)
            }
        }
    }

    private fun processRecord(record: SinkRecord): ActionAndHash {
        // TODO(index should be mandatory)
        val index = topicToIndexMap[record.topic()]
        val value = record.value()
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
        val routing = bulkAction.getParameter(Parameters.ROUTING).toList()
        val hash = when {
            routing.isNotEmpty() -> {
                routing.joinToString("").hashCode()
            }
            bulkAction.id != null -> {
                bulkAction.id.hashCode()
            }
            record.key() != null -> {
                record.key().hashCode()
            }
            else -> null
        }
        return ActionAndHash(bulkAction, hash)
    }

    override fun preCommit(
            currentOffsets: MutableMap<TopicPartition, OffsetAndMetadata>?
    ): MutableMap<TopicPartition, OffsetAndMetadata>
    {
        logger.debug("preCommit called")
        if (isPaused) {
            return EMPTY_OFFSETS
        }
        // TODO(Set flush timeout in config)
        if (!sink.flush(requestTimeout)) {
            pause()
            return EMPTY_OFFSETS
        }
        return super.preCommit(currentOffsets)
    }

    override fun flush(currentOffsets: MutableMap<TopicPartition, OffsetAndMetadata>?) {
    }

    private fun pause() {
        context.pause(*context.assignment().toTypedArray())
        isPaused = true
        logger.info("Paused consuming new records")
    }

    private fun resume() {
        context.resume(*context.assignment().toTypedArray())
        isPaused = false
        logger.info("Resumed consuming new records")
    }
}
