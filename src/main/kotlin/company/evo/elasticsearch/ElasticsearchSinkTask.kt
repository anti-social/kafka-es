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
import org.apache.kafka.connect.runtime.WorkerConfig
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask

import org.slf4j.LoggerFactory

import company.evo.kafka.Timeout


class ElasticsearchSinkTask() : SinkTask() {
    private var testEsClient: JestClient? = null

    private var topicToIndexMap = emptyMap<String, String>()
    private var flushTimeoutMs = WorkerConfig.OFFSET_COMMIT_TIMEOUT_MS_DEFAULT
    private var requestTimeoutMs = Config.REQUEST_TIMEOUT_DEFAULT
    private var protobufIncludeDefaultValues = Config.PROTOBUF_INCLUDE_DEFAULT_VALUES_DEFAULT

    private var esClient: JestClient? = null
    private var sink: Sink? = null
    private var isPaused = false
    private var processedRecords: Int = 0

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
        try {
            val config = Config(props)
            this.topicToIndexMap = config.getMap(Config.TOPIC_INDEX_MAP)
            // 90% from offset commit timeout
            this.flushTimeoutMs = 90 * config.getLong(WorkerConfig.OFFSET_COMMIT_TIMEOUT_MS_CONFIG) / 100
            this.protobufIncludeDefaultValues = config.getBoolean(
                Config.PROTOBUF_INCLUDE_DEFAULT_VALUES
            )
            val esUrl = config.getList(Config.CONNECTION_URL)
            val testEsClient = this.testEsClient
            requestTimeoutMs = config.getLong(Config.REQUEST_TIMEOUT)
            val esClient = if (testEsClient != null) {
                testEsClient
            } else {
                esClientFactory
                        .setHttpClientConfig(
                                HttpClientConfig.Builder(esUrl)
                                        .multiThreaded(true)
                                        .connTimeout(requestTimeoutMs.toInt())
                                        .readTimeout(requestTimeoutMs.toInt())
                                        .build()
                        )
                esClientFactory.`object`
            }
            this.esClient = esClient
            this.sink = Sink(
                    esClient,
                    bulkSize = config.getInt(Config.BULK_SIZE),
                    queueSize = config.getInt(Config.QUEUE_SIZE),
                    maxInFlightRequests = config.getInt(Config.MAX_IN_FLIGHT_REQUESTS),
                    heartbeatIntervalMs = config.getLong(Config.HEARTBEAT_INTERVAL),
                    retryIntervalMs = config.getLong(Config.RETRY_INTERVAL),
                    maxRetryIntervalMs = config.getLong(Config.MAX_RETRY_INTERVAL)
            )
        } catch (e: ConfigException) {
            throw ConnectException(
                    "Couldn't start ${this::class.java} due to configuration error", e
            )
        }
    }

    override fun stop() {
        logger.debug("Stopping ElasticsearchSinkTask")
        sink?.close()
        esClient?.close()
        isPaused = false
        processedRecords = 0
    }

    override fun version(): String {
        return "unknown"
    }

    override fun put(records: MutableCollection<SinkRecord>) {
        logger.debug("Recieved ${records.size} records")
        val sink = getSink()
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
                val timeout = Timeout(requestTimeoutMs)
                if (!sink.put(action.action, action.hash, isPaused, timeout)) {
                    pause()
                }
                processedRecords += 1
            } catch (e: IllegalArgumentException) {
                logger.error("Malformed message", e)
            }
        }
    }

    private fun getSink(): Sink {
        return sink ?: throw ConnectException("Sink is not initialized")
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
        val timeout = Timeout(flushTimeoutMs)
        val sink = getSink()
        if (isPaused) {
            return EMPTY_OFFSETS
        }
        if (!sink.flush(timeout)) {
            pause()
            return EMPTY_OFFSETS
        }
        logger.info("Committing $processedRecords processed records")
        processedRecords = 0
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
