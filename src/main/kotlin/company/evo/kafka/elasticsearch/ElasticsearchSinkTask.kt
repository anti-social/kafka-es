package company.evo.kafka.elasticsearch

import java.util.Objects

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
import org.apache.kafka.connect.runtime.ConnectorConfig


class ElasticsearchSinkTask() : SinkTask() {
    private var testEsClient: JestClient? = null

    private var name: String = "unknown"
    private var topicToIndexMap = emptyMap<String, String>()
    private var flushTimeoutMs = WorkerConfig.OFFSET_COMMIT_TIMEOUT_MS_DEFAULT
    private var requestTimeoutMs = Config.REQUEST_TIMEOUT_DEFAULT

    private var esClient: JestClient? = null
    private var sink: Sink? = null
    private var isPaused = false
    private var processedRecords: Int = 0

    private var protobufProcessor = ProtobufProcessor()
    private val jsonProcessor = JsonProcessor()

    private val requestTimeout = Timeout(requestTimeoutMs)
    private val flushTimeout = Timeout(flushTimeoutMs)

    companion object {
        private val logger = LoggerFactory.getLogger(ElasticsearchSinkTask::class.java)

        private val EMPTY_OFFSETS: MutableMap<TopicPartition, OffsetAndMetadata> = HashMap()
    }

    internal constructor(esClient: JestClient) : this() {
        this.testEsClient = esClient
    }

    override fun start(props: MutableMap<String, String>) {
        logger.debug("Starting ElasticsearchSinkTask")
        try {
            val config = Config(props)
            this.name = config.getString(ConnectorConfig.NAME_CONFIG)
            this.topicToIndexMap = config.getMap(Config.TOPIC_INDEX_MAP)
            // 90% from the offset commit timeout
            this.flushTimeoutMs = 90 * config.getLong(WorkerConfig.OFFSET_COMMIT_TIMEOUT_MS_CONFIG) / 100
            this.protobufProcessor = ProtobufProcessor(
                    includeDefaultValues = config.getBoolean(
                            Config.PROTOBUF_INCLUDE_DEFAULT_VALUES
                    )
            )
            val esUrl = config.getList(Config.CONNECTION_URL)
            val testEsClient = this.testEsClient
            requestTimeoutMs = config.getLong(Config.REQUEST_TIMEOUT)
            val esClient = if (testEsClient != null) {
                testEsClient
            } else {
                logger.info("[$name] Initializing Elasticsearch client for cluster: $esUrl")
                JestClientFactory().apply {
                    setHttpClientConfig(
                            HttpClientConfig.Builder(esUrl)
                                    .multiThreaded(true)
                                    .connTimeout(requestTimeoutMs.toInt())
                                    .readTimeout(requestTimeoutMs.toInt())
                                    .build()
                    )
                }
                        .`object`
            }
            this.esClient = esClient
            this.sink = Sink(
                    name,
                    esUrl,
                    esClient,
                    bulkSize = config.getInt(Config.BULK_SIZE),
                    queueSize = config.getInt(Config.QUEUE_SIZE),
                    maxInFlightRequests = config.getInt(Config.MAX_IN_FLIGHT_REQUESTS),
                    delayBeetweenRequests = config.getLong(Config.DELAY_BEETWEEN_REQUESTS),
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
        logger.info("[$name] Stopping ElasticsearchSinkTask")
        sink?.close()
        esClient?.close()
        isPaused = false
        processedRecords = 0
    }

    override fun version(): String {
        return "unknown"
    }

    override fun put(records: MutableCollection<SinkRecord>) {
        if (records.isNotEmpty()) {
            logger.debug("[$name] Recieved ${records.size} records")
        }
        val sink = getSink()
        if (isPaused) {
            if (sink.waitingElastic()) {
                return
            } else {
                resume()
            }
        }

        records.forEach {
            processRecord(sink, it)
        }
    }

    private fun getSink(): Sink {
        return sink ?: throw ConnectException("Sink is not initialized")
    }

    private fun processRecord(sink: Sink, record: SinkRecord) {
        val index = topicToIndexMap[record.topic()]
        val value = record.value()
        if (value is List<*>) {
            value.forEach {
                processValue(sink, it, index, record)
            }
        } else {
            processValue(sink, value, index, record)
        }
    }

    private fun processValue(sink: Sink, value: Any?, index: String?, record: SinkRecord) {
        val bulkAction = when (value) {
            is Map<*,*> -> {
                jsonProcessor.process(value, index)
            }
            is Message -> {
                protobufProcessor.process(value, index)
            }
            else -> {
                throw IllegalArgumentException(
                        "Expected one of [${Map::class.java}, ${Message::class.java}] " +
                                "but was: ${value?.javaClass}"
                )
            }
        }
        val routing = bulkAction.getParameter(Parameters.ROUTING).toList()
        // TODO(Possibly we always should hash only topic, partition and key)
        val hash = when {
            routing.isNotEmpty() -> {
                Objects.hash(*routing.toTypedArray())
            }
            bulkAction.id != null -> {
                bulkAction.id.hashCode()
            }
            record.key() != null -> {
                record.key().hashCode()
            }
            else -> {
                Objects.hash(record.topic(), record.kafkaPartition())
            }
        }
        try {
            requestTimeout.reset()
            if (!sink.put(bulkAction, hash, isPaused, requestTimeout)) {
                pause()
            }
            processedRecords += 1
        } catch (e: IllegalArgumentException) {
            logger.error("[$name] Malformed message", e)
        }
    }

    override fun preCommit(
            currentOffsets: MutableMap<TopicPartition, OffsetAndMetadata>?
    ): MutableMap<TopicPartition, OffsetAndMetadata>
    {
        val sink = getSink()
        if (isPaused) {
            return EMPTY_OFFSETS
        }
        flushTimeout.reset()
        if (!sink.flush(flushTimeout)) {
            pause()
            return EMPTY_OFFSETS
        }
        if (processedRecords > 0) {
            logger.info("[$name] Committing $processedRecords processed records")
        }
        processedRecords = 0
        return super.preCommit(currentOffsets)
    }

    override fun flush(currentOffsets: MutableMap<TopicPartition, OffsetAndMetadata>?) {
    }

    private fun pause() {
        context.pause(*context.assignment().toTypedArray())
        isPaused = true
        logger.info("[$name] Paused consuming new records")
    }

    private fun resume() {
        context.resume(*context.assignment().toTypedArray())
        isPaused = false
        logger.info("[$name] Resumed consuming new records")
    }
}
