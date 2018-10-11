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

import company.evo.Timeout
import company.evo.bulk.BulkActor
import company.evo.bulk.BulkSink
import company.evo.bulk.elasticsearch.BulkAction
import company.evo.bulk.elasticsearch.ElasticBulkHasher
import company.evo.bulk.elasticsearch.ElasticBulkWriter

import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout

import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.apache.http.nio.client.HttpAsyncClient
import org.apache.kafka.connect.runtime.ConnectorConfig


class ElasticsearchSinkTask() : SinkTask(), CoroutineScope {
    companion object {
        private val logger = LoggerFactory.getLogger(ElasticsearchSinkTask::class.java)

        private val EMPTY_OFFSETS: MutableMap<TopicPartition, OffsetAndMetadata> = HashMap()
    }

    private lateinit var job: Job
    override val coroutineContext: CoroutineContext
        get() = Dispatchers.Default + job

    private var testHttpClient: HttpAsyncClient? = null

    private var name: String = "unknown"
    private var index: String? = null
    private var topicToIndexMap = emptyMap<String, String>()
    private var flushTimeoutMs = WorkerConfig.OFFSET_COMMIT_TIMEOUT_MS_DEFAULT
    private var requestTimeoutMs = Config.REQUEST_TIMEOUT_DEFAULT

    private var esClient: JestClient? = null
    private lateinit var sink: BulkSink<BulkAction>
    private var isPaused = false
    private var processedRecords: Int = 0

    private var protobufProcessor = ProtobufProcessor()
    private val jsonProcessor = JsonProcessor()

    private val requestTimeout = Timeout(requestTimeoutMs)
    private val flushTimeout = Timeout(flushTimeoutMs)

    internal constructor(httpClient: HttpAsyncClient) : this() {
        this.testHttpClient = httpClient
    }

    override fun start(props: MutableMap<String, String>) {
        logger.debug("Starting ElasticsearchSinkTask")
        try {
            val config = Config(props)
            this.name = config.getString(ConnectorConfig.NAME_CONFIG)
            this.index = config.getString(Config.INDEX)
            this.topicToIndexMap = config.getMap(Config.TOPIC_INDEX_MAP)
            // 90% from the offset commit timeout
            this.flushTimeoutMs = 90 * config.getLong(WorkerConfig.OFFSET_COMMIT_TIMEOUT_MS_CONFIG) / 100
            this.protobufProcessor = ProtobufProcessor(
                    includeDefaultValues = config.getBoolean(
                            Config.PROTOBUF_INCLUDE_DEFAULT_VALUES
                    )
            )
            val esUrls = config.getList(Config.CONNECTION_URL)
//            val testEsClient = this.testEsClient
            requestTimeoutMs = config.getLong(Config.REQUEST_TIMEOUT)
//            val esClient = if (testEsClient != null) {
//                testEsClient
//            } else {
//                logger.info("[$name] Initializing Elasticsearch client for cluster: $esUrls")
//                JestClientFactory().apply {
//                    setHttpClientConfig(
//                            HttpClientConfig.Builder(esUrls)
//                                    .multiThreaded(true)
//                                    .connTimeout(requestTimeoutMs.toInt())
//                                    .readTimeout(requestTimeoutMs.toInt())
//                                    .build()
//                    )
//                }
//                        .`object`
//            }
//            this.esClient = esClient
            this.job = Job()
            val hasher = ElasticBulkHasher()
            val httpClient = testHttpClient ?: HttpAsyncClientBuilder.create()
                    .build()
            this.sink = BulkSink(
                    hasher,
                    concurrency = config.getInt(Config.MAX_IN_FLIGHT_REQUESTS)
            ) {
                BulkActor(
                        this, ElasticBulkWriter(httpClient, esUrls),
                        bulkSize = config.getInt(Config.BULK_SIZE),
                        bulkQueueSize = config.getInt(Config.QUEUE_SIZE)
                )
            }
//            this.sink = Sink(
//                    name,
//                    esUrl,
//                    esClient,
//                    bulkSize = config.getInt(Config.BULK_SIZE),
//                    queueSize = config.getInt(Config.QUEUE_SIZE),
//                    maxInFlightRequests = config.getInt(Config.MAX_IN_FLIGHT_REQUESTS),
//                    delayBeetweenRequests = config.getLong(Config.DELAY_BEETWEEN_REQUESTS),
//                    retryIntervalMs = config.getLong(Config.RETRY_INTERVAL),
//                    maxRetryIntervalMs = config.getLong(Config.MAX_RETRY_INTERVAL)
//            )
        } catch (e: ConfigException) {
            throw ConnectException(
                    "Couldn't start ${this::class.java} due to configuration error", e
            )
        }
    }

    override fun stop() {
        logger.info("[$name] Stopping ElasticsearchSinkTask")
        sink.close()
//        httpClient.close()
        isPaused = false
        processedRecords = 0
    }

    override fun version(): String {
        return "unknown"
    }

    override fun put(records: MutableCollection<SinkRecord>) = runBlocking {
        if (records.isNotEmpty()) {
            logger.debug("[$name] Received ${records.size} records")
        }
//        val sink = getSink()
//        if (isPaused) {
//            if (sink.waitingElastic()) {
//                return
//            } else {
//                resume()
//            }
//        }

        records.forEach {
            processRecord(it)
        }
    }

//    private fun getSink(): BulkSink<BulkAction> {
//        return sink ?: throw ConnectException("Sink is not initialized")
//    }

    private fun processRecord(record: SinkRecord) {
        val index = topicToIndexMap[record.topic()] ?: index
        val value = record.value()
        if (value is List<*>) {
            value.forEach {
                processValue(it, index, record)
            }
        } else {
            processValue(value, index, record)
        }
    }

    private fun processValue(value: Any?, index: String?, record: SinkRecord) {
        val anyBulkAction = when (value) {
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
        val bulkAction = BulkAction(
                BulkAction.Operation.DELETE, anyBulkAction.index, anyBulkAction.type, anyBulkAction.id,
                routing = anyBulkAction.getParameter("routing").firstOrNull()?.toString(),
                source = anyBulkAction.getSource()
        )
//        val routing = bulkAction.getParameter(Parameters.ROUTING).toList()
//        // TODO(Possibly we always should hash only topic, partition and key)
//        val hash = when {
//            routing.isNotEmpty() -> {
//                Objects.hash(*routing.toTypedArray())
//            }
//            bulkAction.id != null -> {
//                bulkAction.id.hashCode()
//            }
//            record.key() != null -> {
//                record.key().hashCode()
//            }
//            else -> {
//                Objects.hash(record.topic(), record.kafkaPartition())
//            }
//        }
        try {
            requestTimeout.reset()
            runBlocking {
                withTimeout(requestTimeout.timeLeft()) {
                    sink.put(bulkAction)
                }
            }
//            if (!sink.put(bulkAction)) {
//                pause()
//            }
            processedRecords += 1
        } catch (e: IllegalArgumentException) {
            logger.error("[$name] Malformed message", e)
        }
    }

    override fun preCommit(
            currentOffsets: MutableMap<TopicPartition, OffsetAndMetadata>?
    ): MutableMap<TopicPartition, OffsetAndMetadata> {
//        val sink = getSink()
        if (isPaused) {
            return EMPTY_OFFSETS
        }
        runBlocking {
            flushTimeout.reset()
            withTimeout(flushTimeout.timeLeft()) {
                sink.flush()
            }.let { isFlushed ->
                if (!isFlushed) {
                    pause()
                    return@runBlocking EMPTY_OFFSETS
                }
            }
//        if (!sink.flush(flushTimeout)) {
//            pause()
//            return EMPTY_OFFSETS
//        }
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
