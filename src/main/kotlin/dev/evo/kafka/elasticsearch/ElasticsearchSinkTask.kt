package dev.evo.kafka.elasticsearch

import dev.evo.elasticart.transport.ElasticsearchKtorTransport
import dev.evo.elasticart.transport.ElasticsearchTransport

import dev.evo.kafka.castOrFail

import io.ktor.client.engine.cio.CIO

import java.util.concurrent.atomic.AtomicReference

import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.runtime.ConnectorConfig
import org.apache.kafka.connect.runtime.WorkerConfig
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask

import org.slf4j.LoggerFactory

import kotlin.coroutines.CoroutineContext

import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.runBlocking
import kotlin.random.Random

@kotlin.time.ExperimentalTime
@kotlinx.coroutines.ExperimentalCoroutinesApi
class ElasticsearchSinkTask() : SinkTask(), CoroutineScope {
    lateinit var job: Job
    override val coroutineContext: CoroutineContext
        get() = Dispatchers.Default + CoroutineExceptionHandler { _, throwable ->
            throwable.printStackTrace()
            lastException.set(throwable)
        }
    private val lastException = AtomicReference<Throwable>()

    private var esTestTransport: ElasticsearchTransport? = null
    private lateinit var esTransport: ElasticsearchTransport

    private var name: String = "unknown"
    private var index: String? = null
    private var topicToIndexMap = emptyMap<String, String>()
    private var flushTimeoutMs = WorkerConfig.OFFSET_COMMIT_TIMEOUT_MS_DEFAULT
    private var requestTimeoutMs = Config.REQUEST_TIMEOUT_DEFAULT
    private var maxInFlightRequest = Config.MAX_IN_FLIGHT_REQUESTS_DEFAULT
    private var delayBetweenRequestsMs = Config.DELAY_BEETWEEN_REQUESTS_DEFAULT
    private var bulkSize = Config.BULK_SIZE_DEFAULT
    private var bulkDelayMs = Config.BULK_DELAY_DEFAULT
    private var bulkQueueSize = Config.QUEUE_SIZE_DEFAULT
    private var retryIntervalMs = Config.RETRY_INTERVAL_DEFAULT
    private var maxRetryIntervalMs = Config.MAX_RETRY_INTERVAL_DEFAULT

    private lateinit var sink: ElasticsearchSink<BulkAction>
    private var processedRecords = 0L
    private var lastFlushResult: ElasticsearchSink.FlushResult = ElasticsearchSink.FlushResult.Ok

    private val isPaused
        get() = lastFlushResult !is ElasticsearchSink.FlushResult.Ok

    companion object {
        private val logger = LoggerFactory.getLogger(ElasticsearchSinkTask::class.java)

        private val EMPTY_OFFSETS: MutableMap<TopicPartition, OffsetAndMetadata> = HashMap()
    }

    internal constructor(esTestTransport: ElasticsearchTransport) : this() {
        this.esTestTransport = esTestTransport
    }

    override fun start(props: MutableMap<String, String>) {
        logger.debug("Starting ElasticsearchSinkTask")
        job = Job()

        try {
            val config = Config(props)
            name = config.getString(ConnectorConfig.NAME_CONFIG)
            index = config.getString(Config.INDEX)
            topicToIndexMap = config.getMap(Config.TOPIC_INDEX_MAP)
            // 90% from the offset commit timeout
            flushTimeoutMs = 90 * config.getLong(WorkerConfig.OFFSET_COMMIT_TIMEOUT_MS_CONFIG) / 100
            maxInFlightRequest = config.getInt(Config.MAX_IN_FLIGHT_REQUESTS)
            delayBetweenRequestsMs = config.getLong(Config.DELAY_BEETWEEN_REQUESTS)
            bulkSize = config.getInt(Config.BULK_SIZE)
            bulkDelayMs = config.getLong(Config.BULK_DELAY)
            bulkQueueSize = config.getInt(Config.QUEUE_SIZE)
            requestTimeoutMs = config.getLong(Config.REQUEST_TIMEOUT)
            retryIntervalMs = config.getLong(Config.RETRY_INTERVAL)
            maxRetryIntervalMs = config.getLong(Config.MAX_RETRY_INTERVAL)

            val esTestTransport = this.esTestTransport
            esTransport = if (esTestTransport != null) {
                esTestTransport
            } else {
                val esUrl = config.getList(Config.CONNECTION_URL)
                logger.info("[$name] Initializing Elasticsearch client for cluster: $esUrl")
                // TODO: Mutliple endpoint urls
                ElasticsearchKtorTransport(esUrl[0], CIO.create {}) {
                    gzipRequests = config.getBoolean(Config.COMPRESSION_ENABLED)
                }
            }
        } catch (e: ConfigException) {
            throw ConnectException(
                    "Couldn't start ${this::class.java} due to configuration error", e
            )
        }
    }

    override fun stop() {
        job.cancel()
    }

    override fun open(partitions: MutableCollection<TopicPartition>) {
        logger.info("Opening [$name]")
        val esBulkSender = ElasticsearchBulkSender(
            esTransport, requestTimeoutMs
        )
        sink = ElasticsearchSink(
            this,
            concurrency = maxInFlightRequest,
            router = { action ->
                val routingKey = action.meta.routing ?: action.meta.id()
                routingKey?.hashCode() ?: Random.nextInt()
            },
            bulkSize = bulkSize,
            bulkDelayMs = bulkDelayMs,
            maxPendingBulks = bulkQueueSize,
            bulkWriterFactory = { channel ->
                BulkSinkActor(
                    this, name, channel,
                    { actions -> esBulkSender.sendBulk(actions, refresh = false) },
                    delayBetweenRequestsMs = delayBetweenRequestsMs,
                    minRetryDelayMs = retryIntervalMs,
                    maxRetryDelayMs = maxRetryIntervalMs,
                    metrics = Metrics.kafkaEsMetrics,
                ).job
            }
        )
    }

    override fun close(partitions: MutableCollection<TopicPartition>) {
        // close is called before partition rebalancing so we need to clean up all
        // messages which were sent earlier
        logger.info("Closing [$name]")
        sink.close()
        lastFlushResult = ElasticsearchSink.FlushResult.Ok
        processedRecords = 0
    }

    override fun version(): String {
        return "unknown"
    }

    override fun put(records: MutableCollection<SinkRecord>) {
        val exc = lastException.get()
        if (exc != null) {
            throw exc
        }

        if (records.isNotEmpty()) {
            logger.debug("[$name] Received ${records.size} records")
        }

        val actions = preprocessRecords(records)
        processedRecords += actions.size
        if (actions.isEmpty()) {
            return
        }

        val isSent = runBlocking {
            sink.send(actions, requestTimeoutMs)
        }
        if (!isSent) {
            pause()
        }
    }

    private fun preprocessRecords(records: Collection<SinkRecord>): List<BulkAction> {
        return buildList(records.size) {
            records.forEach { record ->
                when (val value = record.value()) {
                    is BulkAction -> {
                        add(maybeUpdateActionIndex(value, record.topic()))
                    }
                    is List<*> -> {
                        val topic = record.topic()
                        for (action in value) {
                            add(maybeUpdateActionIndex(castOrFail(action), topic))
                        }
                    }
                    else -> error("Unsupported record value type: ${value::class}")
                }
            }
        }
    }

    private fun maybeUpdateActionIndex(action: BulkAction, topic: String): BulkAction {
        // Index could be set by a transformation
        if (action.meta.index.isNullOrEmpty()) {
            val indexName = topicToIndexMap.getOrDefault(topic, index)
            action.meta.index = requireNotNull(indexName) {
                "Cannot find out an index for an action"
            }
        }
        return action
    }

    override fun preCommit(
        currentOffsets: MutableMap<TopicPartition, OffsetAndMetadata>?
    ): MutableMap<TopicPartition, OffsetAndMetadata> {
        val flushResult = runBlocking {
            sink.flush(flushTimeoutMs, lastFlushResult)
        }
        if (flushResult !is ElasticsearchSink.FlushResult.Ok) {
            pause(flushResult)
            return EMPTY_OFFSETS
        }
        resume()

        if (processedRecords > 0) {
            logger.info("[$name] Committing $processedRecords processed records")
        }
        processedRecords = 0
        return super.preCommit(currentOffsets)
    }

    override fun flush(currentOffsets: MutableMap<TopicPartition, OffsetAndMetadata>?) {}

    private fun pause(flushResult: ElasticsearchSink.FlushResult = ElasticsearchSink.FlushResult.SendTimeout) {
        if (isPaused) {
            return
        }
        lastFlushResult = flushResult
        context.pause(*context.assignment().toTypedArray())
        logger.info("[$name] Paused consuming new records")
    }

    private fun resume() {
        if (!isPaused) {
            return
        }
        lastFlushResult = ElasticsearchSink.FlushResult.Ok
        context.resume(*context.assignment().toTypedArray())
        logger.info("[$name] Resumed consuming new records")
    }
}
