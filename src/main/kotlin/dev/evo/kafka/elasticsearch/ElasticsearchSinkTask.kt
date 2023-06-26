package dev.evo.kafka.elasticsearch

import dev.evo.elasticmagic.serde.kotlinx.JsonSerde
import dev.evo.elasticmagic.transport.ElasticsearchKtorTransport
import dev.evo.elasticmagic.transport.ElasticsearchTransport
import dev.evo.elasticmagic.transport.PlainRequest
import dev.evo.elasticmagic.transport.PlainResponse
import dev.evo.elasticmagic.transport.Tracker

import dev.evo.kafka.castOrFail
import dev.evo.kafka.WatchDog

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
import org.apache.kafka.connect.util.LoggingContext

import org.slf4j.LoggerFactory
import org.slf4j.MDC
import org.slf4j.NDC

import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.job
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.slf4j.MDCContext

import kotlin.coroutines.CoroutineContext
import kotlin.random.Random
import kotlin.text.toIntOrNull
import kotlin.time.Duration

/**
 * Sink task lifecycle:
 * - initialize
 * - start
 * - open -------+ close and reopen when rebalancing
 *  - put*       |
 *  - preCommit  |
 * - close ------+
 * - stop
 */
class ElasticsearchSinkTask() : SinkTask(), CoroutineScope {
    private val job = Job()
    private val lastException = AtomicReference<Throwable>()
    private val exceptionHandler = CoroutineExceptionHandler { _, throwable ->
        throwable.printStackTrace()
        lastException.set(throwable)
    }
    override val coroutineContext: CoroutineContext get() =
        job + Dispatchers.Default + MDCContext() + exceptionHandler

    private var esTestTransport: ElasticsearchTransport? = null

    private var taskId: Int = -1
    private lateinit var name: String
    private lateinit var esUrl: List<String>
    private var compressionEnabled = Config.COMPRESSION_ENABLED_DEFAULT
    private var index: String? = null
    private var topicToIndexMap = emptyMap<String, String>()
    private var flushTimeoutMs = WorkerConfig.OFFSET_COMMIT_TIMEOUT_MS_DEFAULT
    private var requestTimeoutMs = Config.REQUEST_TIMEOUT_DEFAULT
    private var keepAliveTimeMs = Config.KEEP_ALIVE_TIME_DEFAULT
    private var maxInFlightRequest = Config.MAX_IN_FLIGHT_REQUESTS_DEFAULT
    private var delayBetweenRequestsMs = Config.DELAY_BEETWEEN_REQUESTS_DEFAULT
    private var bulkSize = Config.BULK_SIZE_DEFAULT
    private var bulkDelayMs = Config.BULK_DELAY_DEFAULT
    private var bulkQueueSize = Config.QUEUE_SIZE_DEFAULT
    private var retryIntervalMs = Config.RETRY_INTERVAL_DEFAULT
    private var maxRetryIntervalMs = Config.MAX_RETRY_INTERVAL_DEFAULT
    private var stuckTimeoutMs = Config.TASK_STUCK_TIMEOUT_DEFAULT
    private val isWatchDogActive
        get() = taskId >=0 && stuckTimeoutMs > 0

    private lateinit var sink: ElasticsearchSink<BulkAction>
    private var processedRecords = 0L
    private var lastFlushResult: ElasticsearchSink.FlushResult = ElasticsearchSink.FlushResult.Ok

    private val isPaused
        get() = lastFlushResult !is ElasticsearchSink.FlushResult.Ok

    companion object {
        private val logger = LoggerFactory.getLogger(ElasticsearchSinkTask::class.java)

        private val EMPTY_OFFSETS: MutableMap<TopicPartition, OffsetAndMetadata> = HashMap()

        private val metricsUpdater: AtomicReference<MetricsUpdater> = AtomicReference()

        fun installMetrics(metricsUpdater: MetricsUpdater) {
            if (!this@Companion.metricsUpdater.compareAndSet(null, metricsUpdater)) {
                throw IllegalStateException("Metrics have already been installed")
            }
        }
    }

    internal constructor(esTestTransport: ElasticsearchTransport) : this() {
        this.esTestTransport = esTestTransport
    }

    override fun start(props: MutableMap<String, String>) {
        try {
            val config = Config(props)
            name = config.getString(ConnectorConfig.NAME_CONFIG)

            NDC.push(name)
            logger.debug("Starting")

            index = config.getString(Config.INDEX)
            esUrl = config.getList(Config.CONNECTION_URL)
            compressionEnabled = config.getBoolean(Config.COMPRESSION_ENABLED)
            topicToIndexMap = config.getMap(Config.TOPIC_INDEX_MAP)
            // 90% from the offset commit timeout
            flushTimeoutMs = 90 * config.getLong(WorkerConfig.OFFSET_COMMIT_TIMEOUT_MS_CONFIG) / 100
            maxInFlightRequest = config.getInt(Config.MAX_IN_FLIGHT_REQUESTS)
            delayBetweenRequestsMs = config.getLong(Config.DELAY_BEETWEEN_REQUESTS)
            bulkSize = config.getInt(Config.BULK_SIZE)
            bulkDelayMs = config.getLong(Config.BULK_DELAY)
            bulkQueueSize = config.getInt(Config.QUEUE_SIZE)
            requestTimeoutMs = config.getLong(Config.REQUEST_TIMEOUT)
            keepAliveTimeMs = config.getLong(Config.KEEP_ALIVE_TIME)
            retryIntervalMs = config.getLong(Config.RETRY_INTERVAL)
            maxRetryIntervalMs = config.getLong(Config.MAX_RETRY_INTERVAL)
            stuckTimeoutMs = config.getLong(Config.TASK_STUCK_TIMEOUT)
        } catch (e: ConfigException) {
            throw ConnectException(
                "Couldn't start ${this::class.java} due to configuration error", e
            )
        }
    }

    override fun stop() {
        logger.info("Stopping")

        cancel()
        NDC.pop()
    }

    override fun open(partitions: MutableCollection<TopicPartition>) {
        logger.info("Opening")

        val loggingConnectorContextValue = MDC.get(LoggingContext.CONNECTOR_CONTEXT)?.trim() ?: "[$name]"
        // Example of kafka connect connector logging context:
        // [my-connector|task-3]
        taskId = loggingConnectorContextValue
            .trim('[', ']')
            .split('|', limit = 2)
            .getOrNull(1)
            ?.removePrefix("task-")
            ?.toIntOrNull() ?: 0

        lastFlushResult = ElasticsearchSink.FlushResult.Ok
        processedRecords = 0

        val metricsUpdater = metricsUpdater.get()

        val esTestTransport = this.esTestTransport
        val esTransport = if (esTestTransport != null) {
            esTestTransport
        } else {
            MDC.put("es_url", esUrl.joinToString(",", prefix = "[", postfix = "]"))
            logger.info("Initializing Elasticsearch client for cluster: $esUrl")
            // TODO: Mutliple endpoint urls
            ElasticsearchKtorTransport(esUrl[0], CIO.create {
                requestTimeout = requestTimeoutMs
                if (keepAliveTimeMs >= 0) {
                    endpoint.keepAliveTime = keepAliveTimeMs
                }
            }) {
                gzipRequests = compressionEnabled

                if (metricsUpdater != null) {
                    trackers = listOf(
                        {
                            object : Tracker {
                                override suspend fun onRequest(request: PlainRequest) {
                                    metricsUpdater.onSend(name, taskId, request.content.size.toLong())
                                }

                                override suspend fun onResponse(responseResult: Result<PlainResponse>, duration: Duration) {}
                            }
                        },
                    )
                }
            }
        }
        val esBulkSender = ElasticsearchBulkSender(
            esTransport, requestTimeoutMs
        )
        sink = ElasticsearchSink(
            loggingConnectorContextValue = loggingConnectorContextValue,
            coroutineScope = this,
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
                    coroutineName = "sink$loggingConnectorContextValue",
                    this, name, taskId, channel,
                    { actions -> esBulkSender.sendBulk(actions, refresh = false) },
                    delayBetweenRequestsMs = delayBetweenRequestsMs,
                    minRetryDelayMs = retryIntervalMs,
                    maxRetryDelayMs = maxRetryIntervalMs,
                    metricsUpdater = metricsUpdater,
                ).job
            }
        )

        if (isWatchDogActive) {
            WatchDog.register(
                name,
                taskId,
                stuckTimeoutMs = stuckTimeoutMs,
                retryTimeoutMs = stuckTimeoutMs / 6
            )
        }
    }

    override fun close(partitions: MutableCollection<TopicPartition>) {
        logger.info("Closing")

        if (isWatchDogActive) {
            WatchDog.unregister(name, taskId)
        }

        coroutineContext.job.cancelChildren()
        runBlocking {
            coroutineContext.job.children.toList().joinAll()
        }
    }

    override fun version(): String {
        return "unknown"
    }

    override fun put(records: MutableCollection<SinkRecord>) {
        val exc = lastException.get()
        if (exc != null) {
            throw exc
        }

        logger.debug("Received ${records.size} records")

        if (isWatchDogActive && records.isNotEmpty()) {
            WatchDog.kick(name, taskId)
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
        currentOffsets: MutableMap<TopicPartition, OffsetAndMetadata>
    ): MutableMap<TopicPartition, OffsetAndMetadata> {
        logger.debug("Committing")

        val flushResult = runBlocking {
            sink.flush(flushTimeoutMs, lastFlushResult)
        }
        if (flushResult !is ElasticsearchSink.FlushResult.Ok) {
            pause(flushResult)
            return EMPTY_OFFSETS
        }
        resume()

        if (processedRecords > 0) {
            val logOffsets = currentOffsets.mapValues { it.value.offset() }
            logger.info("Processed $processedRecords records; committing offsets $logOffsets")
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
        logger.info("Paused consuming new records")
    }

    private fun resume() {
        if (!isPaused) {
            return
        }
        lastFlushResult = ElasticsearchSink.FlushResult.Ok
        context.resume(*context.assignment().toTypedArray())
        logger.info("Resumed consuming new records")
    }
}
