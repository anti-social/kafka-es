package company.evo.elasticsearch

import com.google.protobuf.Message
import io.searchbox.client.JestClient
import io.searchbox.client.JestClientFactory
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.core.Bulk
import io.searchbox.core.BulkResult
import io.searchbox.core.Ping

import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask

import org.slf4j.LoggerFactory
import java.io.IOException
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread


class ElasticsearchSinkTask() : SinkTask() {
    private var testEsClient: JestClient? = null

    lateinit private var esClient: JestClient
    lateinit private var topicToIndexMap: Map<String, String>
    private var protobufIncludeDefaultValues: Boolean =
            Config.PROTOBUF_INCLUDE_DEFAULT_VALUES_DEFAULT

    lateinit private var doggedThread: Thread
    private val doggedThreadMonitor = Object()
    private var workingDog = AtomicBoolean(false)
    lateinit private var heartbeatThread: Thread
    private val heartbeatThreadMonitor = Object()
    private val workingHeartbeat = AtomicBoolean(false)
    private var isPaused = false
    private val retriableActions = ArrayList<AnyBulkableAction>()

    companion object {
        val logger = LoggerFactory.getLogger(ElasticsearchSinkTask::class.java)
        val esClientFactory = JestClientFactory()

        // TODO(Review all the exceptions)
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
        val maxRetryInterval = Config.MAX_RETRY_INTERVAL_DEFAULT
        val esUrl = config.getList(Config.CONNECTION_URL)
        val testEsClient = this.testEsClient
        if (testEsClient != null) {
            this.esClient = testEsClient
        } else {
            val requestTimeout = config.getInt(Config.REQUEST_TIMEOUT)
            esClientFactory
                    .setHttpClientConfig(
                            HttpClientConfig.Builder(esUrl)
                                    .connTimeout(requestTimeout)
                                    .readTimeout(requestTimeout)
                                    .build())
            this.esClient = esClientFactory.`object`
        }
        doggedThread = thread(name = "elastic-dogging") {
            synchronized(doggedThreadMonitor) {
                while (!Thread.interrupted()) {
                    try {
                        doggedThreadMonitor.wait()
                        var retryInterval = 1000L
                        while (true) {
                            Thread.sleep(retryInterval)
                            synchronized(retriableActions) actions@ {
                                retriableActions.retainAll(sendToElastic(retriableActions))
                                if (retriableActions.isEmpty()) {
                                    workingDog.set(false)
                                    return@actions
                                }
                            }
                            // exponential retry interval
                            retryInterval *= retryInterval
                            retryInterval = minOf(retryInterval, maxRetryInterval)
                        }
                    } catch (e: InterruptedException) {}
                }
            }
        }
        heartbeatThread = thread(name = "elastic-heartbeat") {
            synchronized(heartbeatThreadMonitor) {
                while (!Thread.interrupted()) {
                    try {
                        heartbeatThreadMonitor.wait()
                        while (true) {
                            Thread.sleep(5000)
                            try {
                                logger.info("Pinging elasticsearch: ${esUrl}")
                                // TODO(Check status for sink indexes)
                                val res = esClient.execute(Ping.Builder().build())
                                if (res.isSucceeded) {
                                    workingHeartbeat.set(false)
                                    break
                                }
                            } catch (e: IOException) {
                                continue
                            }
                        }
                    } catch (e: InterruptedException) {}
                }
            }
        }
    }

    override fun stop() {
        logger.debug("Stopping ElasticsearchSinkTask")
        doggedThread.interrupt()
        heartbeatThread.interrupt()
        isPaused = false
        workingDog.set(false)
        workingHeartbeat.set(false)
        esClient.close()
    }

    override fun version(): String {
        return "unknown"
    }

    override fun put(records: MutableCollection<SinkRecord>) {
        logger.info("Recieved ${records.size} records")
        val actions = ArrayList<AnyBulkableAction>()
        if (isPaused) {
            if (workingDog.get() || workingHeartbeat.get()) {
                return
            } else {
                resume()
                synchronized(retriableActions) {
                    actions.addAll(retriableActions)
                }
            }
        }
        records.forEach { record ->
            // TODO(index should be mandatory)
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
        try {
            val retryActions = sendToElastic(actions)
            if (retryActions.isNotEmpty()) {
                pause()
                workingDog.set(true)
                synchronized(retriableActions) {
                    retriableActions.addAll(retryActions)
                }
                synchronized(doggedThreadMonitor) {
                    doggedThreadMonitor.notifyAll()
                }
            }
        } catch (e: IOException) {
            logger.error("Error when sending request to Elasticsearch: $e")
            pause()
            workingHeartbeat.set(true)
            synchronized(retriableActions) {
                retriableActions.addAll(actions)
            }
            synchronized(heartbeatThreadMonitor) {
                heartbeatThreadMonitor.notifyAll()
            }
        } finally {
            actions.clear()
        }
    }

    override fun preCommit(
            currentOffsets: MutableMap<TopicPartition, OffsetAndMetadata>?
    ): MutableMap<TopicPartition, OffsetAndMetadata>
    {
        if (isPaused) {
            return HashMap()
        }
        return super.preCommit(currentOffsets)
    }

    override fun flush(currentOffsets: MutableMap<TopicPartition, OffsetAndMetadata>?) {
    }

    private fun sendToElastic(actions: Collection<AnyBulkableAction>)
            : Collection<AnyBulkableAction>
    {
        if (actions.isEmpty()) {
            return emptyList()
        }
        logger.info("Sending ${actions.size} actions to elasticsearch")
        val bulkRequest = Bulk.Builder()
                .addAction(actions)
                .build()
        val bulkResult = esClient.execute(bulkRequest)
        val retriableActions = ArrayList<AnyBulkableAction>()
        if (!bulkResult.isSucceeded) {
            val failedItems = ArrayList<BulkResult.BulkResultItem>()
            val retriableItems = ArrayList<BulkResult.BulkResultItem>()
            // TODO(Fix Jest to correctly process missing id (for create operation))
            bulkResult.items.zip(actions).forEach { (item, action) ->
                if (item.error == null) return@forEach
                if (item.errorType in NON_RETRIABLE_ES_ERRORS) {
                    failedItems.add(item)
                } else {
                    retriableItems.add(item)
                    retriableActions.add(action)
                }
            }
            if (failedItems.isNotEmpty()) {
                // TODO(Save non retriable documents into dedicated topic)
                logger.error(formatFailedItems(
                        "Some documents weren't indexed, skipping them",
                        failedItems
                ))
            }
            if (retriableItems.isNotEmpty()) {
                logger.error(formatFailedItems(
                        "Some documents weren't indexed, retrying them",
                        retriableItems))
            }
        }
        return retriableActions
    }

    private fun pause() {
        context.pause(*context.assignment().toTypedArray())
        isPaused = true
    }

    private fun resume() {
        context.resume(*context.assignment().toTypedArray())
        isPaused = false
    }

    private fun formatFailedItems(
            message: String,
            items: Collection<BulkResult.BulkResultItem>): String
    {
        return "$message:\n" +
                items.map { "\t${it.errorType}: ${it.errorReason}" }
                        .joinToString("\n")
    }
}
