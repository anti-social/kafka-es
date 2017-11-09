package company.evo.elasticsearch

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import com.google.protobuf.Message

import io.searchbox.client.JestClient
import io.searchbox.client.JestClientFactory
import io.searchbox.client.config.HttpClientConfig

import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask

import org.slf4j.LoggerFactory


class ElasticsearchSinkTask() : SinkTask() {
    private var testEsClient: JestClient? = null

    lateinit private var esClient: JestClient
    lateinit private var topicToIndexMap: Map<String, String>
    val bulkSize = 500
    private var protobufIncludeDefaultValues: Boolean =
            Config.PROTOBUF_INCLUDE_DEFAULT_VALUES_DEFAULT

    lateinit private var sinkThread: Thread
    lateinit private var sinkQueue: BlockingQueue<Collection<AnyBulkableAction>>
    lateinit private var sinkConfirmationQueue: BlockingQueue<String>
    lateinit private var heartbeatThread: Thread
    private val waitingElastic = AtomicBoolean(false)

    private var isPaused = false
    private var requestCounter = 0
    private var actions = ArrayList<AnyBulkableAction>()

    companion object {
        private val logger = LoggerFactory.getLogger(ElasticsearchSinkTask::class.java)
        private val esClientFactory = JestClientFactory()

        private val EMPTY_OFFSETS: MutableMap<TopicPartition, OffsetAndMetadata> = HashMap()
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
        val heartbeatThreadMonitor = Object()
        heartbeatThread = Thread(
                Heartbeat(esClient, 5000, heartbeatThreadMonitor, waitingElastic),
                "elastic-heartbeat"
        )
        heartbeatThread.start()
        sinkQueue = ArrayBlockingQueue(20)
        sinkConfirmationQueue = ArrayBlockingQueue(20)
        sinkThread = Thread(
                Sink(esClient, sinkQueue, sinkConfirmationQueue, heartbeatThreadMonitor, waitingElastic),
                "elastic-sink"
        )
        sinkThread.start()
    }

    override fun stop() {
        logger.debug("Stopping ElasticsearchSinkTask")
        sinkThread.interrupt()
        heartbeatThread.interrupt()
        waitingElastic.set(false)
        isPaused = false
        requestCounter = 0
        esClient.close()
    }

    override fun version(): String {
        return "unknown"
    }

    override fun put(records: MutableCollection<SinkRecord>) {
        logger.info("Recieved ${records.size} records")
        if (isPaused) {
            if (waitingElastic.get()) {
                return
            } else {
                resume()
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
                if (actions.size >= bulkSize) {
                    send(actions)
                    actions.clear()
                }
            } catch (e: IllegalArgumentException) {
                logger.error("Malformed message: $e")
            }
        }
    }

    private fun send(actions: Collection<AnyBulkableAction>) {
        try {
            if (sinkQueue.offer(actions.toList(), 5000, TimeUnit.MILLISECONDS)) {
                requestCounter += 1
                logger.info("Put ${actions.size} actions into queue")
            }
        } catch (e: InterruptedException) {
            throw ConnectException(e)
        }
    }

    override fun preCommit(
            currentOffsets: MutableMap<TopicPartition, OffsetAndMetadata>?
    ): MutableMap<TopicPartition, OffsetAndMetadata>
    {
        logger.info("Pre commit: waiting $requestCounter requests to be finished")
        if (isPaused) {
            return EMPTY_OFFSETS
        }
        if (actions.isNotEmpty()) {
            send(actions)
            actions.clear()
        }
        while (requestCounter > 0) {
            val confirm = sinkConfirmationQueue.poll(5000, TimeUnit.MILLISECONDS)
            if (confirm == null) {
                pause()
                return EMPTY_OFFSETS
            } else {
                requestCounter -= 1
            }
        }
        return super.preCommit(currentOffsets)
    }

    override fun flush(currentOffsets: MutableMap<TopicPartition, OffsetAndMetadata>?) {
    }

    private fun pause() {
        logger.info("Pausing consuming new records")
        context.pause(*context.assignment().toTypedArray())
        isPaused = true
    }

    private fun resume() {
        logger.info("Resuming consuming new records")
        context.resume(*context.assignment().toTypedArray())
        isPaused = false
    }
}
