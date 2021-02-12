package dev.evo.kafka.elasticsearch

import kotlin.time.TimeMark
import kotlin.time.TimeSource

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.ClosedReceiveChannelException
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.channels.receiveOrNull
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.select

/**
 * Sink message with some data.
 */
sealed class SinkMsg<T> {
    /**
     * The data itself.
     */
    data class Data<T>(val data: List<T>) : SinkMsg<T>()

    /**
     * A special message confirming that all data sent earlier has been successfully processed.
     *
     * @param flushed the latch, when it released all messages are processed
     */
    data class Flush<T>(val flushed: Latch) : SinkMsg<T>()
}

/**
 * Router actor splits messages from an input channel into several output channels.
 *
 * @param scope a [CoroutineScope] to launch an actor
 * @param inChannel the input channel
 * @param outChannels output channels
 * @param router a function that is used to route a message into certain output channel
 */
class RoutingActor<T>(
    scope: CoroutineScope,
    inChannel: ReceiveChannel<SinkMsg<T>>,
    outChannels: Array<SendChannel<SinkMsg<T>>>,
    router: (T) -> Int,
) {
    init {
        require(outChannels.isNotEmpty())
    }

    private val job = scope.launch {
        while (true) {
            when (val msg = inChannel.receiveOrNull()) {
                is SinkMsg.Data -> {
                    if (outChannels.size == 1) {
                        outChannels[0].send(msg)
                    } else {
                        val data = msg.data
                        val baseGroupCapacity = (data.size / outChannels.size)
                        val groupCapacity = baseGroupCapacity + baseGroupCapacity / 10
                        val groups = data.fold(
                            Array<MutableList<T>>(outChannels.size) { ArrayList(groupCapacity) }
                        ) { groups, elem ->
                            val groupIx = (router(elem) and 0x7FFF_FFFF) % outChannels.size
                            groups[groupIx].add(elem)
                            groups
                        }
                        for ((channel, groupedData) in outChannels.zip(groups)) {
                            if (groupedData.isNotEmpty()) {
                                channel.send(SinkMsg.Data(groupedData))
                            }
                        }
                    }
                }
                is SinkMsg.Flush -> {
                    outChannels.forEach { ch ->
                        ch.send(msg)
                    }
                }
                null -> break
            }
        }
    }

    /**
     * Cancels actor's coroutine.
     */
    fun cancel(cause: CancellationException? = null) {
        job.cancel(cause)
    }
}

/**
 * Bulk actor takes messages from an input channel groups them and sends into an output channel.
 *
 * @param scope a [CoroutineScope] to launch an actor
 * @param channel the input channel
 * @param bulkChannel the output channel with grouped messages
 * @param bulkSize maximum number of grouped messages inside a single bulk
 * @param bulkDelayMs maximum delay to wait from the first message in a bulk
 * @param clock a [TimeSource] for testing purposes
 */
class BulkActor<T>(
    scope: CoroutineScope,
    channel: ReceiveChannel<SinkMsg<T>>,
    private val bulkChannel: SendChannel<SinkMsg<T>>,
    private val bulkSize: Int,
    bulkDelayMs: Long = Long.MAX_VALUE,
    clock: TimeSource = TimeSource.Monotonic,
) {
    private var buffer = ArrayList<T>(bulkSize)
    private var firstMessageMark: TimeMark? = null
    private val job = scope.launch {
        while (true) {
            val timeoutMs = firstMessageMark.let { firstMessageMark ->
                if (firstMessageMark == null || buffer.isEmpty()) {
                    // Wait for the first message endlessly
                    Long.MAX_VALUE
                } else {
                    bulkDelayMs - firstMessageMark.elapsedNow().toLongMilliseconds()
                }
            }

            try {
                select<Unit> {
                    channel.onReceive { msg ->
                        when (msg) {
                            is SinkMsg.Data -> {
                                for (v in msg.data) {
                                    buffer.add(v)
                                    if (buffer.size >= bulkSize) {
                                        flushBuffer()
                                    }
                                }
                                if (firstMessageMark == null && buffer.isNotEmpty()) {
                                    firstMessageMark = clock.markNow()
                                }
                            }
                            is SinkMsg.Flush -> {
                                flushBuffer()
                                bulkChannel.send(msg)
                            }
                        }
                    }
                    onTimeout(timeoutMs) {
                        flushBuffer()
                    }
                }
            } catch (ex: ClosedReceiveChannelException) {
                break
            }
        }
    }

    private suspend fun flushBuffer() {
        if (buffer.isEmpty()) {
            return
        }
        bulkChannel.send(SinkMsg.Data(buffer))
        buffer = ArrayList(bulkSize)
        firstMessageMark = null
    }

    fun cancel(cause: CancellationException? = null) {
        job.cancel(cause)
    }
}

/**
 * Represents result of sending bulk actions.
 */
sealed class SendBulkResult<out T> {
    class Success<T>(
        val totalTimeMs: Long,
        val tookTimeMs: Long,
        val successActionsCount: Long,
        val retryActions: List<T>,
    ) : SendBulkResult<T>()
    object IOError : SendBulkResult<Nothing>()
    object Timeout : SendBulkResult<Nothing>()
}

/**
 * Bulk writer actor takes actions from an input channel and sends them into Elasticsearch.
 *
 * @param scope a [CoroutineScope] to launch an actor
 * @param channel the input channel
 * @param sendBulk an Elasticsearch bulk requests sender
 * @param delayBetweenRequestsMs the delay between bulk requests
 * @param minRetryDelayMs the minimum delay time before retry
 * @param maxRetryDelayMs the maximum delay time before retry
 */
class BulkSinkActor<T>(
    private val scope: CoroutineScope,
    private val connectorName: String,
    channel: ReceiveChannel<SinkMsg<T>>,
    private val sendBulk: suspend (List<T>) -> SendBulkResult<T>,
    private val minRetryDelayMs: Long,
    private val maxRetryDelayMs: Long,
    delayBetweenRequestsMs: Long = 0,
    private val metrics: KafkaEsMetrics? = null,
    clock: TimeSource = TimeSource.Monotonic,
) {
    val job = scope.launch {
        var lastProcessTimeMark = clock.markNow()
        while (true) {
            when (val msg = channel.receiveOrNull()) {
                is SinkMsg.Data -> {
                    delay(delayBetweenRequestsMs - lastProcessTimeMark.elapsedNow().toLongMilliseconds())
                    process(msg.data)
                    lastProcessTimeMark = clock.markNow()
                }
                is SinkMsg.Flush -> {
                    msg.flushed.countDown()
                }
                null -> break
            }
        }
    }

    private fun setLabels(labels: KafkaEsLabels) = labels.apply {
        connectorName = this@BulkSinkActor.connectorName
    }

    private suspend fun process(bulk: List<T>) {
        var retryDelayMs = minRetryDelayMs
        while (true) {
            val retryActions = when (val sendResult = sendBulk(bulk)) {
                is SendBulkResult.Success<*> -> {
                    metrics?.let { metrics ->
                        metrics.bulksCount.inc(::setLabels)
                        metrics.bulksTotalTime.add(sendResult.totalTimeMs, ::setLabels)
                        metrics.bulksTookTime.add(sendResult.tookTimeMs, ::setLabels)
                        metrics.bulkActionsCount.add(sendResult.successActionsCount, ::setLabels)
                    }
                    sendResult.retryActions
                }
                SendBulkResult.IOError -> {
                    metrics?.bulksErrorCount?.inc(::setLabels)
                    bulk
                }
                SendBulkResult.Timeout -> {
                    metrics?.bulksTimeoutCount?.inc(::setLabels)
                    bulk
                }
            }
            if (retryActions.isEmpty()) {
                break
            }
            delay(retryDelayMs)
            retryDelayMs = (retryDelayMs * 2).coerceAtMost(maxRetryDelayMs)
        }
    }

    fun cancel(cause: CancellationException? = null) {
        job.cancel(cause)
    }
}
