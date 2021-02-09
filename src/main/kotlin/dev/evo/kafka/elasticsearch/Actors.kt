package dev.evo.kafka.elasticsearch

import kotlin.time.TimeMark
import kotlin.time.TimeSource

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ClosedReceiveChannelException
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.channels.receiveOrNull
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
class RouterActor<T>(
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
 * Bulk sink creates all channels and binds them with actors.
 * Methods of this object are not thread-safe.
 *
 * @param scope a [CoroutineScope] to launch an actor
 * @param concurrency a number of sink writers
 * @param router a function that used to route message into writer
 * @param bulkSize maximum number of grouped messages inside a single bulk
 * @param bulkDelayMs maximum delay to wait from the first message in a bulk
 * @param maxPendingBulks maximum number of buffered bulks that wait in a queue
 * @param bulkWriterFactory a factory that creates bulk writers
 * @param clock a [TimeSource] for testing purposes
 */
class BulkSink<T>(
    scope: CoroutineScope,
    private val concurrency: Int,
    router: (T) -> Int,
    bulkSize: Int,
    bulkDelayMs: Long,
    maxPendingBulks: Int,
    bulkWriterFactory: (ReceiveChannel<SinkMsg<T>>) -> Job,
    private val clock: TimeSource = TimeSource.Monotonic,
) {
    private var overflowBuffer = emptyList<T>()
    private val inChannel = Channel<SinkMsg<T>>(Channel.RENDEZVOUS)
    private val partitionedChannels = (0 until concurrency).map {
        Channel<SinkMsg<T>>(Channel.RENDEZVOUS)
    }
    private val bulkChannels = (0 until concurrency).map {
        Channel<SinkMsg<T>>(maxPendingBulks)
    }

    private val routerActor = RouterActor(
        scope, inChannel, partitionedChannels.toTypedArray(), router
    )
    private val bulkActors = partitionedChannels.zip(bulkChannels).map { (channel, bulkChannel) ->
        BulkActor(scope, channel, bulkChannel, bulkSize, bulkDelayMs)
    }
        .toTypedArray()
    private val bulkWriters = bulkChannels.map { channel ->
        bulkWriterFactory(channel)
    }

    sealed class FlushResult(val isFlushed: Boolean) {
        object Ok : FlushResult(true)
        object SendTimeout : FlushResult(false)
        class WaitTimeout(val latch: Latch) : FlushResult(false)
    }

    suspend fun send(data: List<T>, timeoutMs: Long): Boolean {
        val sendData = if (overflowBuffer.isNotEmpty()) {
            overflowBuffer + data
        } else {
            data
        }

        val isSent = select<Boolean> {
            inChannel.onSend(SinkMsg.Data(sendData)) { true }
            onTimeout(timeoutMs) { false }
        }
        overflowBuffer = if (isSent) {
            emptyList()
        } else {
            sendData
        }
        return isSent
    }

    suspend fun flush(timeoutMs: Long, lastFlushResult: FlushResult? = null): FlushResult {
        val startFlushMark = clock.markNow()
        if (overflowBuffer.isNotEmpty()) {
            if(!send(emptyList(), timeoutMs)) {
                return FlushResult.SendTimeout
            }
        }

        val latch = when (lastFlushResult) {
            is FlushResult.WaitTimeout -> lastFlushResult.latch
            else -> {
                val latch = Latch(concurrency)
                val isSent = select<Boolean> {
                    inChannel.onSend(SinkMsg.Flush(latch)) {
                        true
                    }
                    onTimeout(timeoutMs - startFlushMark.elapsedNow().toLongMilliseconds()) {
                        false
                    }
                }
                if (!isSent) {
                    return FlushResult.SendTimeout
                }
                latch
            }
        }

        return select {
            latch.onAwait {
                FlushResult.Ok
            }
            onTimeout(timeoutMs - startFlushMark.elapsedNow().toLongMilliseconds()) {
                FlushResult.WaitTimeout(latch)
            }
        }
    }

    fun cancel(cause: CancellationException? = null) {
        bulkWriters.forEach { w -> w.cancel(cause) }
        bulkActors.forEach { actor -> actor.cancel(cause) }
        routerActor.cancel(cause)
    }

    fun close() {
        inChannel.close()
        partitionedChannels.forEach { ch -> ch.close() }
        bulkChannels.forEach { ch -> ch.close() }
    }
}
