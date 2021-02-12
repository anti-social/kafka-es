package dev.evo.kafka.elasticsearch

import kotlin.time.TimeSource

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.selects.select

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
class ElasticsearchSink<T>(
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

    private val routerActor = RoutingActor(
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
