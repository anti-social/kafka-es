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
    init {
        require(concurrency > 0) {
            "Concurrency must be greater than 0"
        }
    }

    private var overflowBuffer = emptyList<T>()
    private val inChannel = Channel<SinkMsg<T>>(Channel.RENDEZVOUS)

    private val partitionedChannels: List<Channel<SinkMsg<T>>>?
    private val routerActor: RoutingActor<T>?

    private val bulkChannels: List<Channel<SinkMsg<T>>>
    private val bulkActors: List<BulkActor<T>>

    init {
        if (concurrency > 1) {
            partitionedChannels = (0 until concurrency).map {
                Channel(Channel.RENDEZVOUS)
            }
            routerActor = RoutingActor(
                scope, inChannel, partitionedChannels.toTypedArray(), router
            )
            bulkChannels = (0 until concurrency).map {
                Channel(maxPendingBulks)
            }
            bulkActors = partitionedChannels.zip(bulkChannels).map { (channel, bulkChannel) ->
                BulkActor(scope, channel, bulkChannel, bulkSize, bulkDelayMs)
            }
        } else {
            partitionedChannels = null
            routerActor = null
            bulkChannels = listOf(Channel(maxPendingBulks))
            bulkActors = listOf(
                BulkActor(scope, inChannel, bulkChannels[0], bulkSize, bulkDelayMs)
            )
        }
    }

    private val bulkWriters = bulkChannels.map { channel ->
        bulkWriterFactory(channel)
    }

    sealed class FlushResult {
        object Ok : FlushResult()
        object SendTimeout : FlushResult()
        class WaitTimeout(val latch: Latch) : FlushResult()
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

    suspend fun flush(timeoutMs: Long, lastFlushResult: FlushResult): FlushResult {
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
                    onTimeout(timeoutMs - startFlushMark.elapsedNow().inWholeMilliseconds) {
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
            onTimeout(timeoutMs - startFlushMark.elapsedNow().inWholeMilliseconds) {
                FlushResult.WaitTimeout(latch)
            }
        }
    }

    fun cancel(cause: CancellationException? = null) {
        bulkWriters.forEach { w -> w.cancel(cause) }
        bulkActors.forEach { actor -> actor.cancel(cause) }
        routerActor?.cancel(cause)
    }
}
