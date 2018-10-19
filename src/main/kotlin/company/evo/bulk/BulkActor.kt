package company.evo.bulk

import company.evo.Timeout

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlinx.coroutines.channels.Channel.Factory.UNLIMITED
import java.lang.Exception

import java.util.concurrent.atomic.AtomicInteger

interface Hasher<in T> {
    fun hash(obj: T): Int
}

interface BulkSink<in T> : AutoCloseable {
    suspend fun put(action: T)
    suspend fun flush(): Boolean
}

fun <T> BulkSink(
        hasher: Hasher<T>,
        concurrency: Int,
        actorFactory: () -> BulkActor<T>
) = BulkSinkImpl(hasher, concurrency, actorFactory)

class BulkSinkImpl<in T>(
        private val hasher: Hasher<T>,
        private val concurrency: Int,
        private val actorFactory: () -> BulkActor<T>
): BulkSink<T> {

    private val actors = (1..concurrency).map { actorFactory() }

    override suspend fun put(action: T) {
        val hash = hasher.hash(action)
        actors[hash % concurrency].put(action)
    }

    override suspend fun flush(): Boolean {
        return actors.all { it.flush() }
    }

    override fun close() {
        actors.forEach { it.close() }
    }
}

interface BulkActor<in T> : AutoCloseable {
    suspend fun put(action: T)
    suspend fun flush(): Boolean

    companion object {
        operator fun <T> invoke(
                scope: CoroutineScope,
                bulkWriter: BulkWriter<T>,
                bulkSize: Int,
                bulkQueueSize: Int = 0,
                maxDelayMs: Long = -1,
                delayBetweenBulksMs: Long = -1
        ): BulkActor<T> {
            return BulkActorImpl(scope, bulkWriter, bulkSize, bulkQueueSize, maxDelayMs, delayBetweenBulksMs)
        }
    }
}

class BulkActorImpl<in T>(
        private val scope: CoroutineScope,
        private val bulkWriter: BulkWriter<T>,
        private val bulkSize: Int,
        private val bulkQueueSize: Int = 0,
        maxDelayMs: Long = -1,
        delayBetweenBulksMs: Long = -1
) : BulkActor<T> {

    companion object {
        private val FLUSH_ON_TIMEOUT = ActionMsg.Flush()
        private val CLOSED_MESSAGE = "Is closed"
    }

    private sealed class ActionMsg {
        data class Add<T>(val action: T) : ActionMsg()
        data class Flush(val processed: CompletableDeferred<Unit>? = null) : ActionMsg()
    }

    private sealed class BulkResultMsg {
        object Ok : BulkResultMsg()
        object Fail : BulkResultMsg()
        data class Err(val caused: Exception) : BulkResultMsg()
    }

    private var epoch = System.nanoTime()
    private fun echo(msg: String) {
        println("[${(System.nanoTime() - epoch) / 1_000_000}] [${Thread.currentThread().name}] $msg")
    }

    private val timeout = if (maxDelayMs > 0) Timeout(maxDelayMs) else null
    private val bulksDelay = if (delayBetweenBulksMs > 0) Timeout(delayBetweenBulksMs) else null

    @Volatile
    private var isClosed: Boolean = false

    private var job: Job = Job(parent = scope.coroutineContext[Job])
    private var actionChannel: SendChannel<ActionMsg> = startActionActor()
    private var bulkChannel: SendChannel<List<T>> = startBulkActor()
    private var bulkResultChannel = Channel<BulkResultMsg>(UNLIMITED)
    private val pendingBulks = AtomicInteger()

    private suspend fun sendBulk(buffer: List<T>, flushMsg: ActionMsg.Flush? = null) {
        pendingBulks.incrementAndGet()
        if (flushMsg?.processed != null) {
            flushMsg.processed.complete(Unit)
        }
        bulkChannel.send(buffer)
    }

    override suspend fun put(action: T) {
        checkClosed()
        actionChannel.send(ActionMsg.Add(action))
    }

    override suspend fun flush(): Boolean {
        checkClosed()
        return try {
            val processed = CompletableDeferred<Unit>()
            val msg = ActionMsg.Flush(processed)
            actionChannel.send(msg)
            processed.await()
            val pendingBulkResults = pendingBulks.get()
            (1..pendingBulkResults).all {
                when (val resultMsg = bulkResultChannel.receive()) {
                    is BulkResultMsg.Ok -> true
                    is BulkResultMsg.Fail -> false
                    is BulkResultMsg.Err -> {
                        restart()
                        throw resultMsg.caused
                    }
                }.also {
                    pendingBulks.decrementAndGet()
                }
            }
        } catch (exc: CancellationException) {
            // If flush was cancelled the data is in inconsistent state
            // so we must cancel all the coroutines and clear all the data
            restart()
            throw exc
        }
    }

    private fun checkClosed() {
        if (isClosed) {
            throw CancellationException(CLOSED_MESSAGE)
        }
    }

    private fun restart() {
        close()
        job = Job(parent = scope.coroutineContext[Job])
        pendingBulks.set(0)
        actionChannel = startActionActor()
        bulkChannel = startBulkActor()
        isClosed = false
    }

    private fun startActionActor(): SendChannel<ActionMsg> = scope.actor(
            job + CoroutineName("action-actor"),
            capacity = 0
    ) {
        var buffer = ArrayList<T>(bulkSize)
        while (true) {
            val msgOrNull = try {
                if (buffer.isEmpty() || timeout == null) {
                    // Wait first action infinitely
                    receive()
                } else {
                    // Wait next actions no more than maxDelayMs
                    withTimeoutOrNull(timeout.timeLeft()) {
                        receive()
                    }
                }
            } catch (exc: ClosedReceiveChannelException) {
                break
            }
            val msg = msgOrNull ?: FLUSH_ON_TIMEOUT

            when (msg) {
                is ActionMsg.Add<*> -> {
                    if (buffer.isEmpty()) {
                        // Note the time if it is first action
                        timeout?.reset()
                    }
                    @Suppress("UNCHECKED_CAST")
                    buffer.add(msg.action as T)
                    if (buffer.size >= bulkSize) {
                        sendBulk(buffer)
                        buffer = ArrayList()
                    }
                }
                is ActionMsg.Flush -> {
                    if (buffer.isEmpty()) {
                        msg.processed?.complete(Unit)
                    } else {
                        sendBulk(buffer, msg)
                        buffer = ArrayList()
                    }
                }
            }
        }
    }

    private fun startBulkActor() = scope.actor<List<T>>(
            job + CoroutineName("bulk-actor"),
            capacity = bulkQueueSize
    ) {
        bulkResultChannel = Channel(UNLIMITED)
        var isFirstBulk = true
        for (bulk in this) {
            if (bulksDelay != null && !isFirstBulk) {
                delay(bulksDelay.timeLeft())
            }
            val resultMsg = try {
                if (bulkWriter.write(bulk)) {
                    BulkResultMsg.Ok
                } else {
                    BulkResultMsg.Fail
                }
            } catch (exc: Exception) {
                BulkResultMsg.Err(exc)
            }
            bulkResultChannel.send(resultMsg)
            bulksDelay?.reset()
            isFirstBulk = false
        }
    }

    override fun close() {
        if (isClosed) {
            return
        }
        isClosed = true
        // TODO Should we really close channels?
        actionChannel.close()
        bulkChannel.close()
        bulkResultChannel.close()
        job.cancel()
    }
}
