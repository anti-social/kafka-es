package company.evo.bulk

import company.evo.Timeout
import company.evo.bulk.elasticsearch.BulkAction

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlinx.coroutines.channels.Channel.Factory.UNLIMITED
import java.util.concurrent.atomic.AtomicBoolean

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
}

fun <T> BulkActor(
        scope: CoroutineScope,
        bulkWriter: BulkWriter<T>,
        bulkSize: Int,
        bulkQueueSize: Int = 0,
        maxDelayMs: Long = -1,
        delayBetweenBulksMs: Long = -1
) = BulkActorImpl(scope, bulkWriter, bulkSize, bulkQueueSize, maxDelayMs, delayBetweenBulksMs)

class BulkActorImpl<in T>(
        private val scope: CoroutineScope,
        private val bulkWriter: BulkWriter<T>,
        private val bulkSize: Int,
        private val bulkQueueSize: Int = 0,
        maxDelayMs: Long = -1,
        delayBetweenBulksMs: Long = -1
) : BulkActor<T> {

    private sealed class Msg {
        data class Add<T>(val action: T) : Msg()
        data class Flush(val processed: CompletableDeferred<Unit>? = null) : Msg()
    }

    companion object {
        private val FLUSH_ON_TIMEOUT = Msg.Flush()
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
    private var actionChannel: SendChannel<Msg> = startActionActor()
    private var bulkChannel: SendChannel<List<T>> = startBulkActor()
    private var bulkResultChannel = Channel<Boolean>(UNLIMITED)
    private val pendingBulks = AtomicInteger()

    private suspend fun sendBulk(buffer: List<T>, flushMsg: Msg.Flush? = null) {
        pendingBulks.incrementAndGet()
        if (flushMsg?.processed != null) {
            flushMsg.processed.complete(Unit)
        }
        bulkChannel.send(buffer)
    }

    override suspend fun put(action: T) {
        if (isClosed) {
            throw CancellationException()
        }
        actionChannel.send(Msg.Add(action))
    }

    override suspend fun flush(): Boolean {
        if (isClosed) {
            throw CancellationException()
        }
        return try {
            val processed = CompletableDeferred<Unit>()
            val msg = Msg.Flush(processed)
            actionChannel.send(msg)
            processed.await()
            val pendingBulkResults = pendingBulks.get()
            (1..pendingBulkResults).all {
                bulkResultChannel.receive()
                        .also { pendingBulks.decrementAndGet() }
            }
        } catch (exc: CancellationException) {
            restart()
            throw exc
        }
    }

    private fun restart() {
        close()
        job = Job(parent = scope.coroutineContext[Job])
        actionChannel = startActionActor()
        bulkChannel = startBulkActor()
        pendingBulks.set(0)
        isClosed = false
    }

    private fun startActionActor(): SendChannel<Msg> = scope.actor(job, capacity = 0) {
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
                is Msg.Add<*> -> {
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
                is Msg.Flush -> {
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

    private fun startBulkActor() = scope.actor<List<T>>(job, capacity = bulkQueueSize) {
        bulkResultChannel = Channel(UNLIMITED)
        var isFirstBulk = true
        for (bulk in this) {
            if (bulksDelay != null && !isFirstBulk) {
                delay(bulksDelay.timeLeft())
            }
            val isWritten = bulkWriter.write(bulk)
            bulksDelay?.reset()
            bulkResultChannel.send(isWritten)
            isFirstBulk = false
        }
    }

    override fun close() {
        // TODO Should we really close channels?
        actionChannel.close()
        bulkChannel.close()
        bulkResultChannel.close()
//        job.cancelChildren()
        job.cancel()
        isClosed = true
    }
}
