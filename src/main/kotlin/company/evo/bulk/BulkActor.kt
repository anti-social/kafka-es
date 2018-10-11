package company.evo.bulk

import company.evo.Timeout

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.Channel.Factory.UNLIMITED
import kotlinx.coroutines.channels.ClosedReceiveChannelException
import kotlinx.coroutines.channels.actor

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
        scope: CoroutineScope,
        private val bulkWriter: BulkWriter<T>,
        bulkSize: Int,
        bulkQueueSize: Int = 0,
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
    private fun log(msg: String) {
//        println("${(System.nanoTime() - epoch) / 1_000_000}: [${Thread.currentThread().name}] $msg")
    }
    private fun echo(msg: String) {
//        println("[${(System.nanoTime() - epoch) / 1_000_000}] $msg")
    }

    private val job = Job(parent = scope.coroutineContext[Job])
    private val timeout = if (maxDelayMs > 0) Timeout(maxDelayMs) else null
    private val sentActionMessages = AtomicInteger()
    private val receivedActionMessages = AtomicInteger()
    private val actionChannel = scope.actor<Msg>(job, capacity = 0) {
        log("Action actor started, job is $job")
        var buffer = ArrayList<T>(bulkSize)
        // TODO Remove try-catch blocks after testing is finished
        try {
            while (true) {
                echo("Waiting action message")
                val msgOrNull = try {
                    if (buffer.isEmpty() || timeout == null) {
                        // Wait first action infinitely
                        receive()
                    } else {
                        // Wait next actions no more than maxDelayMs
                        echo("timeout: ${timeout.timeLeft()}")
                        withTimeoutOrNull(timeout.timeLeft()) {
                            receive()
                        }
                    }
                } catch (exc: ClosedReceiveChannelException) {
                    break
                }
                if (msgOrNull != null) {
                    val n = receivedActionMessages.incrementAndGet()
                    echo("#$n msg received: $msgOrNull")
                } else {
                    echo("Flush on timeout")
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
                            echo("Flushing by size")
                            sendBulk(buffer)
                            buffer = ArrayList()
                        }
                    }
                    is Msg.Flush -> {
                        if (buffer.isEmpty()) {
                            msg.processed?.complete(Unit)
                        } else {
                            log("Flusing by timeout or flush message")
                            sendBulk(buffer, msg)
                            buffer = ArrayList()
                        }
                    }
                }
            }
        } catch (exc: CancellationException) {
            log("Action actor cancelled")
            throw exc
        }
        log("Action actor ended")
    }
    private val bulkChannel = scope.actor<List<T>>(job, capacity = bulkQueueSize) {
        log("Bulk actor started, job is $job")
        val bulksDelay = if (delayBetweenBulksMs > 0) Timeout(delayBetweenBulksMs) else null
        var isFirstBulk = true
        for (bulk in this) {
            log("Received bulk")
            if (bulksDelay != null && !isFirstBulk) {
                delay(bulksDelay.timeLeft())
            }
            val isWritten = bulkWriter.write(bulk)
            bulksDelay?.reset()
            log("Bulk was written")
            bulkResultChannel.send(isWritten)
            isFirstBulk = false
        }
        log("Bulk actor ended")
    }
    private val pendingBulks = AtomicInteger()
    private val bulkResultChannel = Channel<Boolean>(UNLIMITED)

    private suspend fun sendBulk(buffer: List<T>, flushMsg: Msg.Flush? = null) {
        pendingBulks.incrementAndGet()
        echo("completing $flushMsg, ${flushMsg?.processed}, ${flushMsg === FLUSH_ON_TIMEOUT}")
        if (flushMsg?.processed != null) {
            echo("completed")
            flushMsg.processed.complete(Unit)
        }
        echo("sending bulk")
        bulkChannel.send(buffer)
        echo("bulk sent")
    }

    override suspend fun put(action: T) {
        log(">>> put: $action")
        val msg = Msg.Add(action)
        actionChannel.send(msg)
        echo("#${sentActionMessages.incrementAndGet()} msg sent: $msg")
    }

    override suspend fun flush(): Boolean {
        log(">>> flush")
        val processed = CompletableDeferred<Unit>()
        val msg = Msg.Flush(processed)
        actionChannel.send(msg)
        echo("#${sentActionMessages.incrementAndGet()} msg sent: $msg")
        echo("Awaiting message processed: $msg")
        processed.await()
        echo("Message was processed")
        val pendingBulkResults = pendingBulks.get()
        echo("Flushing $pendingBulkResults bulks")
        val isFlushed = (1..pendingBulkResults).all {
            bulkResultChannel.receive()
                    .also { _ -> pendingBulks.decrementAndGet() }
                    .also { _ -> echo("5.3") }
        }
        echo("Flush result: $isFlushed")
        return isFlushed
    }

    override fun close() {
        log(">>> close")
        // TODO Should we really close channels?
        actionChannel.close()
        bulkChannel.close()
        bulkResultChannel.close()
        job.cancel()
    }
}
