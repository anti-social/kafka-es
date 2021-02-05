package dev.evo.kafka.elasticsearch

import java.util.concurrent.atomic.AtomicInteger

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.selects.SelectClause1

/**
 * Asynchronous analogue of Java's [java.util.concurrent.CountDownLatch].
 *
 * @param num the number of [countDown] invokes to pass through [await]; should be greater than zero.
 *
 * Usage:
 *
 *     import kotlin.time.measureTime
 *     import kotlinx.coroutines.delay
 *     import kotlinx.coroutines.launch
 *
 *     val latch = Latch(2)
 *     launch {
 *         val duration = measureTime {
 *             latch.await()
 *         }
 *         println("Waited ${duration.toLongMilliseconds()} ms")
 *     }
 *     launch {
 *         delay(100)
 *         latch.countDown()
 *     }
 *     launch {
 *         delay(500)
 *         latch.countDown()
 *     }
 */
class Latch(num: Int) {
    private val count = AtomicInteger(num.also { require(num > 0) })
    private val deferred = CompletableDeferred<Unit>()

    /**
     * Decrements the count and releases the latch if the count reaches zero.
     */
    fun countDown() {
        if (count.decrementAndGet() == 0) {
            deferred.complete(Unit)
        }
    }

    /**
     * Returns `true` if this latch has released.
     */
    val isReleased: Boolean
        get() = deferred.isCompleted

    /**
     * Suspends the current coroutine until the latch has counted down to zero.
     */
    suspend fun await() {
        deferred.await()
    }

    /**
     * Clause to use inside a [kotlinx.coroutines.selects.select] expression.
     *
     * Usage:
     *
     *     import kotlinx.coroutines.delay
     *     import kotlinx.coroutines.launch
     *     import kotlinx.coroutines.selects.select
     *
     *     val latch = Latch(2)
     *
     *     launch {
     *         val isLatchReleased = select {
     *             latch.onAwait { println("Latch released") }
     *             onTimeout(1_000) { println("Timed out") }
     *         }
     *     }
     *
     *     launch {
     *         delay(500)
     *         latch.countDown()
     *     }
     *     launch {
     *         delay(995)
     *         latch.countDown()
     *     }
     */
    val onAwait: SelectClause1<Unit>
        get() = deferred.onAwait
}
