package dev.evo.kafka.elasticsearch

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe

import java.io.IOException

import kotlin.time.DurationUnit

import kotlin.time.TestTimeSource
import kotlin.time.toDuration

import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runTest

class RoutingActorTests : StringSpec({
    "routing" {
        runTest(UnconfinedTestDispatcher()) {
            val inChannel = Channel<SinkMsg<Int>>(Channel.RENDEZVOUS)
            val outChannels = Array(3) {
                Channel<SinkMsg<Int>>(Channel.UNLIMITED)
            }
            val router = RoutingActor(
                coroutineName = "routing",
                coroutineScope = this,
                inChannel = inChannel,
                outChannels = outChannels.toList().toTypedArray(),
            ) { v -> v }

            try {
                inChannel.send(SinkMsg.Data(listOf(0, 1, 4, Int.MIN_VALUE, Int.MAX_VALUE)))
                inChannel.send(SinkMsg.Data(listOf(0, Int.MAX_VALUE)))
                outChannels[0].tryReceive().getOrNull() shouldBe SinkMsg.Data(listOf(0, Int.MIN_VALUE))
                outChannels[0].tryReceive().getOrNull() shouldBe SinkMsg.Data(listOf(0))
                outChannels[0].tryReceive().getOrNull() shouldBe null
                outChannels[1].tryReceive().getOrNull() shouldBe SinkMsg.Data(listOf(1, 4, Int.MAX_VALUE))
                outChannels[1].tryReceive().getOrNull() shouldBe SinkMsg.Data(listOf(Int.MAX_VALUE))
                outChannels[1].tryReceive().getOrNull() shouldBe null
                outChannels[2].tryReceive().getOrNull() shouldBe null
            } finally {
                router.cancel()
            }
        }
    }
})

class BufferingActorTests : StringSpec({
    "no max delay" {
        runTest(UnconfinedTestDispatcher()) {
            val channel = Channel<SinkMsg<Int>>(0)
            val bulkChannel = Channel<SinkMsg<Int>>(Channel.UNLIMITED)
            val bulker = BufferingActor(
                coroutineName = "buffering",
                coroutineScope = this,
                channel = channel,
                bulkChannel = bulkChannel,
                bulkSize = 2,
            )

            try {
                channel.send(SinkMsg.Data(emptyList()))
                channel.send(SinkMsg.Data(emptyList()))
                bulkChannel.tryReceive().getOrNull() shouldBe null

                channel.send(SinkMsg.Data(listOf(1)))
                channel.send(SinkMsg.Data(listOf(2)))
                bulkChannel.tryReceive().getOrNull() shouldBe SinkMsg.Data(listOf(1, 2))
                bulkChannel.tryReceive().getOrNull() shouldBe null

                channel.send(SinkMsg.Data(listOf(1, 2, 3, 4, 5)))
                bulkChannel.tryReceive().getOrNull() shouldBe SinkMsg.Data(listOf(1, 2))
                bulkChannel.tryReceive().getOrNull() shouldBe SinkMsg.Data(listOf(3, 4))
                bulkChannel.tryReceive().getOrNull() shouldBe null
                channel.send(SinkMsg.Data(listOf(6)))
                bulkChannel.tryReceive().getOrNull() shouldBe SinkMsg.Data(listOf(5, 6))
                bulkChannel.tryReceive().getOrNull() shouldBe null
            } finally {
                bulker.cancel()
                channel.close()
                bulkChannel.close()
            }
        }
    }

    "with max delay" {
        runTest(UnconfinedTestDispatcher()) {
            val channel = Channel<SinkMsg<Int>>(0)
            val bulkChannel = Channel<SinkMsg<Int>>(Channel.UNLIMITED)
            val clock = TestTimeSource()
            val bulker = BufferingActor(
                coroutineName = "buffering",
                coroutineScope = this,
                channel = channel,
                bulkChannel = bulkChannel,
                bulkSize = 3,
                bulkDelayMs = 10,
                clock = clock,
            )

            try {
                channel.send(SinkMsg.Data(listOf(1)))
                bulkChannel.tryReceive().getOrNull() shouldBe null

                clock += 8.toDuration(DurationUnit.MILLISECONDS)
                advanceTimeBy(8)
                bulkChannel.tryReceive().getOrNull() shouldBe null

                channel.send(SinkMsg.Data(listOf(2)))

                clock += 1.toDuration(DurationUnit.MILLISECONDS)
                advanceTimeBy(1)
                bulkChannel.tryReceive().getOrNull() shouldBe null

                clock += 1.toDuration(DurationUnit.MILLISECONDS)
                advanceTimeBy(2)
                bulkChannel.tryReceive().getOrNull() shouldBe SinkMsg.Data(listOf(1, 2))
            } finally {
                bulker.cancel()
                channel.close()
                bulkChannel.close()
            }
        }
    }

    "with max delay after flush by size" {
        runTest(UnconfinedTestDispatcher()) {
            val channel = Channel<SinkMsg<Int>>(0)
            val bulkChannel = Channel<SinkMsg<Int>>(Channel.UNLIMITED)
            val clock = TestTimeSource()
            val bulker = BufferingActor(
                coroutineName = "buffering",
                coroutineScope = this,
                channel = channel,
                bulkChannel = bulkChannel,
                bulkSize = 3,
                bulkDelayMs = 10,
                clock = clock,
            )

            try {
                // Initial delay should be ignored
                clock += 8.toDuration(DurationUnit.MILLISECONDS)
                advanceTimeBy(8)
                channel.send(SinkMsg.Data(listOf(1)))
                bulkChannel.tryReceive().getOrNull() shouldBe null

                clock += 9.toDuration(DurationUnit.MILLISECONDS)
                advanceTimeBy(9)
                bulkChannel.tryReceive().getOrNull() shouldBe null
                channel.send(SinkMsg.Data(listOf(2, 3, 4)))
                bulkChannel.tryReceive().getOrNull() shouldBe SinkMsg.Data(listOf(1, 2, 3))

                clock += 9.toDuration(DurationUnit.MILLISECONDS)
                advanceTimeBy(9)
                bulkChannel.tryReceive().getOrNull() shouldBe null

                clock += 1.toDuration(DurationUnit.MILLISECONDS)
                advanceTimeBy(2)
                bulkChannel.tryReceive().getOrNull() shouldBe SinkMsg.Data(listOf(4))
            } finally {
                bulker.cancel()
                channel.close()
                bulkChannel.close()
            }
        }
    }
})

class BulkSinkActorTests : StringSpec({
    "retry on error" {
        runTest {
            val channel = Channel<SinkMsg<Unit>>()
            var retries = 0
            val sink = BulkSinkActor(
                coroutineName = "buffering",
                coroutineScope = this,
                connectorName = "<test>",
                taskId = 0,
                channel = channel,
                sendBulk = {
                    retries++
                    when (retries) {
                        1 -> SendBulkResult.IOError(IOException("io error"))
                        2 -> SendBulkResult.Success(
                            1, 1, 1, emptyList(), emptyList()
                        )
                        else -> throw IllegalStateException()
                    }
                },
                minRetryDelayMs = 15_000,
                maxRetryDelayMs = 600_000,
            )

            try {
                channel.send(SinkMsg.Data(listOf(Unit)))

                val flushed = Latch(1)
                launch {
                    channel.send(SinkMsg.Flush(flushed))
                    flushed.await()
                }

                advanceTimeBy(14_000)
                flushed.isReleased shouldBe false

                advanceTimeBy(1_001)
                flushed.isReleased shouldBe true
            } finally {
                sink.cancel()
                channel.close()
            }
        }
    }

    "retry on timeout" {
        runTest {
            val channel = Channel<SinkMsg<Unit>>()
            var retries = 0
            val sink = BulkSinkActor(
                coroutineName = "sink",
                coroutineScope = this,
                connectorName = "<test>",
                taskId = 0,
                channel = channel,
                sendBulk = {
                    retries++
                    when (retries) {
                        1 -> {
                            delay(3_000)
                            SendBulkResult.Timeout(IllegalStateException())
                        }
                        2 -> {
                            SendBulkResult.Success(
                                1, 1, 1, emptyList(), emptyList()
                            )
                        }
                        else -> throw IllegalStateException()
                    }
                },
                minRetryDelayMs = 15_000,
                maxRetryDelayMs = 600_000,
            )

            try {
                channel.send(SinkMsg.Data(listOf(Unit)))

                val flushed = Latch(1)
                launch {
                    channel.send(SinkMsg.Flush(flushed))
                    flushed.await()
                }

                flushed.isReleased shouldBe false

                advanceTimeBy(2_000)
                flushed.isReleased shouldBe false

                advanceTimeBy(15_000)
                flushed.isReleased shouldBe false

                advanceTimeBy(1_001)
                flushed.isReleased shouldBe true
            } finally {
                sink.cancel()
                channel.close()
            }
        }
    }

    "retry partially" {
        runTest {
            val channel = Channel<SinkMsg<Int>>()
            var retries = 0
            val sink = BulkSinkActor(
                coroutineName = "sink",
                coroutineScope = this,
                connectorName = "<test>",
                taskId = 0,
                channel = channel,
                sendBulk = {
                    retries++
                    when (retries) {
                        1 -> SendBulkResult.Success(
                            1, 1, 1, emptyList(), listOf(2)
                        )
                        2 -> SendBulkResult.Success(
                            1, 1, 1, emptyList(), emptyList()
                        )
                        else -> throw IllegalStateException()
                    }
                },
                minRetryDelayMs = 15_000,
                maxRetryDelayMs = 600_000,
            )

            try {
                channel.send(SinkMsg.Data(listOf(1, 2, 3)))

                val flushed = Latch(1)
                launch {
                    channel.send(SinkMsg.Flush(flushed))
                    flushed.await()
                }

                flushed.isReleased shouldBe false

                advanceTimeBy(1_000)
                flushed.isReleased shouldBe false

                advanceTimeBy(14_001)
                flushed.isReleased shouldBe true
            } finally {
                sink.cancel()
                channel.close()
            }
        }
    }
})
