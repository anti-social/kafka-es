package dev.evo.kafka.elasticsearch

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe

import kotlin.time.TestTimeSource
import kotlin.time.milliseconds

import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runBlockingTest

class ReoutingActorTests : StringSpec({
    "routing" {
        runBlockingTest {
            val inChannel = Channel<SinkMsg<Int>>(Channel.RENDEZVOUS)
            val outChannels = Array(3) {
                Channel<SinkMsg<Int>>(Channel.UNLIMITED)
            }
            val router = RoutingActor(
                this,
                inChannel,
                outChannels.toList().toTypedArray(),
            ) { v -> v }

            try {
                inChannel.send(SinkMsg.Data(listOf(0, 1, 4, Int.MIN_VALUE, Int.MAX_VALUE)))
                inChannel.send(SinkMsg.Data(listOf(0, Int.MAX_VALUE)))
                outChannels[0].poll() shouldBe SinkMsg.Data(listOf(0, Int.MIN_VALUE))
                outChannels[0].poll() shouldBe SinkMsg.Data(listOf(0))
                outChannels[0].poll() shouldBe null
                outChannels[1].poll() shouldBe SinkMsg.Data(listOf(1, 4, Int.MAX_VALUE))
                outChannels[1].poll() shouldBe SinkMsg.Data(listOf(Int.MAX_VALUE))
                outChannels[1].poll() shouldBe null
                outChannels[2].poll() shouldBe null
            } finally {
                router.cancel()
            }
        }
    }
})

class BulkActorTests : StringSpec({
    "bulk actor: no max delay" {
        runBlockingTest {
            val channel = Channel<SinkMsg<Int>>(0)
            val bulkChannel = Channel<SinkMsg<Int>>(Channel.UNLIMITED)
            val bulker = BulkActor(
                this,
                channel,
                bulkChannel,
                2,
            )

            try {
                channel.send(SinkMsg.Data(emptyList()))
                channel.send(SinkMsg.Data(emptyList()))
                bulkChannel.poll() shouldBe null

                channel.send(SinkMsg.Data(listOf(1)))
                channel.send(SinkMsg.Data(listOf(2)))
                bulkChannel.poll() shouldBe SinkMsg.Data(listOf(1, 2))
                bulkChannel.poll() shouldBe null

                channel.send(SinkMsg.Data(listOf(1, 2, 3, 4, 5)))
                bulkChannel.poll() shouldBe SinkMsg.Data(listOf(1, 2))
                bulkChannel.poll() shouldBe SinkMsg.Data(listOf(3, 4))
                bulkChannel.poll() shouldBe null
                channel.send(SinkMsg.Data(listOf(6)))
                bulkChannel.poll() shouldBe SinkMsg.Data(listOf(5, 6))
                bulkChannel.poll() shouldBe null
            } finally {
                channel.close()
                bulkChannel.close()
            }
        }
    }

    "bulk actor: with max delay" {
        runBlockingTest {
            val channel = Channel<SinkMsg<Int>>(0)
            val bulkChannel = Channel<SinkMsg<Int>>(Channel.UNLIMITED)
            val clock = TestTimeSource()
            val bulker = BulkActor(
                this,
                channel,
                bulkChannel,
                3,
                10,
                clock,
            )

            try {
                channel.send(SinkMsg.Data(listOf(1)))
                bulkChannel.poll() shouldBe null

                clock += 8.milliseconds
                advanceTimeBy(8)
                bulkChannel.poll() shouldBe null

                channel.send(SinkMsg.Data(listOf(2)))

                clock += 1.milliseconds
                advanceTimeBy(1)
                bulkChannel.poll() shouldBe null

                clock += 1.milliseconds
                advanceTimeBy(1)
                bulkChannel.poll() shouldBe SinkMsg.Data(listOf(1, 2))
            } finally {
                channel.close()
                bulkChannel.close()
            }
        }
    }

    "bulk actor: with max delay after flush by size" {
        runBlockingTest {
            val channel = Channel<SinkMsg<Int>>(0)
            val bulkChannel = Channel<SinkMsg<Int>>(Channel.UNLIMITED)
            val clock = TestTimeSource()
            val bulker = BulkActor(
                this,
                channel,
                bulkChannel,
                3,
                10,
                clock,
            )

            try {
                clock += 8.milliseconds
                advanceTimeBy(8)
                channel.send(SinkMsg.Data(listOf(1)))
                bulkChannel.poll() shouldBe null

                clock += 9.milliseconds
                advanceTimeBy(9)
                bulkChannel.poll() shouldBe null
                channel.send(SinkMsg.Data(listOf(2, 3, 4)))
                bulkChannel.poll() shouldBe SinkMsg.Data(listOf(1, 2, 3))

                clock += 9.milliseconds
                advanceTimeBy(9)
                bulkChannel.poll() shouldBe null

                clock += 1.milliseconds
                advanceTimeBy(1)
                bulkChannel.poll() shouldBe SinkMsg.Data(listOf(4))
            } finally {
                channel.close()
                bulkChannel.close()
            }
        }
    }
})

class BulkSinkActorTests : StringSpec({
    "retry on error" {
        runBlockingTest {
            val channel = Channel<SinkMsg<Unit>>()
            var retries = 0
            val actor = BulkSinkActor(
                this,
                "<test>",
                channel,
                {
                    retries++
                    when (retries) {
                        1 -> SendBulkResult.IOError
                        2 -> SendBulkResult.Success(1, 1, 1, emptyList())
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

                advanceTimeBy(1_000)
                flushed.isReleased shouldBe true
            } finally {
                channel.close()
            }
        }
    }

    "retry on timeout" {
        runBlockingTest {
            val channel = Channel<SinkMsg<Unit>>()
            var retries = 0
            val actor = BulkSinkActor(
                this,
                "<test>",
                channel,
                {
                    retries++
                    when (retries) {
                        1 -> {
                            delay(3_000)
                            SendBulkResult.Timeout
                        }
                        2 -> {
                            SendBulkResult.Success(1, 1, 1, emptyList())
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

                advanceTimeBy(1_000)
                flushed.isReleased shouldBe true
            } finally {
                channel.close()
            }
        }
    }

    "retry partially" {
        runBlockingTest {
            val channel = Channel<SinkMsg<Int>>()
            var retries = 0
            val actor = BulkSinkActor(
                this,
                "<test>",
                channel,
                {
                    retries++
                    when (retries) {
                        1 -> SendBulkResult.Success(1, 1, 1, listOf(2))
                        2 -> SendBulkResult.Success(1, 1, 1, emptyList())
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

                advanceTimeBy(14_000)
                flushed.isReleased shouldBe true
            } finally {
                channel.close()
            }
        }
    }
})
