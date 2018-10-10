package company.evo.kafka.elasticsearch

import company.evo.bulk.BulkWriter
import io.kotlintest.Description
import io.kotlintest.Spec
import io.kotlintest.TestCaseConfig
import io.kotlintest.matchers.beEmpty
import io.kotlintest.matchers.between
import io.kotlintest.properties.Gen
import io.kotlintest.should
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import kotlinx.coroutines.CancellationException

import java.io.IOException

import kotlin.system.measureTimeMillis
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull


class SinkActorTests : StringSpec() {
//    override val defaultTestCaseConfig = TestCaseConfig(enabled = false)

    private data class Action(val id: Int) {
        companion object {
            fun seq(startId: Int, size: Int): List<Action> {
                return (startId until startId+size).map { Action(it) }
            }
        }
    }

    private class ActionHasher : Hasher<Action> {
        override fun hash(obj: Action) = obj.id and 0x7FFF_FFFF
    }

    private open class BulkWriterMock : BulkWriter<Action> {
        private val writtenBulks = Channel<List<Action>>(Channel.UNLIMITED)

        override suspend fun write(actions: List<Action>): Boolean {
            writtenBulks.send(actions)
            return true
        }

        fun fetchAllWrittenBulks(): List<List<Action>> {
            val res = arrayListOf<List<Action>>()
            do {
                val bulk = writtenBulks.poll()
                if (bulk != null) {
                    res.add(bulk)
                } else {
                    break
                }
            } while (true)
            return res
        }
    }

    private class FailingBulkWriter : BulkWriter<Any> {
        override suspend fun write(actions: List<Any>): Boolean {
            throw IOException("test")
        }
    }

    private class DelayedBulkWriter(private val delayMs: Long) : BulkWriterMock() {

        override suspend fun write(actions: List<Action>): Boolean {
            delay(delayMs)
            return super.write(actions)
        }
    }

    override fun beforeSpec(description: Description, spec: Spec) {
        super.beforeSpec(description, spec)
        // Warm up the code
        runBlocking {
            val bulkWriter = BulkWriterMock()
            val bulkSize = 1000
            SinkActorImpl(
                    this, bulkWriter, bulkSize, 5
            ).use { sinkActor ->
                for (i in 1..bulkSize) {
                    sinkActor.put(Action(i))
                }
            }
        }
    }

    companion object {
        fun echo(msg: String) = println(msg)
    }

    init {
        "test bulk size" {
            runBlocking {
                val bulkWriter = BulkWriterMock()
                SinkActorImpl(this, bulkWriter, 2).use { sinkActor ->
                    sinkActor.put(Action(1))
                    bulkWriter.fetchAllWrittenBulks().size shouldBe 0

                    sinkActor.put(Action(2))
                    delay(4)
                    bulkWriter.fetchAllWrittenBulks() shouldBe listOf(Action.seq(1, 2))
                }
            }
        }

        "test overflowing bulk size" {
            runBlocking {
                val bulkWriter = DelayedBulkWriter(20)
                SinkActorImpl(this, bulkWriter, 2).use { sinkActor ->
                    (1..4).forEach { sinkActor.put(Action(it)) }

                    // The actor's channel is full thus timeout should happen
                    withTimeoutOrNull(5) {
                        sinkActor.put(Action(5))
                    } shouldBe null
                    bulkWriter.fetchAllWrittenBulks().size shouldBe 0

                    delay(20)
                    bulkWriter.fetchAllWrittenBulks().size shouldBe 1

                    delay(20)
                    bulkWriter.fetchAllWrittenBulks().size shouldBe 1
                }
            }
        }

        "f:test blocking when channel is full" {
            runBlocking {
                val bulkWriter = DelayedBulkWriter(20)
                (1..100).forEach {
                    SinkActorImpl(this, bulkWriter, 2, maxDelayMs = 5).use { sinkActor ->
                        echo("0")
                        measureTimeMillis {
                            (1..2).forEach { sinkActor.put(Action(it)) }
                        }// shouldBe between(0, 18)
                        echo("1")

                        measureTimeMillis {
                            sinkActor.put(Action(3))
                        }// shouldBe between(0, 18)
                        echo("2")

                        measureTimeMillis {
                            sinkActor.put(Action(4))
                        }// shouldBe between(0, 18)
                        echo("3")

                        measureTimeMillis {
                            sinkActor.put(Action(5))
                        }// shouldBe between(16, 24)
                        echo("4")

//                        bulkWriter.fetchAllWrittenBulks().size shouldBe 1
                        echo("5")

                        measureTimeMillis {
                            sinkActor.flush() shouldBe true
                        }// shouldBe between(36L, 44L)
                        echo("6")
                    }
                    println("=".repeat(80))
                }
            }
        }

        "test max delay" {
            runBlocking {
                val bulkWriter = BulkWriterMock()
                SinkActorImpl(this, bulkWriter, 100, maxDelayMs = 5).use { sinkActor ->
                    sinkActor.put(Action(1))
                    delay(3)
                    bulkWriter.fetchAllWrittenBulks().size shouldBe 0

                    delay(4)
                    bulkWriter.fetchAllWrittenBulks() shouldBe listOf(Action.seq(1, 1))
                }
            }
        }

        "delay between bulks" {
            runBlocking {
                val bulkWriter = BulkWriterMock()
                SinkActorImpl(this, bulkWriter, 2, delayBetweenBulksMs = 20).use { sinkActor ->
                    measureTimeMillis {
                        sinkActor.put(Action(1))
                        sinkActor.flush()
                    } shouldBe between(0, 5)

                    measureTimeMillis {
                        sinkActor.put(Action(2))
                        sinkActor.flush()
                    } shouldBe between(18, 22)
                }
            }
        }

        "test flush" {
            runBlocking {
                val bulkWriter = BulkWriterMock()
                SinkActorImpl(this, bulkWriter, 10, maxDelayMs = 5).use { sinkActor ->
                    sinkActor.put(Action(1))
                    delay(2)
                    bulkWriter.fetchAllWrittenBulks() should beEmpty()

                    sinkActor.flush() shouldBe true
                    bulkWriter.fetchAllWrittenBulks() shouldBe listOf(Action.seq(1, 1))
                }
            }
        }

        "test flush timeout" {
            runBlocking {
                val bulkWriter = DelayedBulkWriter(10)
                SinkActorImpl(this, bulkWriter, 10, maxDelayMs = 5).use { sinkActor ->
                    sinkActor.put(Action(1))
                    sinkActor.flush() shouldBe true
                }
            }
        }

        "test empty flush" {
            runBlocking {
                val bulkWriter = BulkWriterMock()
                SinkActorImpl(this, bulkWriter, 2).use { sinkActor ->
                    sinkActor.flush() shouldBe true
                    bulkWriter.fetchAllWrittenBulks() shouldBe emptyList<List<Action>>()
                }
            }
        }

        "test flush by flush" {
            runBlocking {
                val bulkWriter = BulkWriterMock()
                SinkActorImpl(this, bulkWriter, 3).use { sinkActor ->
                    sinkActor.put(Action(1))
                    sinkActor.flush() shouldBe true
                    bulkWriter.fetchAllWrittenBulks() shouldBe listOf(Action.seq(1, 1))

                    (2..3).forEach { sinkActor.put(Action(it)) }
                    sinkActor.flush() shouldBe true
                    bulkWriter.fetchAllWrittenBulks() shouldBe listOf(Action.seq(2, 2))
                }
            }
        }

        "test put after closing" {
            runBlocking {
                val bulkWriter = BulkWriterMock()
                val sinkActor = SinkActorImpl(this, bulkWriter, 3)

                sinkActor.put(Action(0))
                sinkActor.close()

                shouldThrow<CancellationException> {
                    sinkActor.put(Action(1))
                }
                shouldThrow<CancellationException> {
                    sinkActor.flush()
                }

                bulkWriter.fetchAllWrittenBulks().size shouldBe 0
            }
        }

        "test puts than immediate flush" {
            runBlocking {
                val bulkWriter = BulkWriterMock()
                val sizes = Gen.choose(0, 10).random().asIterable().iterator()
                withContext(Dispatchers.Default) {
                    SinkActorImpl(this, bulkWriter, 2).use { sinkActor ->
                        for (i in (1..1000)) {
                            val size = sizes.next()
                            (0 until size).forEach { sinkActor.put(Action(i + it)) }
                            sinkActor.flush() shouldBe true
                            bulkWriter.fetchAllWrittenBulks() shouldBe Action.seq(i, size).chunked(2)
                        }
                    }
                }
            }
        }

        "use after flush was timed out" {
            runBlocking {
                val bulkWriter = DelayedBulkWriter(20)
                SinkActorImpl(this, bulkWriter, 2).use { sinkActor ->
                    (1..4).forEach { sinkActor.put(Action(it)) }

                    withTimeoutOrNull(30) {
                        sinkActor.flush()
                    } shouldBe null
                    bulkWriter.fetchAllWrittenBulks() shouldBe listOf(Action.seq(1, 2))

                    sinkActor.put(Action(5))
                    measureTimeMillis {
                        sinkActor.flush()
                    } shouldBe between(28, 32)
                    bulkWriter.fetchAllWrittenBulks() shouldBe Action.seq(3, 3).chunked(2)
                }
            }
        }

        "sink with 2 actors" {
            runBlocking {
                val bulkWriter = BulkWriterMock()
                SinkImpl(ActionHasher(), 2) {
                    SinkActorImpl(this, bulkWriter, 2)
                }.use { sink ->
                    sink.put(Action(1))
                    sink.put(Action(2))
                    sink.flush()
                    bulkWriter.fetchAllWrittenBulks() shouldBe listOf(Action.seq(2, 1), Action.seq(1, 1))
                }
            }
        }

        "sink with 2 actors and delayed writer" {
            runBlocking {
                val bulkWriter = DelayedBulkWriter(20)
                SinkImpl(ActionHasher(), 2) {
                    SinkActorImpl(this, bulkWriter, 2, maxDelayMs = 5)
                }.use { sink ->
                    measureTimeMillis {
                        (1..4).forEach { sink.put(Action(it)) }
                    } shouldBe between(0, 2)

                    measureTimeMillis {
                        (5..8).forEach { sink.put(Action(it)) }
                    } shouldBe between(0, 2)

                    measureTimeMillis {
                        (9..10).forEach { sink.put(Action(it)) }
                    } shouldBe between(18, 22)

                    bulkWriter.fetchAllWrittenBulks() shouldBe listOf(
                            listOf(Action(2), Action(4)),
                            listOf(Action(1), Action(3))
                    )

                    measureTimeMillis {
                        sink.flush()
                    } shouldBe between(38, 42)

                    bulkWriter.fetchAllWrittenBulks() shouldBe listOf(
                            listOf(Action(6), Action(8)),
                            listOf(Action(5), Action(7)),
                            listOf(Action(10)),
                            listOf(Action(9))
                    )
                }
            }
        }
    }
}
