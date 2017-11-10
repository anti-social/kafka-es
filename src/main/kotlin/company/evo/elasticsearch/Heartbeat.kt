package company.evo.elasticsearch

import java.io.IOException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock

import kotlin.concurrent.withLock

import io.searchbox.client.JestClient
import io.searchbox.core.Ping

import org.slf4j.LoggerFactory


internal class Heartbeat(
        private val esClient: JestClient,
        private val heartbeatInterval: Int
) : Runnable
{
    private val lock = ReentrantLock()
    private val elasticUnavailable = lock.newCondition()
    private val elasticAvailable = lock.newCondition()
    private val waitingElastic = AtomicBoolean()

    companion object {
        private val logger = LoggerFactory.getLogger(Heartbeat::class.java)
    }

    override fun run() {
        while (!Thread.interrupted()) {
            try {
                lock.withLock {
                    while(!waitingElastic.get()) {
                        elasticUnavailable.await()
                    }
                }
                pulse()
            } catch (e: InterruptedException) {}
        }
    }

    private fun pulse() {
        logger.info("Heartbeat started")
        while (true) {
            Thread.sleep(heartbeatInterval * 1000L)
            try {
                // TODO(Check status for sink indexes)
                val res = esClient.execute(Ping.Builder().build())
                if (res.isSucceeded) {
                    logger.info("Heartbeat finished")
                    lock.withLock {
                        waitingElastic.set(false)
                        elasticAvailable.signalAll()
                    }
                    break
                }
            } catch (e: IOException) {
                continue
            }
        }
    }

    fun start() = lock.withLock {
        waitingElastic.set(true)
        elasticUnavailable.signal()
        logger.info("Waiting for elasticsearch ...")
        while (waitingElastic.get()) {
            elasticAvailable.await()
        }
    }

    fun isWaitingElastic() = waitingElastic.get()
}