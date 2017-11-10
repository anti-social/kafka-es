package company.evo.elasticsearch

import java.io.IOException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.Condition
import java.util.concurrent.locks.Lock

import kotlin.concurrent.withLock

import io.searchbox.client.JestClient
import io.searchbox.core.Ping

import org.slf4j.LoggerFactory


internal class Heartbeat(
        private val esClient: JestClient,
        private val heartbeatInterval: Int,
        private val lock: Lock,
        private val elasticUnavailable: Condition,
        private val elasticAvailable: Condition,
        private val waitingElastic: AtomicBoolean
) : Runnable
{
    companion object {
        val logger = LoggerFactory.getLogger(Heartbeat::class.java)
    }

    override fun run() {
        while (!Thread.interrupted()) {
            try {
                lock.withLock {
                    while(!waitingElastic.get()) {
                        elasticUnavailable.await()
                    }
                }
                waitElastic()
            } catch (e: InterruptedException) {}
        }
    }

    private fun waitElastic() {
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
}