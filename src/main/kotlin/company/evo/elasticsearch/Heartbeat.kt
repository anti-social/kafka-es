package company.evo.elasticsearch

import java.io.IOException
import java.util.concurrent.atomic.AtomicBoolean

import io.searchbox.client.JestClient
import io.searchbox.core.Ping

import org.slf4j.LoggerFactory


internal class Heartbeat(
        private val esClient: JestClient,
        private val heartbeatInterval: Int,
        private val monitor: Object,
        private val waitingElastic: AtomicBoolean
) : Runnable
{
    companion object {
        val logger = LoggerFactory.getLogger(Heartbeat::class.java)
    }

    override fun run() = synchronized(monitor) {
        while (!Thread.interrupted()) {
            try {
                monitor.wait()
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
                    waitingElastic.set(false)
                    monitor.notifyAll()
                    break
                }
            } catch (e: IOException) {
                continue
            }
        }
    }
}