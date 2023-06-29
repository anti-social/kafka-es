package dev.evo.kafka

import org.slf4j.LoggerFactory

import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ConcurrentHashMap

object WatchDog {
    private data class TaskKey(val connector: String, val taskId: Int) {
        override fun toString(): String {
            return "$connector|task-$taskId"
        }
    }

    private data class TaskValue(
        var timeLeftMs: Long,
        // A timeout after which a task is considered stuck
        val stuckTimeoutMs: Long,
        // A timeout for stuck tasks to restart them again
        val retryTimeoutMs: Long,
    )

    private val restApiBaseUrl = System.getenv("KAFKA_CONNECT_API_URL") ?: "http://localhost:8083"

    private val registeredTasks = ConcurrentHashMap<TaskKey, TaskValue>()
    private val channel = ArrayBlockingQueue<TaskKey>(64)

    private val logger = LoggerFactory.getLogger(WatchDog::class.java)

    init {
        startWatchDog()
        startRestarter()
    }

    private fun startWatchDog() {
        val checkIntervalMs = 60_000L
        val thread = Thread(
            {
                logger.info("Starting watch dog thread")

                while (true) {
                    Thread.sleep(checkIntervalMs)

                    registeredTasks.replaceAll { key, value ->
                        value.timeLeftMs -= checkIntervalMs
                        if (value.timeLeftMs <= 0) {
                            logger.warn("${key} found in a stuck state")
                            if (!channel.offer(key)) {
                                logger.warn("Watch dog's channel is full")
                            }
                            value.timeLeftMs = value.retryTimeoutMs
                        }
                        value
                    }
                }
            },
            "watch-dog"
        )
        thread.start()
    }

    private fun startRestarter() {
        val client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build()
        val thread = Thread(
            {
                logger.info("Starting watch dog restarter thread")

                while (true) {
                    val task = channel.take()
                    try {
                        logger.info("Restarting stuck task: ${task}")
                        val request = HttpRequest.newBuilder(URI("$restApiBaseUrl/connectors/${task.connector}/tasks/${task.taskId}/restart"))
                            .timeout(Duration.ofSeconds(10))
                            .POST(HttpRequest.BodyPublishers.ofString(""))
                            .build()
                        val response = client.send(request, HttpResponse.BodyHandlers.ofString())
                        val statusCode = response.statusCode()
                        if (statusCode !in 200..299) {
                            logger.warn("Unsuccessful response when restarting $task task: $statusCode\n${response.body()}")
                        }
                    } catch (e: Exception) {
                        logger.error("Error when restarting $task task: $e")
                    }
                }
            },
            "watch-dog-restarter"
        )
        thread.start()
    }

    fun register(connector: String, taskId: Int, stuckTimeoutMs: Long, retryTimeoutMs: Long) {
        registeredTasks.putIfAbsent(
            TaskKey(connector, taskId),
            TaskValue(
                timeLeftMs = stuckTimeoutMs,
                stuckTimeoutMs = stuckTimeoutMs,
                retryTimeoutMs = retryTimeoutMs,
            )
        )
    }

    fun unregister(connector: String, taskId: Int) {
        registeredTasks.remove(TaskKey(connector, taskId))
    }

    fun kick(connector: String, taskId: Int) {
        registeredTasks.computeIfPresent(
            TaskKey(connector, taskId),
            { key, value ->
                value.timeLeftMs = value.stuckTimeoutMs
                value
            }
        )
    }
}
