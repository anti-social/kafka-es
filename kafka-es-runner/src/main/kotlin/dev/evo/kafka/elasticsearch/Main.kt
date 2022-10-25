package dev.evo.kafka.elasticsearch

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.context
import com.github.ajalt.clikt.core.findOrSetObject
import com.github.ajalt.clikt.core.requireObject
import com.github.ajalt.clikt.core.subcommands
import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.arguments.multiple
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.int

import dev.evo.prometheus.ktor.MetricsFeature
import dev.evo.prometheus.ktor.metricsModule

import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty

import org.apache.kafka.connect.cli.ConnectDistributed
import org.apache.kafka.connect.cli.ConnectStandalone

private const val METRICS_APP_STOP_GRACE_PERIOD_MS = 1000L
private const val METRICS_APP_STOP_TIMEOUT_MS = 2000L

class Cmd : CliktCommand() {
    override fun run() {}
}

sealed class KafkaConnectCmd(
    private val runner: (Array<String>) -> Unit,
    name: String? = null,
) : CliktCommand(name = name, treatUnknownOptionsAsArgs = true) {

    val metricsPort by option(
        help = "Metrics port to listen on. If not set, an application won't expose its metrics"
    ).int()

    val arguments by argument().multiple()

    override fun run() {
        val metricsApp = metricsPort?.let { metricsPort ->
            ElasticsearchSinkTask.installMetrics(Metrics.kafkaEsMetrics)

            embeddedServer(
                Netty,
                port = metricsPort,
                module = {
                    metricsModule(MetricsFeature(Metrics))
                }
            )
                .start(wait = false)
        }


        runner(arguments.toTypedArray())

        metricsApp?.stop(METRICS_APP_STOP_GRACE_PERIOD_MS, METRICS_APP_STOP_TIMEOUT_MS)
    }
}

class Standalone : KafkaConnectCmd(ConnectStandalone::main)

class Distributed : KafkaConnectCmd(ConnectDistributed::main)

fun runWithSubcommands(subcommands: List<CliktCommand>, args: Array<String>) {
    Cmd()
        .subcommands(subcommands)
        .main(args)
}

fun main(args: Array<String>) {
    runWithSubcommands(
        listOf(Standalone(), Distributed()),
        args
    )
}
