package dev.evo.kafka.elasticsearch

import com.xenomachina.argparser.ArgParser
import com.xenomachina.argparser.default
import com.xenomachina.argparser.mainBody

import dev.evo.prometheus.ktor.MetricsFeature
import dev.evo.prometheus.ktor.metricsModule

import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty

import org.apache.kafka.connect.cli.ConnectDistributed
import org.apache.kafka.connect.cli.ConnectStandalone

class Args(parser: ArgParser) {
    val metricsPort by parser.storing(
            "--metrics-port",
            help = "Metrics port to listen on"
    ) { toInt() }
            .default(9090)
    val connectRunner by parser.mapping(
            "--standalone" to ConnectStandalone::main,
            "--distributed" to ConnectDistributed::main,
            help = "Mode: standalone or distributed"
    )
    val connectConfigs by parser.positionalList(
            "CONFIGS",
            help = "Kafka connect configs"
    )
}

fun main(args: Array<String>) = mainBody {
    val parsedArgs = ArgParser(args).parseInto(::Args)

    val metricsApp = embeddedServer(
            Netty,
            port = parsedArgs.metricsPort,
            module = {
                metricsModule(MetricsFeature(Metrics))
            }
    )
            .start(wait = false)

    ElasticsearchSinkTask.installMetrics(Metrics.kafkaEsMetrics)

    parsedArgs.connectRunner(parsedArgs.connectConfigs.toTypedArray())
    metricsApp.stop(1000, 2000)
}
