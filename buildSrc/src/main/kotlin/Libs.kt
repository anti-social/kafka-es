import org.gradle.api.JavaVersion

object Versions {
    val java = JavaVersion.VERSION_1_8

    // Kotlin and its libraries
    const val kotlin = "1.7.20"
    const val kotlinxCoroutines = "1.6.4"
    const val kotlinxSerialization = "1.4.1"

    // Libraries
    const val kafka = "3.3.1"
    const val protobuf = "3.19.1"
    const val elasticmagic = "0.0.16"
    const val ktor = "2.1.2"
    const val slf4j = "1.7.36"

    // Tests
    val kotest = "5.5.1"

    // Runner
    const val argparser = "2.0.7"
    const val prometheusKt = "0.2.0"
}
