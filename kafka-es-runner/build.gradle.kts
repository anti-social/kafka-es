plugins {
    application
}

dependencies {
    implementation(project(":"))
    implementation("com.xenomachina:kotlin-argparser:${Versions.argparser}")
    implementation("org.apache.kafka:connect-runtime:${Versions.kafka}")
    implementation("dev.evo.prometheus:prometheus-kt-ktor:${Versions.prometheusKt}")
    implementation("io.ktor:ktor-server-core:${Versions.ktor}")
    implementation("io.ktor:ktor-server-netty:${Versions.ktor}")
}

tasks.withType(org.jetbrains.kotlin.gradle.tasks.KotlinCompile::class.java) {
    kotlinOptions {
        jvmTarget = Versions.java.toString()
        freeCompilerArgs = listOf(
            "-opt-in=kotlin.time.ExperimentalTime",
        )
    }
}

application {
    mainClass.set("dev.evo.kafka.elasticsearch.MainKt")
    applicationDefaultJvmArgs = listOf(
        "-Dlog4j.configuration=file:config/connect-log4j.properties"
    )
}
