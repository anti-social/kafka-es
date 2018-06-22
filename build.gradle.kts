import com.google.protobuf.gradle.ExecutableLocator
import org.jetbrains.kotlin.gradle.plugin.KotlinPluginWrapper
import java.nio.file.Paths
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    application
    java
    id("org.jetbrains.kotlin.jvm") version "1.2.50"
    id("com.google.protobuf") version "0.8.5"
}

repositories {
     mavenCentral()
}

val kafkaVersion = "1.0.1"
val jestVersion = "5.3.3"
val protobufVersion = "3.5.1"
val junitJupiterVersion = "5.2.0"

dependencies {
    protobuf("com.google.protobuf:protobuf-java:$protobufVersion")

    compile(kotlin("stdlib-jdk8"))
    compile("org.apache.kafka:connect-api:$kafkaVersion")
    compile("org.apache.kafka:connect-json:$kafkaVersion")
    compile("org.apache.kafka:connect-runtime:$kafkaVersion")
    compile("io.searchbox:jest:$jestVersion")
    compile("io.searchbox:jest-common:$jestVersion")
    compile("com.google.protobuf:protobuf-java:$protobufVersion")
    compile("com.google.protobuf:protobuf-java-util:$protobufVersion")

    testCompile("org.junit.jupiter:junit-jupiter-api:$junitJupiterVersion")
    testRuntime("org.junit.jupiter:junit-jupiter-engine:$junitJupiterVersion")
    testCompile("org.assertj:assertj-core:3.8.0")
}

application {
    mainClassName = "org.apache.kafka.connect.cli.ConnectStandalone"
    applicationDefaultJvmArgs = listOf(
            "-Dlog4j.configuration=file:config/connect-log4j.properties"
    )
}

protobuf.protobuf.run {
    protoc(closureOf<ExecutableLocator> {
        artifact = "com.google.protobuf:protoc:$protobufVersion"
    })
}

tasks {
    val run by getting(JavaExec::class) {
        System.getProperty("exec.args")?.let {
            args = it.trim().split("\\s+".toRegex())
        }
    }
    val test by getting(Test::class) {
        useJUnitPlatform()
    }
}

java.sourceSets {
    getByName("main").java.srcDirs(
            Paths.get(protobuf.protobuf.generatedFilesBaseDir, "main", "java")
    )
    getByName("test").java.srcDirs(
            Paths.get(protobuf.protobuf.generatedFilesBaseDir, "test", "java")
    )
}

val compileKotlin by tasks.getting(KotlinCompile::class) {
    dependsOn("generateProto")
}
val compileTestKotlin by tasks.getting(KotlinCompile::class) {
    dependsOn("generateTestProto")
}
