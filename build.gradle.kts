import com.google.protobuf.gradle.ExecutableLocator
import com.jfrog.bintray.gradle.BintrayExtension
import java.nio.file.Paths
import java.util.Date
import org.gradle.jvm.tasks.Jar
import org.jetbrains.kotlin.gradle.plugin.KotlinPluginWrapper
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

buildscript {
    val kotlinVersion = "1.3.0-rc-131"

    repositories {
        maven(url = "https://dl.bintray.com/kotlin/kotlin-eap")
        mavenCentral()
    }

    dependencies {
        classpath("org.jetbrains.kotlin:kotlin-gradle-plugin:$kotlinVersion")
    }
}

repositories {
    maven(url = "https://dl.bintray.com/kotlin/kotlin-eap")
    mavenCentral()
}

plugins {
    application
    java
    `maven-publish`
    id("com.google.protobuf") version "0.8.5"
    id("com.jfrog.bintray") version "1.8.2"
    id("org.ajoberstar.grgit") version "2.2.1"
}

apply {
    plugin("kotlin")
}

repositories {
     mavenCentral()
}

val grgit: org.ajoberstar.grgit.Grgit by extra
val gitDescribe = grgit.describe(mapOf("match" to listOf("v*")))
        ?: "v0.0.0-unknown"
version = gitDescribe.trimStart('v')

val kafkaVersion = "1.0.2"
val jestVersion = "5.3.3"
val protobufVersion = "3.5.1"
val junitJupiterVersion = "5.2.0"

dependencies {
    val kotlintestVersion = "3.1.10"
    val mockkVersion = "1.8.9.kotlin13"

    compile(kotlin("stdlib-jdk8"))
    compile(kotlin("reflect"))
    compile("org.jetbrains.kotlinx", "kotlinx-coroutines-core", "0.30.1-eap13")
    compile("org.jetbrains.kotlinx", "kotlinx-coroutines-jdk8", "0.30.1-eap13")

    compile("org.apache.kafka:connect-api:$kafkaVersion")
    compile("org.apache.kafka:connect-json:$kafkaVersion")
    compile("org.apache.kafka:connect-runtime:$kafkaVersion")

    compile("io.searchbox:jest:$jestVersion")
    compile("io.searchbox:jest-common:$jestVersion")

    compile("com.google.protobuf:protobuf-java:$protobufVersion")
    compile("com.google.protobuf:protobuf-java-util:$protobufVersion")

    compile("com.fasterxml.jackson.module", "jackson-module-kotlin", "2.9.4.1")

    testCompile("io.kotlintest", "kotlintest-core", kotlintestVersion)
    testCompile("io.kotlintest", "kotlintest-assertions", kotlintestVersion)
    testCompile("io.kotlintest", "kotlintest-runner-junit5", kotlintestVersion)

    testCompile("io.mockk", "mockk", mockkVersion)

    testCompile("org.elasticsearch.client", "elasticsearch-rest-high-level-client", "6.4.2")

    testCompile("org.slf4j", "slf4j-simple", "1.8.0-beta2")
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
        val includeTags = project.properties["include"]?.also {
            systemProperty("kotlintest.tags.include", it)
        }
        val defaultExcludeTags = if (includeTags == null ) "Integration" else null
        (project.properties["exclude"] ?: defaultExcludeTags)?.let {
            systemProperty("kotlintest.tags.exclude", it)
        }

        useJUnitPlatform()
        outputs.upToDateWhen { false }
    }
}

sourceSets {
    getByName("main").java.srcDirs(
            Paths.get(protobuf.protobuf.generatedFilesBaseDir, "main", "java")
    )
    getByName("test").java.srcDirs(
            Paths.get(protobuf.protobuf.generatedFilesBaseDir, "test", "java")
    )
}

val javaVersion = JavaVersion.VERSION_1_8.toString()

tasks.withType(JavaCompile::class.java) {
    sourceCompatibility = javaVersion
    targetCompatibility = javaVersion
}
tasks.withType(KotlinCompile::class.java) {
    kotlinOptions {
        jvmTarget = javaVersion
    }
}

val jar by tasks.getting(Jar::class)

val sourceJar by tasks.creating(Jar::class) {
    classifier = "sources"
    from(sourceSets["main"].allSource)
}

publishing {
    publications {
        register("jar", MavenPublication::class) {
            groupId = "company.evo"
            artifactId = project.name
            version = project.version.toString()
            from(components["java"])
            artifact(sourceJar)
        }
    }
}

bintray {
    user = properties["bintrayUser"]?.toString()
            ?: System.getenv("BINTRAY_USER")
    key = properties["bintrayApiKey"]?.toString()
            ?: System.getenv("BINTRAY_API_KEY")
    pkg(delegateClosureOf<BintrayExtension.PackageConfig> {
        repo = "maven"
        name = project.name
        userOrg = "evo"
        setLicenses("Apache-2.0")
        setLabels("kafka-connect", "elasticsearch-connector", "kafka-elasticsearch-sink")
        vcsUrl = "https://github.com/anti-social/kafka-es.git"
        version(delegateClosureOf<BintrayExtension.VersionConfig> {
            name = "kafka-es"
            released = Date().toString()
            vcsTag = gitDescribe
        })
    })
    setPublications("jar")
    publish = true
    dryRun = hasProperty("bintrayDryRun")
}
