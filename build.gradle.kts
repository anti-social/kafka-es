import com.google.protobuf.gradle.protobuf
import com.google.protobuf.gradle.protoc

import java.nio.file.Paths

import org.gradle.jvm.tasks.Jar

import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    application
    java
    jacoco
    `maven-publish`
    signing
    id("io.github.gradle-nexus.publish-plugin") version "1.1.0"
    id("org.jetbrains.kotlin.jvm") version "1.5.31"
    kotlin("plugin.serialization") version "1.5.31"
    id("com.google.protobuf") version "0.8.17"
    id("org.ajoberstar.grgit") version "4.1.0"
}

repositories {
    mavenCentral()
}

group = "dev.evo.kafka-es"

val gitDescribe = grgit.describe(mapOf("tags" to true, "match" to listOf("v*")))
        ?: "v0.0.0-unknown"
version = gitDescribe.trimStart('v')

val kotlinCoroutinesVersion = "1.5.2"
val kotlinSerializationVersion = "1.3.0-RC"
val kafkaVersion = "3.0.0"
val protobufVersion = "3.19.1"
val junitJupiterVersion = "5.2.0"
val assertjVersion = "3.8.0"
val kotestVersion = "4.4.3"
val esTransportVersion = "0.0.9"
val prometheusKtVersion = "0.1.2"
val ktorVersion = "1.6.4"
val argparserVersion = "2.0.7"

dependencies {
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:$kotlinCoroutinesVersion")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:$kotlinSerializationVersion")
    implementation("org.apache.kafka:connect-api:$kafkaVersion")
    implementation("org.apache.kafka:connect-runtime:$kafkaVersion")
    implementation("com.google.protobuf:protobuf-java:$protobufVersion")
    implementation("com.google.protobuf:protobuf-java-util:$protobufVersion")

    api("dev.evo.elasticart:elasticart-elasticsearch-transport:$esTransportVersion")
    implementation("dev.evo.prometheus:prometheus-kt-ktor:$prometheusKtVersion")
    implementation("io.ktor:ktor-client-cio:$ktorVersion")
    implementation("io.ktor:ktor-server-core:$ktorVersion")
    implementation("io.ktor:ktor-server-netty:$ktorVersion")

    implementation("com.xenomachina:kotlin-argparser:$argparserVersion")

    testImplementation("org.junit.jupiter:junit-jupiter-api:$junitJupiterVersion")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:$junitJupiterVersion")
    testImplementation("org.assertj:assertj-core:$assertjVersion")
    testImplementation("io.kotest:kotest-runner-junit5:$kotestVersion")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:$kotlinCoroutinesVersion")
}

application {
    mainClass.set("dev.evo.kafka.elasticsearch.MainKt")
    applicationDefaultJvmArgs = listOf(
        "-Dlog4j.configuration=file:config/connect-log4j.properties"
    )
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:$protobufVersion"
    }
}

tasks {
    val test by getting(Test::class) {
        useJUnitPlatform()
    }
    val jacocoTestReport by getting(JacocoReport::class) {
        reports {
            html.isEnabled = true
            xml.isEnabled = true
        }
    }
}

sourceSets["main"].java {
    srcDir(
            Paths.get(protobuf.protobuf.generatedFilesBaseDir, "main", "java")
    )
}
sourceSets["test"].java {
    srcDir(
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
        freeCompilerArgs = listOf(
            "-Xopt-in=kotlin.ExperimentalStdlibApi",
            "-Xopt-in=kotlin.time.ExperimentalTime",
            "-Xopt-in=kotlinx.coroutines.ExperimentalCoroutinesApi"
        )
    }
}

val compileKotlin by tasks.getting(KotlinCompile::class) {
    dependsOn("generateProto")
}
val compileTestKotlin by tasks.getting(KotlinCompile::class) {
    dependsOn("generateTestProto")
}

val jar by tasks.getting(Jar::class)

val sourceJar by tasks.creating(Jar::class) {
    archiveClassifier.set("sources")
    from(sourceSets["main"].allSource)
}

val javadocJar by project.tasks.registering(Jar::class) {
    archiveClassifier.set("javadoc")
}

signing {
    sign(publishing.publications)
}

publishing {
    publications {
        create<MavenPublication>("jar") {
            groupId = project.group.toString()
            artifactId = project.name
            version = project.version.toString()
            from(components["java"])
            artifact(sourceJar)
            artifact(javadocJar)

            pom {
                name.set("kafka-es")
                description.set("Kafka connect elasticsearch sink")
                url.set("https://github.com/anti-social/kafka-es")

                licenses {
                    license {
                        name.set("The Apache License, Version 2.0")
                        url.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
                    }
                }

                scm {
                    url.set("https://github.com/anti-social/kafka-es")
                    connection.set("scm:https://github.com/anti-social/kafka-es.git")
                    developerConnection.set("scm:git://github.com/anti-social/kafka-es.git")
                }

                developers {
                    developer {
                        id.set("anti-social")
                        name.set("Oleksandr Koval")
                        email.set("kovalidis@gmail.com")
                    }
                }
            }
        }
    }

    repositories {
        maven {
            name = "test"
            url = uri("file://${buildDir}/repos/testMaven")
        }

        maven {
            val gitlabRepoUrl = findProperty("gitlabRepoUrl")?.toString()
                ?: System.getenv("GITLAB_REPO_URL")
            val gitlabToken = project.properties["gitlabToken"]?.toString()
                ?: System.getenv("GITLAB_TOKEN")

            name = "gitlab"
            if (gitlabRepoUrl != null) {
                url = uri(gitlabRepoUrl)
                credentials(HttpHeaderCredentials::class) {
                    name = "Job-Token"
                    value = gitlabToken
                }
                authentication {
                    create<HttpHeaderAuthentication>("header")
                }
            }
        }
    }


}

nexusPublishing {
    repositories {
        sonatype {
            val baseSonatypeUrl = project.properties["sonatypeUrl"]?.toString()
                ?: System.getenv("SONATYPE_URL")
                ?: "https://s01.oss.sonatype.org"

            nexusUrl.set(uri("$baseSonatypeUrl/service/local/"))
            snapshotRepositoryUrl.set(uri("$baseSonatypeUrl/content/repositories/snapshots/"))

            val sonatypeUser = project.properties["sonatypeUser"]?.toString()
                ?: System.getenv("SONATYPE_USER")
            val sonatypePassword = project.properties["sonatypePassword"]?.toString()
                ?: System.getenv("SONATYPE_PASSWORD")

            username.set(sonatypeUser)
            password.set(sonatypePassword)
        }
    }
}
