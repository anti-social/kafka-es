import com.google.protobuf.gradle.protobuf
import com.google.protobuf.gradle.protoc

import java.nio.file.Paths

import org.gradle.jvm.tasks.Jar

import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    application
    java
    `maven-publish`
    jacoco
    id("org.jetbrains.kotlin.jvm") version "1.4.21"
    kotlin("plugin.serialization") version "1.4.21"
    id("com.google.protobuf") version "0.8.14"
    id("org.ajoberstar.grgit") version "4.1.0"
}

repositories {
    mavenCentral()
    jcenter()
    maven {
        url = uri("https://dl.bintray.com/evo/maven")
    }
}

group = "dev.evo"

val gitDescribe = grgit.describe(mapOf("tags" to true, "match" to listOf("v*")))
        ?: "v0.0.0-unknown"
version = gitDescribe.trimStart('v')

val kotlinCoroutinesVersion = "1.4.2"
val kotlinSerializationVersion = "1.0.1"
val kafkaVersion = "2.7.0"
val protobufVersion = "3.14.0"
val junitJupiterVersion = "5.2.0"
val assertjVersion = "3.8.0"
val kotestVersion = "4.4.0.RC2"
val esTransportVersion = "0.0.8"
val prometheusKtVersion = "0.1.0-rc-3"
val ktorVersion = "1.5.1"
val argparserVersion = "2.0.7"

dependencies {
    implementation(kotlin("stdlib-jdk8"))
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:$kotlinCoroutinesVersion")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:$kotlinSerializationVersion")
    implementation("org.apache.kafka:connect-api:$kafkaVersion")
    implementation("org.apache.kafka:connect-runtime:$kafkaVersion")
    implementation("com.google.protobuf:protobuf-java:$protobufVersion")
    implementation("com.google.protobuf:protobuf-java-util:$protobufVersion")

    implementation("dev.evo:elasticart-elasticsearch-transport:$esTransportVersion")
    implementation("dev.evo:prometheus-kt-ktor:$prometheusKtVersion")
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
    archiveClassifier.set("source")
    from(sourceSets["main"].allSource)
}

publishing {
    publications {
        create<MavenPublication>("jar") {
            groupId = project.group.toString()
            artifactId = project.name
            version = project.version.toString()
            from(components["java"])
            artifact(sourceJar)

            pom {
                licenses {
                    license {
                        name.set("The Apache License, Version 2.0")
                        url.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
                    }
                }
                developers {
                    developer {
                        name.set("Alexander Koval")
                        email.set("kovalidis@gmail.com")
                    }
                }
                scm {
                    connection.set("scm:git:git://github.com/anti-social/kafka-es.git")
                    url.set("https://github.com/anti-social/kafka-es")
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
            val bintrayPackageName = project.name
            val bintrayRepoName = findProperty("bintrayRepoName")?.toString()
                    ?: System.getenv("BINTRAY_REPO_NAME")
            val bintrayUsername = findProperty("bintrayUser")?.toString()
                    ?: System.getenv("BINTRAY_USER")
            val bintrayApiKey = findProperty("bintrayApiKey")?.toString()
                    ?: System.getenv("BINTRAY_API_KEY")
            val bintrayPublish = findProperty("bintrayPublish")?.toString()
                    ?: System.getenv("BINTRAY_PUBLISH")
                    ?: "0"

            name = "bintray"
            url = uri("https://api.bintray.com/maven/$bintrayUsername/$bintrayRepoName/$bintrayPackageName/;publish=$bintrayPublish")
            credentials {
                username = bintrayUsername
                password = bintrayApiKey
            }
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
