import com.google.protobuf.gradle.protobuf
import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import org.gradle.api.tasks.testing.logging.TestLogEvent
import org.gradle.jvm.tasks.Jar
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import java.nio.file.Paths

plugins {
    java
    jacoco
    signing
    `maven-publish`
    kotlin("jvm") version Versions.kotlin
    kotlin("plugin.serialization") version Versions.kotlin
    id("io.github.gradle-nexus.publish-plugin") version "1.1.0"
    id("com.google.protobuf") version "0.9.3"
    id("org.ajoberstar.grgit") version "4.1.0"
}

group = "dev.evo.kafka-es"

val gitDescribe = grgit.describe(mapOf("tags" to true, "match" to listOf("v*")))
    ?: "v0.0.0-unknown"
version = gitDescribe.trimStart('v')

val javaVersion = Versions.java.toString()

allprojects {
    group = rootProject.group
    version = rootProject.version

    apply {
        plugin("signing")
        plugin("maven-publish")
        plugin("org.jetbrains.kotlin.jvm")
    }

    repositories {
        mavenCentral()
    }

    tasks.withType(JavaCompile::class.java) {
        sourceCompatibility = javaVersion
        targetCompatibility = javaVersion
    }

    signing {
        sign(publishing.publications)
    }

    val jar by tasks.getting(Jar::class)

    val sourceJar by tasks.creating(Jar::class) {
        archiveClassifier.set("sources")
        from(sourceSets["main"].allSource)
    }

    val javadocJar by tasks.registering(Jar::class) {
        archiveClassifier.set("javadoc")
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
                    name.set(project.name)
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
                url = uri("file://${rootProject.buildDir}/repos/testMaven")
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
}

dependencies {
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:${Versions.kotlinxCoroutines}")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-slf4j:${Versions.kotlinxCoroutines}")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:${Versions.kotlinxSerialization}")
    implementation("org.apache.kafka:connect-api:${Versions.kafka}")
    implementation("org.apache.kafka:connect-runtime:${Versions.kafka}")
    implementation("com.google.protobuf:protobuf-java:${Versions.protobuf}")
    implementation("com.google.protobuf:protobuf-java-util:${Versions.protobuf}")
    implementation("org.slf4j:slf4j-api:${Versions.slf4j}")
    implementation("org.slf4j:slf4j-ext:${Versions.slf4j}")

    implementation("dev.evo.elasticmagic:elasticmagic-transport-ktor:${Versions.elasticmagic}")
    implementation("dev.evo.elasticmagic:elasticmagic-serde-kotlinx-json:${Versions.elasticmagic}")
    implementation("io.ktor:ktor-client-cio:${Versions.ktor}")

    testImplementation("io.kotest:kotest-runner-junit5:${Versions.kotest}")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:${Versions.kotlinxCoroutines}")
}

tasks {
    val jacocoTestReport by getting(JacocoReport::class) {
        reports {
            html.required.set(true)
            xml.required.set(true)
        }
    }
}

tasks.withType(KotlinCompile::class.java) {
    kotlinOptions {
        jvmTarget = javaVersion
        freeCompilerArgs = listOf(
            "-opt-in=kotlin.time.ExperimentalTime",
            "-opt-in=kotlinx.coroutines.ExperimentalCoroutinesApi",
        )
    }
}

val compileKotlin by tasks.getting(KotlinCompile::class) {
    dependsOn("generateProto")
}
val compileTestKotlin by tasks.getting(KotlinCompile::class) {
    dependsOn("generateTestProto")
}

tasks.withType<Test>().configureEach {
    useJUnitPlatform()

    outputs.upToDateWhen { false }

    testLogging {
        events = mutableSetOf<TestLogEvent>().apply {
            add(TestLogEvent.FAILED)
            if (project.hasProperty("showPassedTests")) {
                add(TestLogEvent.PASSED)
            }
            if (project.hasProperty("showTestsOutput")) {
                add(TestLogEvent.STANDARD_OUT)
                add(TestLogEvent.STANDARD_ERROR)
            }
        }
        exceptionFormat = TestExceptionFormat.FULL
    }
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:${Versions.protobuf}"
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
