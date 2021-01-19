import com.google.protobuf.gradle.protobuf
import com.google.protobuf.gradle.protoc
import com.jfrog.bintray.gradle.BintrayExtension
import java.nio.file.Paths
import java.util.Date
import org.gradle.jvm.tasks.Jar
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    application
    java
    `maven-publish`
    id("org.jetbrains.kotlin.jvm") version "1.4.21"
    id("com.google.protobuf") version "0.8.14"
    id("com.jfrog.bintray") version "1.8.2"
    id("org.ajoberstar.grgit") version "2.2.1"
}

repositories {
     mavenCentral()
}

group = "company.evo"

val grgit: org.ajoberstar.grgit.Grgit by extra
val gitDescribe = grgit.describe(mapOf("match" to listOf("v*")))
        ?: "v0.0.0-unknown"
version = gitDescribe.trimStart('v')

val kafkaVersion = "2.7.0"
val jestVersion = "6.3.1"
val protobufVersion = "3.14.0"
val junitJupiterVersion = "5.2.0"
val assertjVersion = "3.8.0"

dependencies {
    implementation(kotlin("stdlib-jdk8"))
    implementation("org.apache.kafka:connect-api:$kafkaVersion")
    implementation("org.apache.kafka:connect-json:$kafkaVersion")
    implementation("org.apache.kafka:connect-runtime:$kafkaVersion")
    implementation("io.searchbox:jest:$jestVersion")
    implementation("io.searchbox:jest-common:$jestVersion")
    implementation("com.google.protobuf:protobuf-java:$protobufVersion")
    implementation("com.google.protobuf:protobuf-java-util:$protobufVersion")

    testImplementation("org.junit.jupiter:junit-jupiter-api:$junitJupiterVersion")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:$junitJupiterVersion")
    testImplementation("org.assertj:assertj-core:$assertjVersion")
}

application {
    mainClass.set("org.apache.kafka.connect.cli.ConnectStandalone")
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
    val run by getting(JavaExec::class) {
        System.getProperty("exec.args")?.let {
            args = it.trim().split("\\s+".toRegex())
        }
    }
    val test by getting(Test::class) {
        useJUnitPlatform()
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
