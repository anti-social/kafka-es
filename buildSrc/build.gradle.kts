import org.gradle.kotlin.dsl.`kotlin-dsl`

repositories {
    mavenCentral()
    maven("https://plugins.gradle.org/m2/")
}

plugins {
    idea
    `kotlin-dsl`
}

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
}

idea {
    module {
        isDownloadJavadoc = false
        isDownloadSources = false
    }
}
