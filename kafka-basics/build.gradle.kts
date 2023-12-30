plugins {
    id("java")
}

group = "io.conduktor.demos"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.slf4j:slf4j-simple:2.0.9")
    implementation("org.slf4j:slf4j-api:2.0.9")
    implementation("org.apache.kafka:kafka-clients:3.1.0")
}

tasks.test {
    useJUnitPlatform()
}