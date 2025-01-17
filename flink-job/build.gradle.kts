plugins {
    id("java")
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
}

repositories {
    mavenCentral()
}

dependencies {

    val flinkVersion = "1.20.0"

    implementation("org.mongodb:mongodb-driver:3.6.3")

    implementation("org.apache.flink:flink-connector-base:$flinkVersion")

    implementation("org.apache.flink:flink-core:$flinkVersion")
    implementation("org.apache.flink:flink-streaming-java:$flinkVersion")
    implementation("org.apache.flink:flink-clients:$flinkVersion")
    implementation("org.apache.flink:flink-connector-kafka:3.3.0-1.20")


    implementation("com.fasterxml.jackson.core:jackson-databind:2.15.2")

    compileOnly("org.projectlombok:lombok:1.18.26")
    annotationProcessor("org.projectlombok:lombok:1.18.26")

    testImplementation("junit:junit:4.13.2")
    testCompileOnly("org.projectlombok:lombok:1.18.26")
    testAnnotationProcessor("org.projectlombok:lombok:1.18.26")
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
}

tasks.named<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar>("shadowJar") {
    archiveClassifier.set("")
}

tasks.build {
    dependsOn(tasks.shadowJar)
}
