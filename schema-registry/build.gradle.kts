plugins {
    id("buildlogic.java-common-conventions")
    id("buildlogic.integration-test-conventions")
    application
}

repositories {
    mavenCentral()
    maven {
        url = uri("https://packages.confluent.io/maven/")
    }
}

dependencies {
    implementation("io.confluent:kafka-schema-registry-client:8.1.0")
    implementation("io.confluent:kafka-protobuf-provider:8.1.0")
    implementation("io.confluent:kafka-protobuf-serializer:8.1.0")
    implementation("com.google.protobuf:protobuf-java:4.33.2")

    implementation("org.slf4j:slf4j-api:2.0.16")

    implementation("tools.jackson.core:jackson-databind:3.0.3")
    implementation("tools.jackson.dataformat:jackson-dataformat-yaml:3.0.3")

    testImplementation("org.testcontainers:testcontainers-junit-jupiter:2.0.3")
//    testImplementation("org.testcontainers:org.junit.jupiter:junit-jupiter:5.8.1")
    testImplementation("org.testcontainers:testcontainers:2.0.3")
    testImplementation("org.testcontainers:testcontainers-kafka:2.0.3")

    runtimeOnly("org.slf4j:slf4j-simple:2.0.16")
}

sourceSets {
    main {
        java {
            srcDir("../generated/java")
        }
    }
}

application {
    mainClass.set("com.acme.tools.SchemaRegistryRegister")
}

tasks.register<JavaExec>("registerSchemas") {
    group = "publishing"
    description = "Registers protobuf schemas in Confluent Schema Registry"

    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set(application.mainClass)

    workingDir = rootDir
}
