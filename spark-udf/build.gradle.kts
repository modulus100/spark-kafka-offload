import org.gradle.api.tasks.scala.ScalaCompile
import org.gradle.api.tasks.testing.Test

plugins {
    id("buildlogic.java-library-conventions")
    scala
}

repositories {
    mavenCentral()
    maven {
        url = uri("https://packages.confluent.io/maven/")
    }
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }

    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

tasks.withType<JavaCompile>().configureEach {
    options.release.set(17)
}

tasks.withType<ScalaCompile>().configureEach {
    scalaCompileOptions.additionalParameters = listOf("-target:jvm-1.8")
}

tasks.withType<Test>().configureEach {
    jvmArgs(
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED"
    )
}

sourceSets {
    test {
        java {
            srcDir("../generated/java")
        }
    }
}

dependencies {
    compileOnly("org.apache.spark:spark-sql_2.12:3.5.2")

    testImplementation("org.scala-lang:scala-library:2.12.18")
    testImplementation("org.apache.spark:spark-sql_2.12:3.5.2")
    testImplementation("io.confluent:kafka-protobuf-serializer:8.1.0")
    testImplementation("io.confluent:kafka-schema-registry-client:8.1.0")
    testImplementation("com.google.protobuf:protobuf-java:4.33.2")
}
