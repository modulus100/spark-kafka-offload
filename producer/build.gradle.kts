plugins {
    id("buildlogic.java-common-conventions")
    alias(libs.plugins.spring.boot)
}

repositories {
    mavenCentral()
    maven {
        url = uri("https://packages.confluent.io/maven/")
    }
}

dependencies {
    implementation(libs.springBootStarter)
    implementation(libs.springBootStarterKafka)

    implementation(libs.confluentKafkaProtobufSerializer)
    implementation(libs.protobufJava)
}

sourceSets {
    main {
        java {
            srcDir("../generated/java")
        }
    }
}
