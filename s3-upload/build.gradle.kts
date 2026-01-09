plugins {
    id("buildlogic.java-common-conventions")
    application
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("software.amazon.awssdk:s3:2.25.66")
}

application {
    mainClass.set("com.acme.tools.DescriptorUploader")
}

tasks.register<JavaExec>("uploadDescriptor") {
    group = "publishing"
    description = "Uploads proto descriptor set to S3/MinIO"

    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set(application.mainClass)

    workingDir = rootDir
}
