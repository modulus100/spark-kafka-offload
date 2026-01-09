package com.acme.tools;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public final class DescriptorUploader {
    public static void main(String[] args) throws Exception {
        String endpoint = env("S3_ENDPOINT", "http://localhost:9000");
        String region = env("AWS_REGION", "us-east-1");

        String accessKey = env("AWS_ACCESS_KEY_ID", env("S3_ACCESS_KEY", "admin"));
        String secretKey = env("AWS_SECRET_ACCESS_KEY", env("S3_SECRET_KEY", "password"));

        String bucket = env("DESCRIPTOR_BUCKET", env("S3_BUCKET", "proto-artifacts"));
        String key = env("DESCRIPTOR_KEY", env("S3_KEY", "descriptors/demo.desc"));
        String descriptorFile = env("DESCRIPTOR_FILE", "proto/descriptors/demo.desc");

        Path filePath = Paths.get(descriptorFile);
        if (!Files.exists(filePath)) {
            throw new IllegalStateException("Descriptor file does not exist: " + filePath.toAbsolutePath());
        }

        S3Client s3 = S3Client.builder()
                .endpointOverride(URI.create(endpoint))
                .region(Region.of(region))
                .credentialsProvider(
                        StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                .build();

        PutObjectRequest req = PutObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .contentType("application/octet-stream")
                .build();

        s3.putObject(req, RequestBody.fromFile(filePath));

        System.out.println("Uploaded descriptor to s3://" + bucket + "/" + key);
        System.out.println("(Spark should read it via s3a://" + bucket + "/" + key + ")");

        s3.close();
    }

    private static String env(String name, String defaultValue) {
        String v = System.getenv(name);
        return (v == null || v.isBlank()) ? defaultValue : v;
    }
}
