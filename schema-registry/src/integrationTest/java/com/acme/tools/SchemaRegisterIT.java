package com.acme.tools;

import com.acme.demo.v1.DemoEvent;
import com.acme.test.v1.Root;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
@Tag("integration")
class SchemaRegisterIT {

    @BeforeAll
    static void requireDocker() {
        if (!DockerClientFactory.instance().isDockerAvailable()) {
            throw new IllegalStateException("Docker is not available to Testcontainers. "
                    + "Verify Docker Desktop is running and `docker ps` works from your terminal.");
        }
    }

    private static final Network network = Network.newNetwork();

    @SuppressWarnings("resource")
    @Container
    private static final GenericContainer<?> kafka = new GenericContainer<>(DockerImageName.parse("confluentinc/cp-kafka:8.0.3"))
            .withNetwork(network)
            .withNetworkAliases("kafka")
            // Standard single-node KRaft configuration (copied from Confluent docs and common Testcontainers examples)
            .withEnv("KAFKA_NODE_ID", "1")
            .withEnv("KAFKA_PROCESS_ROLES", "broker,controller")
            .withEnv("KAFKA_LISTENERS", "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093")
            .withEnv("KAFKA_ADVERTISED_LISTENERS", "PLAINTEXT://kafka:9092")
            .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT")
            .withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT")
            .withEnv("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
            .withEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@kafka:9093")
            // Single-node settings to avoid replication issues
            .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
            .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
            .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
            // Fixed cluster ID for reproducibility (optional but helpful)
            .withEnv("CLUSTER_ID", "4L6g3nShT-eMCtK--X86sw")
            // Reliable wait for KRaft broker readiness
            .waitingFor(Wait.forLogMessage(".*\\[SocketServer listenerType=BROKER, nodeId=1\\] Enabling request processing.*", 1)
                    .withStartupTimeout(Duration.ofMinutes(2)));

    @SuppressWarnings("resource")
    @Container
    private static final GenericContainer<?> schemaRegistry = new GenericContainer<>("confluentinc/cp-schema-registry:8.0.3")
            .withNetwork(network)
            .withNetworkAliases("schema-registry")
            .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
            .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:9092")
            .withExposedPorts(8081)
            .dependsOn(kafka)
            .waitingFor(Wait.forHttp("/subjects").forStatusCode(200).withStartupTimeout(Duration.ofMinutes(3)));

    @Test
    void happyPath_registersSchemaInRealSchemaRegistry_andDetectsIncompatibility() throws Exception {
        String url = "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getMappedPort(8081);

        CachedSchemaRegistryClient client = new CachedSchemaRegistryClient(
                url,
                1000,
                java.util.List.of(new ProtobufSchemaProvider()),
                java.util.Map.of()
        );

        SchemaEntry entry = new SchemaEntry(
                "demo.protobuf",
                DemoEvent.class.getName(),
                null,
                null,
                null
        );

        Options options = new Options(false, true, false);
        SchemaRegister registrar = new SchemaRegister(client, url, options);
        RegistrationResult result = registrar.registerAll(List.of(entry));

        assertEquals(1, result.registered());
        String subject = "demo.protobuf-" + DemoEvent.getDescriptor().getFullName();
        assertTrue(client.getAllSubjects().contains(subject));

        String incompatibleProto = readResource("/proto/acme/demo/v1/demo_event_incompatible.proto");
        ProtobufSchema incompatible = new ProtobufSchema(incompatibleProto);

        boolean compatible = client.testCompatibility(subject, incompatible);
        assertFalse(compatible);
    }

    @Test
    void registersSchemaWithAtLeastTwoReferences_viaSerializer() throws Exception {
        String url = "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getMappedPort(8081);

        CachedSchemaRegistryClient client = new CachedSchemaRegistryClient(
                url,
                1000,
                List.of(new ProtobufSchemaProvider()),
                Map.of()
        );

        DescriptorProtos.FileDescriptorProto baseProto = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("acme/test/v1/base.proto")
                .setPackage("acme.test.v1")
                .setSyntax("proto3")
                .addMessageType(DescriptorProtos.DescriptorProto.newBuilder()
                        .setName("Base")
                        .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                                .setName("id")
                                .setNumber(1)
                                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
                                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .build())
                        .build())
                .build();

        DescriptorProtos.FileDescriptorProto commonProto = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("acme/test/v1/common.proto")
                .setPackage("acme.test.v1")
                .setSyntax("proto3")
                .addMessageType(DescriptorProtos.DescriptorProto.newBuilder()
                        .setName("Common")
                        .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                                .setName("ts")
                                .setNumber(1)
                                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64)
                                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .build())
                        .build())
                .build();

        DescriptorProtos.FileDescriptorProto rootProto = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("acme/test/v1/root.proto")
                .setPackage("acme.test.v1")
                .setSyntax("proto3")
                .addDependency("acme/test/v1/base.proto")
                .addDependency("acme/test/v1/common.proto")
                .addMessageType(DescriptorProtos.DescriptorProto.newBuilder()
                        .setName("Root")
                        .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                                .setName("base")
                                .setNumber(1)
                                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                                .setTypeName("acme.test.v1.Base")
                                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .build())
                        .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                                .setName("common")
                                .setNumber(2)
                                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                                .setTypeName("acme.test.v1.Common")
                                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .build())
                        .build())
                .build();

        Descriptors.FileDescriptor baseFd = Descriptors.FileDescriptor.buildFrom(baseProto, new Descriptors.FileDescriptor[0]);
        Descriptors.FileDescriptor commonFd = Descriptors.FileDescriptor.buildFrom(commonProto, new Descriptors.FileDescriptor[0]);
        Descriptors.FileDescriptor rootFd = Descriptors.FileDescriptor.buildFrom(rootProto, new Descriptors.FileDescriptor[]{baseFd, commonFd});

        Descriptors.Descriptor rootDescriptor = rootFd.findMessageTypeByName("Root");
        assertNotNull(rootDescriptor);

        DynamicMessage msg = DynamicMessage.newBuilder(rootDescriptor).build();

        KafkaProtobufSerializer<DynamicMessage> serializer = new KafkaProtobufSerializer<>(client);
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("schema.registry.url", url);
        cfg.put("auto.register.schemas", true);
        cfg.put("use.latest.version", false);
        cfg.put("value.subject.name.strategy", TopicRecordNameStrategy.class.getName());
        serializer.configure(cfg, false);

        String topic = "demo.protobuf.refs";
        byte[] payload = serializer.serialize(topic, msg);
        int idFromPayload = ByteBuffer.wrap(payload, 1, 4).getInt();

        String subject = topic + "-" + rootDescriptor.getFullName();
        assertTrue(client.getAllSubjects().contains(subject));

        var metadata = client.getLatestSchemaMetadata(subject);
        assertNotNull(metadata);
        assertEquals(idFromPayload, metadata.getId());

        assertNotNull(client.getSchemaById(idFromPayload));
        assertNotNull(metadata.getReferences());
        assertTrue(metadata.getReferences().size() >= 2, "Expected at least 2 references but got " + metadata.getReferences().size());

        for (var ref : metadata.getReferences()) {
            assertTrue(client.getAllSubjects().contains(ref.getSubject()), "Missing reference subject " + ref.getSubject());
            assertTrue(ref.getVersion() > 0);
            assertNotNull(client.getSchemaMetadata(ref.getSubject(), ref.getVersion()));
        }
    }

    @Test
    void register_registersSchemaWithAtLeastTwoReferences_andCreatesExpectedSubjectsAndIds() throws Exception {
        String url = "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getMappedPort(8081);

        CachedSchemaRegistryClient client = new CachedSchemaRegistryClient(
                url,
                1000,
                List.of(new ProtobufSchemaProvider()),
                Map.of()
        );

        SchemaEntry entry = new SchemaEntry(
                "demo.protobuf.registrar.refs",
                Root.class.getName(),
                null,
                null,
                null
        );

        Options options = new Options(false, true, false);
        SchemaRegister registrar = new SchemaRegister(client, url, options);
        RegistrationResult result = registrar.registerAll(List.of(entry));

        assertEquals(1, result.registered());

        String subject = entry.topic() + "-" + Root.getDescriptor().getFullName();
        assertTrue(client.getAllSubjects().contains(subject));

        var metadata = client.getLatestSchemaMetadata(subject);
        assertNotNull(metadata);
        assertTrue(metadata.getId() > 0);
        assertNotNull(client.getSchemaById(metadata.getId()));

        assertNotNull(metadata.getReferences());
        assertTrue(metadata.getReferences().size() >= 2, "Expected at least 2 references but got " + metadata.getReferences().size());

        for (var ref : metadata.getReferences()) {
            assertTrue(client.getAllSubjects().contains(ref.getSubject()), "Missing reference subject " + ref.getSubject());
            assertTrue(ref.getVersion() > 0);
            assertNotNull(client.getSchemaMetadata(ref.getSubject(), ref.getVersion()));
        }
    }

    @Test
    void register_setsCompatibilityForReferences_andRejectsIncompatibleReferenceUpdate() throws Exception {
        String url = "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getMappedPort(8081);

        CachedSchemaRegistryClient client = new CachedSchemaRegistryClient(
                url,
                1000,
                List.of(new ProtobufSchemaProvider()),
                Map.of()
        );

        SchemaEntry entry = new SchemaEntry(
                "demo.protobuf.registrar.refs.compat",
                Root.class.getName(),
                null,
                null,
                null
        );

        Options options = new Options(false, true, false);
        SchemaRegister registrar = new SchemaRegister(client, url, options);
        registrar.registerAll(List.of(entry));

        String rootSubject = entry.topic() + "-" + Root.getDescriptor().getFullName();
        var rootMeta = client.getLatestSchemaMetadata(rootSubject);
        assertNotNull(rootMeta);
        assertNotNull(rootMeta.getReferences());
        assertFalse(rootMeta.getReferences().isEmpty(), "Expected references for root schema");

        String baseSubject = rootMeta.getReferences().stream()
                .filter(r -> r != null && ("acme/test/v1/base.proto".equals(r.getName()) || "acme/test/v1/base.proto".equals(r.getSubject())))
                .map(SchemaReference::getSubject)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Could not find Base reference subject in: " + rootMeta.getReferences()));

        DescriptorProtos.FileDescriptorProto baseV2Proto = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("acme/test/v1/base.proto")
                .setPackage("acme.test.v1")
                .setSyntax("proto3")
                .addMessageType(DescriptorProtos.DescriptorProto.newBuilder()
                        .setName("Base")
                        .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                                .setName("id")
                                .setNumber(1)
                                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32)
                                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .build())
                        .build())
                .build();

        Descriptors.FileDescriptor baseV2Fd = Descriptors.FileDescriptor.buildFrom(baseV2Proto, new Descriptors.FileDescriptor[0]);
        ProtobufSchema incompatibleBaseSchema = new ProtobufSchema(baseV2Fd);

        assertThrows(RestClientException.class, () -> client.register(baseSubject, incompatibleBaseSchema));
    }

    private static String readResource(String path) throws Exception {
        try (InputStream in = SchemaRegisterIT.class.getResourceAsStream(path)) {
            if (in == null) {
                throw new IllegalArgumentException("Missing test resource: " + path);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}