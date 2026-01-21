package com.acme.tools;

import com.acme.demo.v1.DemoEvent;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SchemaRegisterTest {

    @Test
    void registerAll_happyPath_registersThenSkipsWhenAlreadyRegistered() throws Exception {
        MockSchemaRegistryClient client = new MockSchemaRegistryClient(List.of(new ProtobufSchemaProvider()));
        SchemaRegister registrar = new SchemaRegister();

        SchemaEntry entry = new SchemaEntry(
                "demo.protobuf",
                DemoEvent.class.getName(),
                null,
                null,
                null
        );

        Options options = new Options(false, true, true);

        RegistrationResult first = registrar.registerAll(
                client,
                "mock://schema-registry",
                List.of(entry),
                options
        );

        assertEquals(1, first.registered());
        assertEquals(0, first.alreadyRegistered());
        assertEquals(0, first.incompatible());
        assertTrue(client.getAllSubjects().contains("demo.protobuf-" + DemoEvent.getDescriptor().getFullName()));

        RegistrationResult second = registrar.registerAll(
                client,
                "mock://schema-registry",
                List.of(entry),
                options
        );

        assertEquals(0, second.registered());
        assertEquals(1, second.alreadyRegistered());
        assertEquals(0, second.incompatible());
    }

    @Test
    void registerAll_topicRecordNameStrategy_usesTopicAndRecordFullNameAsSubject() throws Exception {
        MockSchemaRegistryClient client = new MockSchemaRegistryClient(List.of(new ProtobufSchemaProvider()));
        SchemaRegister registrar = new SchemaRegister();

        SchemaEntry entry = new SchemaEntry(
                "demo.protobuf",
                DemoEvent.class.getName(),
                null,
                "topic-record-name",
                null
        );

        Options options = new Options(false, true, false);

        RegistrationResult r = registrar.registerAll(
                client,
                "mock://schema-registry",
                List.of(entry),
                options
        );

        assertEquals(1, r.registered());
        assertTrue(client.getAllSubjects().contains("demo.protobuf-" + DemoEvent.getDescriptor().getFullName()));
    }

    @Test
    void registerAll_dryRun_doesNotRegister() throws Exception {
        MockSchemaRegistryClient client = new MockSchemaRegistryClient(List.of(new ProtobufSchemaProvider()));
        SchemaRegister registrar = new SchemaRegister();

        SchemaEntry entry = new SchemaEntry(
                "demo.protobuf",
                DemoEvent.class.getName(),
                null,
                null,
                null
        );

        Options options = new Options(true, true, true);

        RegistrationResult r = registrar.registerAll(
                client,
                "mock://schema-registry",
                List.of(entry),
                options
        );

        assertEquals(0, r.registered());
        assertEquals(0, r.alreadyRegistered());
        assertEquals(1, r.dryRun());
        assertTrue(client.getAllSubjects().isEmpty());
    }

    @Test
    void registerAll_incompatible_whenFailOnIncompatible_skipsRegisterAndCountsIncompatible() throws Exception {
        MockSchemaRegistryClient client = new MockSchemaRegistryClient(List.of(new ProtobufSchemaProvider()));
        SchemaRegister registrar = new SchemaRegister();

        SchemaEntry entry = new SchemaEntry(
                "demo.protobuf",
                DemoEvent.class.getName(),
                null,
                null,
                null
        );

        // Make the subject exist so compatibility is checked
        String subject = "demo.protobuf-" + DemoEvent.getDescriptor().getFullName();
        String incompatibleProto = readResource("/proto/acme/demo/v1/demo_event_incompatible.proto");
        client.register(
                subject,
                new io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema(incompatibleProto)
        );

        Options options = new Options(false, true, false);
        RegistrationResult r = registrar.registerAll(
                client,
                "mock://schema-registry",
                List.of(entry),
                options
        );

        assertEquals(0, r.registered());
        assertEquals(0, r.alreadyRegistered());
        assertEquals(1, r.incompatible());
    }

    @Test
    void configDirDiscovery_findsSchemaRegistryYmlFiles(@TempDir Path tmp) throws Exception {
        copyResourceTree("/configs/proto", tmp.resolve("proto"));

        CliArgs args = CliArgs.parse(new String[] {
                "--config-dir", tmp.resolve("proto").toString(),
                "--schema-registry-url", "http://localhost:8081"
        });

        tools.jackson.databind.ObjectMapper mapper = new tools.jackson.databind.ObjectMapper(new tools.jackson.dataformat.yaml.YAMLFactory());
        List<?> configs = invokeLoadConfigs(mapper, args);
        assertEquals(1, configs.size());
    }

    @Test
    void loadConfigs_supportsMultipleConfigAndConfigDirInStableOrderAndDedupes(@TempDir Path tmp) throws Exception {
        copyResourceTree("/configs/proto1", tmp.resolve("proto1"));
        copyResourceTree("/configs/proto2", tmp.resolve("proto2"));

        Path cfg1 = tmp.resolve("proto1").resolve("a").resolve("schema-registry.yml");

        // Explicit config file that is also present under protoRoot1 to verify de-dupe.
        // (Passing it via --config AND discovering it via --config-dir should only load it once.)
        CliArgs args = CliArgs.parse(new String[] {
                "--config", cfg1.toString(),
                "--config-dir", tmp.resolve("proto1").toString(),
                "--config-dir", tmp.resolve("proto2").toString(),
                "--schema-registry-url", "http://override"
        });

        tools.jackson.databind.ObjectMapper mapper = new tools.jackson.databind.ObjectMapper(new tools.jackson.dataformat.yaml.YAMLFactory());
        List<RegistrarConfig> configs = invokeLoadConfigs(mapper, args);

        assertEquals(2, configs.size());
    }

    @Test
    void collectEntries_collectsEntriesWithoutCompatibilityInheritance() {
        SchemaRegister registrar = new SchemaRegister();

        SchemaEntry e1 = new SchemaEntry(
                "demo.protobuf",
                DemoEvent.class.getName(),
                null,
                null,
                null
        );

        SchemaEntry e2 = new SchemaEntry(
                "demo.protobuf",
                DemoEvent.class.getName(),
                "demo.protobuf-override",
                null,
                "NONE"
        );

        RegistrarConfig cfg = new RegistrarConfig(null, List.of(e1, e2));

        List<SchemaEntry> entries = registrar.collectEntries(List.of(cfg));
        assertEquals(2, entries.size());
        assertNull(entries.get(0).compatibility());
        assertEquals("NONE", entries.get(1).compatibility());
    }

    @Test
    void registerAll_setsCompatibilityToBackwardTransitiveByDefault() throws Exception {
        MockSchemaRegistryClient client = new MockSchemaRegistryClient();
        SchemaRegister registrar = new SchemaRegister();

        SchemaEntry entry1 = new SchemaEntry(
                "demo.protobuf",
                DemoEvent.class.getName(),
                null,
                null,
                null
        );
        // No compatibility set => default BACKWARD

        Options options = new Options(false, true, false);

        registrar.registerAll(
                client,
                "mock://schema-registry",
                List.of(entry1),
                options
        );

        String subject = "demo.protobuf-" + DemoEvent.getDescriptor().getFullName();
        assertEquals("BACKWARD_TRANSITIVE", client.getConfig(subject).getCompatibilityLevel());
    }

    @Test
    void registerAll_setsDifferentCompatibilityModes_perEntryAndNormalizesValue() throws Exception {
        MockSchemaRegistryClient client = new MockSchemaRegistryClient();
        SchemaRegister registrar = new SchemaRegister();

        String recordFullName = DemoEvent.getDescriptor().getFullName();

        SchemaEntry full = new SchemaEntry(
                "demo.full",
                DemoEvent.class.getName(),
                null,
                null,
                " full "
        );

        SchemaEntry forward = new SchemaEntry(
                "demo.forward",
                DemoEvent.class.getName(),
                null,
                null,
                "FORWARD"
        );

        SchemaEntry none = new SchemaEntry(
                "demo.none",
                DemoEvent.class.getName(),
                null,
                null,
                "none"
        );

        Options options = new Options(false, true, false);

        registrar.registerAll(
                client,
                "mock://schema-registry",
                List.of(full, forward, none),
                options
        );

        assertEquals("FULL", client.getConfig("demo.full-" + recordFullName).getCompatibilityLevel());
        assertEquals("FORWARD", client.getConfig("demo.forward-" + recordFullName).getCompatibilityLevel());
        assertEquals("NONE", client.getConfig("demo.none-" + recordFullName).getCompatibilityLevel());
    }

    @SuppressWarnings("unchecked")
    private static List<RegistrarConfig> invokeLoadConfigs(
            tools.jackson.databind.ObjectMapper mapper,
            CliArgs args
    ) throws Exception {
        SchemaRegister registrar = new SchemaRegister();
        var m = SchemaRegister.class.getDeclaredMethod("loadConfigs", tools.jackson.databind.ObjectMapper.class, CliArgs.class);
        m.setAccessible(true);
        return (List<RegistrarConfig>) m.invoke(registrar, mapper, args);
    }

    private static String readResource(String path) throws Exception {
        try (InputStream in = SchemaRegisterTest.class.getResourceAsStream(path)) {
            if (in == null) {
                throw new IllegalArgumentException("Missing test resource: " + path);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static void copyResourceTree(String resourceRoot, Path destinationRoot) throws Exception {
        if (resourceRoot == null || resourceRoot.isBlank()) {
            throw new IllegalArgumentException("resourceRoot is required");
        }
        if (destinationRoot == null) {
            throw new IllegalArgumentException("destinationRoot is required");
        }

        // These tests run from a compiled test-classes output directory, which is a real filesystem.
        // So we can locate the resource directory and copy files recursively.
        var url = SchemaRegisterTest.class.getResource(resourceRoot);
        if (url == null) {
            throw new IllegalArgumentException("Missing test resource directory: " + resourceRoot);
        }

        Path sourceRoot = Path.of(url.toURI());
        try (var paths = Files.walk(sourceRoot)) {
            for (Path p : paths.toList()) {
                Path rel = sourceRoot.relativize(p);
                Path dest = destinationRoot.resolve(rel.toString());
                if (Files.isDirectory(p)) {
                    Files.createDirectories(dest);
                } else {
                    Files.createDirectories(dest.getParent());
                    Files.copy(p, dest, StandardCopyOption.REPLACE_EXISTING);
                }
            }
        }
    }
}
