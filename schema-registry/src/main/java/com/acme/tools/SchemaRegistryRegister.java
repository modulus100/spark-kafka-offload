package com.acme.tools;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.dataformat.yaml.YAMLFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Stream;

@JsonIgnoreProperties(ignoreUnknown = true)
record RegistrarConfig(String schemaRegistryUrl, Integer clientCacheCapacity, List<SchemaEntry> entries) {
}

@JsonIgnoreProperties(ignoreUnknown = true)
record SchemaEntry(String topic, String messageClass, String subject, String subjectStrategy, String compatibility) {
}

record Options(boolean dryRun, boolean failOnIncompatible, boolean verbose) {}

record RegistrationResult(int registered, int alreadyRegistered, int dryRun, int incompatible) {}

record RegistrationContext(String subject, ProtobufSchema schema, String messageClass) {}

public final class SchemaRegistryRegister {

    private static final Logger log = LoggerFactory.getLogger(SchemaRegistryRegister.class);

    public SchemaRegistryRegister() {}

    public static void main(String[] args) throws Exception {
        CliArgs cli = CliArgs.parse(args);

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

        SchemaRegistryRegister registrar = new SchemaRegistryRegister();

        List<RegistrarConfig> configs = registrar.loadConfigs(mapper, cli);
        if (configs.isEmpty()) {
            throw new IllegalArgumentException("No config files found");
        }

        List<SchemaEntry> entries = registrar.collectEntries(configs);
        Integer cacheCapacity = null;
        for (RegistrarConfig cfg : configs) {
            if (cacheCapacity == null) {
                cacheCapacity = cfg.clientCacheCapacity();
            }
        }
        if (entries.isEmpty()) {
            throw new IllegalArgumentException("No entries found in config(s)");
        }

        String schemaRegistryUrl = cli.schemaRegistryUrlOverride
                .or(() -> configs.stream().map(RegistrarConfig::schemaRegistryUrl).filter(Objects::nonNull).filter(s -> !s.isBlank()).findFirst())
                .orElseThrow(() -> new IllegalArgumentException("Missing schemaRegistryUrl (set it in YAML or pass --schema-registry-url)"));

        int clientCacheCapacity = cacheCapacity == null ? 1000 : cacheCapacity;

        CachedSchemaRegistryClient client = new CachedSchemaRegistryClient(
                schemaRegistryUrl,
                clientCacheCapacity,
                List.of(new ProtobufSchemaProvider()),
                Collections.emptyMap()
        );

        Options options = new Options(cli.dryRun, cli.failOnIncompatible, cli.verbose);
        RegistrationResult result = registrar.registerAll(client, schemaRegistryUrl, entries, options);

        if (result.incompatible() > 0 && cli.failOnIncompatible) {
            System.err.println("Incompatible schemas: " + result.incompatible());
            System.exit(2);
        }
    }

    RegistrationResult registerAll(
            SchemaRegistryClient client,
            String schemaRegistryUrl,
            List<SchemaEntry> entries,
            Options options
    ) throws Exception {
        Objects.requireNonNull(client, "client");
        Objects.requireNonNull(schemaRegistryUrl, "schemaRegistryUrl");
        Objects.requireNonNull(entries, "entries");
        Objects.requireNonNull(options, "options");

        int incompatible = 0;
        int registered = 0;
        int alreadyRegistered = 0;
        int dryRun = 0;
        KafkaProtobufSerializer<Message> serializer = createSerializer(client, schemaRegistryUrl);

        for (SchemaEntry entry : entries) {
            RegistrationContext ctx = buildContext(entry);

            String compatibility = normalizeCompatibility(entry.compatibility());

            if (options.dryRun()) {
                dryRun++;
                logDryRun(ctx, options);
                continue;
            }

            updateCompatibility(client, ctx.subject(), compatibility);

            Integer existingId = findExistingSchemaId(client, ctx.subject(), ctx.schema());
            if (existingId != null) {
                alreadyRegistered++;
                logSkipAlreadyRegistered(ctx, existingId);
                updateCompatibilityForReferences(client, ctx.subject(), compatibility);
                continue;
            }

            boolean hasAnyVersion = subjectExists(client, ctx.subject());
            if (hasAnyVersion) {
                boolean compatible = client.testCompatibility(ctx.subject(), ctx.schema());
                if (!compatible) {
                    incompatible++;
                    log.warn("[INCOMPATIBLE] subject={} messageClass={}", ctx.subject(), entry.messageClass());
                    if (options.failOnIncompatible()) {
                        continue;
                    }
                }
            }

            int newId = registerSchema(serializer, entry);
            registered++;
            logRegistered(ctx, newId);

            updateCompatibilityForReferences(client, ctx.subject(), compatibility);
        }

        return new RegistrationResult(registered, alreadyRegistered, dryRun, incompatible);
    }

    private KafkaProtobufSerializer<Message> createSerializer(SchemaRegistryClient client, String schemaRegistryUrl) {
        KafkaProtobufSerializer<Message> serializer = new KafkaProtobufSerializer<>(client);

        Map<String, Object> cfg = new HashMap<>();
        cfg.put("schema.registry.url", schemaRegistryUrl);
        cfg.put("auto.register.schemas", true);
        cfg.put("use.latest.version", false);
        cfg.put("value.subject.name.strategy", TopicRecordNameStrategy.class.getName());

        serializer.configure(cfg, false);
        return serializer;
    }

    private RegistrationContext buildContext(SchemaEntry entry) throws Exception {
        Message msg = defaultMessageInstance(entry.messageClass());
        Descriptors.Descriptor descriptor = msg.getDescriptorForType();
        ProtobufSchema schema = new ProtobufSchema(descriptor.getFile());
        String subject = resolveSubject(entry, descriptor);
        return new RegistrationContext(subject, schema, entry.messageClass());
    }

    private String resolveSubject(SchemaEntry entry, Descriptors.Descriptor descriptor) {
        String subject = entry.subject();
        if (subject == null || subject.isBlank()) {
            subject = defaultSubject(entry.topic(), entry.subjectStrategy(), descriptor.getFullName());
        } else {
            throw new IllegalArgumentException("Custom subject is not supported. Configure topic/record-name via topic-record-name strategy instead.");
        }
        return subject;
    }

    private void logDryRun(RegistrationContext ctx, Options options) {
        if (options.verbose()) {
            log.info("[DRY-RUN] would register subject={} messageClass={}", ctx.subject(), ctx.messageClass());
        }
    }

    private String normalizeCompatibility(String compatibility) {
        if (compatibility == null || compatibility.isBlank()) {
            compatibility = "BACKWARD";
        }
        return compatibility.trim().toUpperCase();
    }

    private void updateCompatibility(SchemaRegistryClient client, String subject, String compatibility)
            throws IOException, RestClientException {
        client.updateCompatibility(subject, compatibility);
    }

    private void updateCompatibilityForReferences(SchemaRegistryClient client, String subject, String compatibility)
            throws IOException, RestClientException {
        SchemaMetadata latest = client.getLatestSchemaMetadata(subject);
        if (latest == null || latest.getReferences() == null || latest.getReferences().isEmpty()) {
            return;
        }

        Set<String> updated = new HashSet<>();
        for (var ref : latest.getReferences()) {
            if (ref == null || ref.getSubject() == null || ref.getSubject().isBlank()) {
                continue;
            }
            if (updated.add(ref.getSubject())) {
                client.updateCompatibility(ref.getSubject(), compatibility);
            }
        }
    }

    private Integer findExistingSchemaId(SchemaRegistryClient client, String subject, ProtobufSchema schema)
            throws IOException, RestClientException {
        try {
            return client.getId(subject, schema);
        } catch (RestClientException e) {
            // Confluent error codes:
            // 40401 = Subject not found
            // 40403 = Schema not found
            // 42201 = Invalid schema (e.g. unresolved imported types when references are not supplied during lookup)
            if (e.getErrorCode() != 40401 && e.getErrorCode() != 40403 && e.getErrorCode() != 42201) {
                throw e;
            }
            return null;
        }
    }

    private int registerSchema(KafkaProtobufSerializer<Message> serializer, SchemaEntry entry) throws Exception {
        Message msg = defaultMessageInstance(entry.messageClass());
        byte[] payload = serializer.serialize(entry.topic(), msg);
        return extractSchemaId(payload);
    }

    private Message defaultMessageInstance(String messageClass)
            throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Objects.requireNonNull(messageClass, "messageClass");

        Class<?> clazz = Class.forName(messageClass);
        if (!Message.class.isAssignableFrom(clazz)) {
            throw new IllegalArgumentException("Configured messageClass is not a protobuf Message: " + messageClass);
        }

        Method getDefaultInstanceMethod = clazz.getMethod("getDefaultInstance");
        Object instance = getDefaultInstanceMethod.invoke(null);
        if (!(instance instanceof Message)) {
            throw new IllegalStateException("getDefaultInstance() did not return a protobuf Message for " + messageClass);
        }
        return (Message) instance;
    }

    private int extractSchemaId(byte[] payload) {
        if (payload == null || payload.length < 5) {
            throw new IllegalStateException("Serializer payload is too short to contain a schema id");
        }
        // Confluent framing: magic byte (0) + 4-byte schema id (big-endian)
        return ByteBuffer.wrap(payload, 1, 4).getInt();
    }

    private void logSkipAlreadyRegistered(RegistrationContext ctx, Integer existingId) {
        log.info("[SKIP] subject={} already has schema id={}", ctx.subject(), existingId);
    }

    private void logRegistered(RegistrationContext ctx, int newId) {
        log.info("[REGISTERED] subject={} id={} messageClass={}", ctx.subject(), newId, ctx.messageClass());
    }

    List<SchemaEntry> collectEntries(List<RegistrarConfig> configs) {
        Objects.requireNonNull(configs, "configs");

        List<SchemaEntry> entries = new ArrayList<>();
        for (RegistrarConfig cfg : configs) {
            if (cfg == null || cfg.entries() == null) {
                continue;
            }

            entries.addAll(cfg.entries());
        }
        return entries;
    }

    List<RegistrarConfig> loadConfigs(ObjectMapper mapper, CliArgs cli) throws IOException {
        Objects.requireNonNull(mapper, "mapper");
        Objects.requireNonNull(cli, "cli");

        Set<Path> orderedUniquePaths = new LinkedHashSet<>();

        for (String p : cli.configPaths) {
            if (p == null || p.isBlank()) {
                continue;
            }
            orderedUniquePaths.add(Path.of(p).normalize().toAbsolutePath());
        }

        for (String d : cli.configDirs) {
            if (d == null || d.isBlank()) {
                continue;
            }
            Path dir = Path.of(d);
            if (!Files.exists(dir)) {
                throw new IllegalArgumentException("Config dir does not exist: " + d);
            }

            try (Stream<Path> paths = Files.walk(dir)) {
                List<Path> matches = paths
                        .filter(Files::isRegularFile)
                        .filter(p -> p.getFileName().toString().equals("schema-registry.yml"))
                        .sorted()
                        .toList();
                for (Path p : matches) {
                    orderedUniquePaths.add(p.normalize().toAbsolutePath());
                }
            }
        }

        List<RegistrarConfig> configs = new ArrayList<>();
        for (Path p : orderedUniquePaths) {
            configs.add(mapper.readValue(p.toFile(), RegistrarConfig.class));
        }
        return configs;
    }

    private boolean subjectExists(SchemaRegistryClient client, String subject)
            throws IOException, RestClientException {
        try {
            SchemaMetadata latest = client.getLatestSchemaMetadata(subject);
            return latest != null;
        } catch (RestClientException e) {
            // 40401 = Subject not found
            if (e.getErrorCode() == 40401) {
                return false;
            }
            throw e;
        }
    }

    private String defaultSubject(String topic, String subjectStrategy, String recordFullName) {
        Objects.requireNonNull(topic, "topic");
        if (subjectStrategy == null || subjectStrategy.isBlank()) {
            subjectStrategy = "topic-record-name";
        }
        return switch (subjectStrategy) {
            case "topic-record-name" -> {
                if (recordFullName == null || recordFullName.isBlank()) {
                    throw new IllegalArgumentException("recordFullName is required for subjectStrategy topic-record-name");
                }
                yield topic + "-" + recordFullName;
            }
            default -> throw new IllegalArgumentException("Unsupported subjectStrategy: " + subjectStrategy + ". Only topic-record-name is supported.");
        };
    }

    private Descriptors.Descriptor descriptorFromMessageClass(String messageClass)
            throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Objects.requireNonNull(messageClass, "messageClass");

        Class<?> clazz = Class.forName(messageClass);
        if (!Message.class.isAssignableFrom(clazz)) {
            throw new IllegalArgumentException("Configured messageClass is not a protobuf Message: " + messageClass);
        }

        Method getDescriptorMethod = clazz.getMethod("getDescriptor");
        Object desc = getDescriptorMethod.invoke(null);
        if (!(desc instanceof Descriptors.Descriptor)) {
            throw new IllegalStateException("getDescriptor() did not return a Descriptor for " + messageClass);
        }

        return (Descriptors.Descriptor) desc;
    }

}
