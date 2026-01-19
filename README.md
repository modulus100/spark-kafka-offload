# spark-kafka-offload

## Protobuf

### Generate descriptor set

The Spark consumer uses a Protobuf descriptor set (a `FileDescriptorSet`) for `from_protobuf` decoding.

From the repo root:

```bash
mkdir -p proto/descriptors
buf build --as-file-descriptor-set -o proto/descriptors/demo.desc proto
```

## Schema Registry

Register protobuf schemas in Confluent Schema Registry:

```bash
./gradlew :schema-registry:registerSchemas --args='--config proto/acme/schema-registry.yml --verbose'
```

Dry run:

```bash
./gradlew :schema-registry:registerSchemas --args='--config proto/acme/schema-registry.yml --dry-run --verbose'
```

Register all domains under `proto/` (auto-discovers `schema-registry.yml` files):

```bash
./gradlew :schema-registry:registerSchemas --args='--config-dir proto --schema-registry-url http://localhost:8081 --verbose'
```
