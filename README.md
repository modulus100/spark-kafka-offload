# spark-kafka-offload

## Protobuf

### Generate descriptor set

The Spark consumer uses a Protobuf descriptor set (a `FileDescriptorSet`) for `from_protobuf` decoding.

From the repo root:

```bash
mkdir -p proto/descriptors
buf build --as-file-descriptor-set -o proto/descriptors/demo.desc proto
```
