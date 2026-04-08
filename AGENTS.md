# AGENTS.md

This file provides guidance to AI agents when working with code in this repository.

## What is Khazana

Khazana is the MOSIP Object Store library. It provides pluggable adapter implementations for connecting to different object storage backends. It is consumed as a dependency (JAR) by other MOSIP modules (regclient, regproc, datashare, resident, idrepo).

## Build Commands

All Maven commands should be run from `kernel/` (the parent module):

```bash
# Build and run tests
cd kernel && mvn clean install

# Skip tests
cd kernel && mvn clean install -DskipTests

# Run tests for the object-store module only
cd kernel/object-store && mvn test

# Run a single test class
cd kernel/object-store && mvn test -Dtest=PosixAdapterTest

# Sonar analysis (requires SONAR_TOKEN)
cd kernel && mvn verify -Psonar
```

The build produces `kernel/object-store/target/khazana-<version>.jar`.

## Project Structure

```
kernel/
  pom.xml                  # Parent POM (khazana-parent, groupId: io.mosip.commons)
  object-store/
    pom.xml                # Module POM (artifactId: khazana)
    src/main/java/io/mosip/commons/khazana/
      spi/ObjectStoreAdapter.java   # The core interface all adapters implement
      impl/
        S3Adapter.java              # S3/MinIO adapter (primary production adapter)
        PosixAdapter.java           # Flat-file adapter (stores as zip files)
        SwiftAdapter.java           # OpenStack Swift adapter (not fully tested)
      util/
        ObjectStoreUtil.java        # Builds object paths from (source, process, objectName)
        SafeS3InputStream.java      # Wraps S3Object stream to ensure proper close
        EncryptionHelper.java       # Handles encrypt/decrypt for PosixAdapter pack()
      constant/KhazanaConstant.java
      constant/KhazanaErrorCodes.java
      exception/ObjectStoreAdapterException.java
    src/test/
      resources/application-test.properties  # Test config for all adapters
```

## Architecture

**`ObjectStoreAdapter` SPI** — single interface with methods: `getObject`, `putObject`, `exists`, `deleteObject`, `addObjectMetaData`, `getMetaData`, `incMetadata`, `decMetadata`, `removeContainer`, `pack`, `getAllObjects`, `addTags`, `getTags`, `deleteTags`.

Callers inject the adapter by Spring `@Qualifier` name: `"S3Adapter"`, `"PosixAdapter"`, or `"SwiftAdapter"`. The desired adapter is chosen at the application level by the consuming service.

**Object path construction** — `ObjectStoreUtil.getName(source, process, objectName)` builds the path `source/process/objectName` (any segment can be null/empty and is omitted). When `object.store.s3.use.account.as.bucketname=true`, the container is also prepended and the S3 bucket is the account name instead of the container name.

**S3Adapter specifics:**
- Maintains a singleton `AmazonS3` connection; on any exception it calls `shutdownConnection()` to reset it so the next call retries.
- Retries connection up to `object.store.connection.max.retry` (default 20) attempts.
- Bucket names are always lowercased (S3 requirement).
- Optional `object.store.s3.bucket-name-prefix` is prepended to every bucket name.
- Tags are stored as individual S3 objects under a `Tags/` prefix (not as native S3 object tags).
- `removeContainer` and `pack` are no-ops (return false) in S3Adapter.
- Implements `DisposableBean` to shut down the connection pool on Spring context close.

**PosixAdapter specifics:**
- Stores each container as a ZIP file at `{base.location}/{account}/{container}.zip`.
- Objects inside the ZIP are named `source/process/objectName.zip`; metadata is stored as `source/process/objectName.json`.
- Tags are stored as a separate JSON file: `{account}/{container}_tags.json`.
- `pack()` encrypts the container ZIP in place using `EncryptionHelper`.
- `incMetadata`/`decMetadata` are stubbed (return 0).
- `getAllObjects` returns null (not implemented).

**SwiftAdapter** — connects to OpenStack Swift via JOSS library. Per source comment: "has not been tested."

## Key Configuration Properties

| Property | Default | Description |
|---|---|---|
| `object.store.s3.accesskey` | `accesskey` | S3 access key |
| `object.store.s3.secretkey` | `secretkey` | S3 secret key |
| `object.store.s3.url` | `null` | S3 endpoint URL |
| `object.store.s3.region` | `null` | S3 region |
| `object.store.s3.use.account.as.bucketname` | `false` | Use account param as bucket name |
| `object.store.s3.bucket-name-prefix` | `` | Prefix added to all bucket names |
| `object.store.s3.readlimit` | `10000000` | Read limit for metadata re-upload |
| `object.store.connection.max.retry` | `20` | Max S3 connection retries |
| `object.store.max.connection` | `200` | S3 HTTP connection pool size |
| `object.store.connection.timeout` | `5000` | S3 connection timeout (ms) |
| `object.store.socket.timeout` | `10000` | S3 socket timeout (ms) |
| `object.store.client.execution.timeout` | `15000` | S3 client execution timeout (ms) |
| `object.store.base.location` | `home` | Base filesystem path for PosixAdapter |
| `object.store.swift.username` | `test` | Swift username |
| `object.store.swift.password` | `test` | Swift password |
| `object.store.swift.url` | `null` | Swift auth URL |

