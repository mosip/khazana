[![Maven Package upon a push](https://github.com/mosip/khazana/actions/workflows/push-trigger.yml/badge.svg?branch=release-1.3.x)](https://github.com/mosip/khazana/actions/workflows/push-trigger.yml)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?branch=release-1.3.x&project=mosip_khazana&id=mosip_khazana&metric=alert_status)](https://sonarcloud.io/dashboard?branch=release-1.3.x&id=mosip_khazana)
[![License: MPL 2.0](https://img.shields.io/badge/License-MPL_2.0-brightgreen.svg)](LICENSE)

# Khazana

**Khazana** is the MOSIP object-store library. It publishes one JAR, `io.mosip.commons:khazana`, with pluggable adapters for S3/MinIO, a local zip (POSIX) store, and OpenStack Swift. Registration client, registration processor, datashare, resident, and idrepo inject the adapter they need.

Parent Maven coordinates: `io.mosip.commons:khazana-parent` (`1.4.1-SNAPSHOT`). The parent POM follows the same shape as [bio-utils](https://github.com/mosip/bio-utils): `spring-boot-starter-parent` **4.1.1**, Java 21, **no `kernel-bom`**.

## Features

- **`ObjectStoreAdapter` SPI** — get, put, exists, delete, metadata, tags, pack, and list
- **`S3Adapter`** — Amazon S3 / MinIO via AWS SDK for Java 1.x (`com.amazonaws`). Singleton client, reconnect after failure, bucket names lowercased, tags stored as objects under `Tags/`. Implements `listObjectsByPrefix` and `moveObject` for draft publish/discard.
- **`PosixAdapter`** — one zip per container under `{object.store.base.location}/{account}/{container}.zip`. `pack()` encrypts that zip
- **`SwiftAdapter`** — OpenStack Swift via JOSS. Marked in source as not fully tested
- **Path helper** — `ObjectStoreUtil.getName` joins non-empty `source` / `process` / `objectName` segments with `/`

Callers select an implementation with Spring `@Qualifier`: `S3Adapter`, `PosixAdapter`, or `SwiftAdapter`.

## Engineering standards

| Item | Value |
| ---- | ----- |
| Java | 21 |
| Maven | 3.9+ |
| Spring Boot | **4.1.1** (`spring-boot-starter-parent`) |
| Spring Framework | 7.0.x (managed by Boot 4.1.1) |
| BOM policy | **No `kernel-bom`** — versions are pinned in `kernel/pom.xml` |
| Kernel | `io.mosip.kernel:kernel-core:1.4.1-SNAPSHOT` (logger is inside this JAR) |
| Key manager | `io.mosip.kernel:kernel-keymanager-service:1.4.1-rc.1` classifier `lib` (no `1.4.1-SNAPSHOT` published) |
| AWS SDK | 1.12.797 for `S3Adapter`; 2.55.7 for `S3PoolStatsLogger` metrics only |
| Swift | JOSS 0.10.4 |
| Coverage gate | JaCoCo instruction coverage **90%** on adapters and util (DTOs, constants, config, and exceptions excluded) |
| License | [Mozilla Public License 2.0](LICENSE) |

## Modules

```text
kernel/                  khazana-parent (pom)
└── object-store/        io.mosip.commons:khazana
```

Build from `kernel/`. The jar is `kernel/object-store/target/khazana-<version>.jar`.

## Prerequisites

- **JDK 21**
- **Maven 3.9.6** or higher
- **`kernel-core` 1.4.1-SNAPSHOT** installed locally (or available from the snapshot repository):

```text
cd ../commons/kernel
mvn clean install -Dgpg.skip=true
```

Do **not** import `kernel-bom`. Boot 4.1.1 is the BOM.

## Build

```text
cd kernel
mvn clean install -Dgpg.skip=true
```

Skip tests:

```text
mvn clean install -Dgpg.skip=true -DskipTests
```

Module tests and one class:

```text
cd object-store
mvn test
mvn test -Dtest=PosixAdapterTest
```

Coverage (fails the build under 90% instruction coverage):

```text
mvn verify -Dgpg.skip=true
```

The HTML report is `object-store/target/site/jacoco/index.html`.

SonarCloud (needs `SONAR_TOKEN`):

```text
mvn verify -Psonar -Dgpg.skip=true
```

## Use as a dependency

```xml
<dependency>
  <groupId>io.mosip.commons</groupId>
  <artifactId>khazana</artifactId>
  <version>1.4.1-SNAPSHOT</version>
</dependency>
```

```java
@Autowired
@Qualifier("S3Adapter")
private ObjectStoreAdapter adapter;
```

## Configuration

| Property | Default | Used by |
| -------- | ------- | ------- |
| `object.store.s3.accesskey` | `accesskey` | S3Adapter |
| `object.store.s3.secretkey` | `secretkey` | S3Adapter |
| `object.store.s3.url` | null | S3Adapter endpoint |
| `object.store.s3.region` | null | S3Adapter |
| `object.store.s3.use.account.as.bucketname` | `false` | `false`: bucket is the container. `true`: bucket is the account and the key is prefixed with `container/` |
| `object.store.s3.bucket-name-prefix` | empty | Prepended to every bucket name |
| `object.store.s3.readlimit` | `10000000` | Metadata re-upload |
| `object.store.s3.stream.buffer.size` | `8192` | S3Adapter |
| `object.store.connection.max.retry` | `20` | S3 connect retries |
| `object.store.max.connection` | `200` | S3 HTTP pool |
| `object.store.connection.timeout` | `5000` | milliseconds |
| `object.store.socket.timeout` | `10000` | milliseconds |
| `object.store.client.execution.timeout` | `15000` | milliseconds |
| `object.store.base.location` | `home` | PosixAdapter root directory |
| `object.store.swift.username` | see source | SwiftAdapter `@Value` is not a `${...}` placeholder |
| `object.store.swift.password` | see source | same |
| `object.store.swift.url` | see source | same |

`S3Adapter.removeContainer` and `S3Adapter.pack` return `false`. `PosixAdapter.pack` encrypts the container zip through `EncryptionHelper` (`OfflinePacketCryptoServiceImpl` by default, or the online cryptomanager URL).

## Notices

Third-party attributions and the MOSIP MPL 2.0 compatibility matrix:

- [NOTICE](NOTICE)
- [THIRD-PARTY-NOTICES.txt](THIRD-PARTY-NOTICES.txt)
- [licenses/NOTICE](licenses/NOTICE)

## License

Copyright (c) 2018-2026 MOSIP.

This project is licensed under the [Mozilla Public License 2.0](LICENSE).
