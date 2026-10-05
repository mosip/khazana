# Khazana

MOSIP object-store JAR (`io.mosip.commons:khazana`). Parent is `spring-boot-starter-parent` 4.1.1 (Java 21). Do not import `kernel-bom`. `kernel-core` is `1.4.1-SNAPSHOT`. Callers select an adapter with Spring `@Qualifier`: `S3Adapter`, `PosixAdapter`, or `SwiftAdapter`. Consumed by regclient, regproc, datashare, resident, idrepo. A signature or path-shape change breaks those callers.

Maven cwd is `kernel/`. Jar: `object-store/target/khazana-<version>.jar`.

- `mvn clean install`
- `mvn clean install -DskipTests`
- `cd object-store && mvn test`
- `cd object-store && mvn test -Dtest=<Class>`
- `mvn verify -Psonar` (needs `SONAR_TOKEN`)

## Tree

Read only the branch for the files you are changing.

```
khazana
├── spi        ObjectStoreAdapter, ObjectStoreUtil, constant/, exception/
├── s3         S3Adapter, S3PoolStatsLogger, SafeS3InputStream
├── posix      PosixAdapter, Encryption*, *CryptoUtil
└── swift      SwiftAdapter
```

### spi

Methods: `getObject`, `putObject`, `exists`, `deleteObject`, `addObjectMetaData`, `getMetaData`, `incMetadata`, `decMetadata`, `removeContainer`, `pack`, `getAllObjects`, `addTags`, `getTags`, `deleteTags`, `moveObject`, `listObjectsByPrefix`.

`moveObject` defaults to false. `listObjectsByPrefix` defaults to an empty list (never null). `S3Adapter` implements both. `PosixAdapter` and `SwiftAdapter` keep the defaults. An empty list is not a failure; storage errors throw `ObjectStoreAdapterException`.

`ObjectStoreUtil.getName` skips a null or empty segment and joins the rest with `/`.

```
getName("src", "", "a")           → src/a
getName("acct", "src", "p", "a")  → acct/src/p/a
```

### s3

Primary adapter. AWS SDK 1.x (`com.amazonaws`). One shared `AmazonS3` client. On any exception set `connection = null` so the next call rebuilds it. Retry cap: `object.store.connection.max.retry` (20).

- Lowercase every bucket name. Prepend `object.store.s3.bucket-name-prefix` when set.
- `use.account.as.bucketname=false` (default): bucket is the container. `true`: bucket is the account and the key is `container/` + object name.
- Tags are objects under `Tags/`, not native S3 object tags.
- `listObjectsByPrefix` uses paginated `ListObjectsV2`. When the account is the bucket, returned keys have the `container/` segment stripped.
- `moveObject` copies then optionally deletes. A missing source throws `ObjectStoreAdapterException` wrapping `AmazonS3Exception` (HTTP 404).
- `removeContainer` and `pack` return false.
- Close S3 bodies through `SafeS3InputStream`.
- `S3PoolStatsLogger` is AWS SDK 2.x `MetricPublisher` only.

Defaults: accesskey/secretkey `accesskey`/`secretkey`, url/region null, readlimit `10000000`, stream.buffer.size `8192`, max.connection `200`, connection.timeout `5000`, socket.timeout `10000`, client.execution.timeout `15000`.

Test: `mvn test -Dtest=S3AdapterTest`

### posix

Each container is one zip: `{object.store.base.location}/{account}/{container}.zip` (default base `home`).

- Entry name: `source/process/objectName.zip`. Metadata: `source/process/objectName.json`.
- Tags: `{account}/{container}_tags.json` beside the zip, not inside it.
- `pack()` encrypts that zip in place via `EncryptionHelper` (needs `kernel-keymanager-service` 1.4.1-rc.1 classifier `lib`).
- `incMetadata` / `decMetadata` return 0. `getAllObjects` returns null.

Test: `mvn test -Dtest=PosixAdapterTest`

### swift

OpenStack Swift via JOSS. Source marks this adapter as not tested.

`@Value` fields are plain strings (`object.store.swift.username:test`, same shape for password and url). They are not `${...}` placeholders, so they do not read Spring properties.

Accounts are cached in a `Map<String, Account>`.
