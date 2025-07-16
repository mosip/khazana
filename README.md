[![Maven Package upon a push](https://github.com/mosip/khazana/actions/workflows/push_trigger.yml/badge.svg?branch=release-1.2.0.1)](https://github.com/mosip/khazana/actions/workflows/push_trigger.yml)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?branch=develop&project=mosip_khazana&id=mosip_khazana&metric=alert_status)](https://sonarcloud.io/dashboard?branch=develop&id=mosip_khazana)

# Khazana
The Khazana is the Object Store in MOSIP. This library provides different Adapter implementations to connect to Object Store. Its used by regclient, regproc, datashare, resident, idrepo etc. modules to connect to object store.

## Overview
By default MOSIP provides 3 object store adapter implementation -
* S3 Adapter: Used for distributed object storage connection.
* Posix Adapter: used for flat file object store connection.
* Swift Adapter: used for distributed object storage connection.

## License
This project is licensed under the terms of [Mozilla Public License 2.0](LICENSE).
