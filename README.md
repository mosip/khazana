[![Build Status](https://api.travis-ci.com/mosip/khazana.svg?branch=1.2.0-rc2)](https://app.travis-ci.com/github/mosip/khazana)  [![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=mosip_khazana&id=mosip_khazana&branch=1.2.0-rc2&metric=alert_status)](https://sonarcloud.io/dashboard?id=mosip_khazana&branch=1.2.0-rc2)

# Khazana
The Khazana is the Object Store in MOSIP. This library provides different Adapter implementations to connect to Object Store. Its used by regclient, regproc, datashare, resident, idrepo etc. modules to connect to object store.

## Overview
By default MOSIP provides 3 object store adapter implementation -
* S3 Adapter: Used for distributed object storage connection.
* Posix Adapter: used for flat file object store connection.
* Swift Adapter: used for distributed object storage connection.

## License
This project is licensed under the terms of [Mozilla Public License 2.0](LICENSE).
