---
title: "Encryption Spec"
---
<!--
 - Licensed to the Apache Software Foundation (ASF) under one or more
 - contributor license agreements.  See the NOTICE file distributed with
 - this work for additional information regarding copyright ownership.
 - The ASF licenses this file to You under the Apache License, Version 2.0
 - (the "License"); you may not use this file except in compliance with
 - the License.  You may obtain a copy of the License at
 -
 -   http://www.apache.org/licenses/LICENSE-2.0
 -
 - Unless required by applicable law or agreed to in writing, software
 - distributed under the License is distributed on an "AS IS" BASIS,
 - WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 - See the License for the specific language governing permissions and
 - limitations under the License.
 -->

# Iceberg Encryption Spec

This document specifies the standard encryption scheme for Apache Iceberg tables (v3 and above).

## Standard Key Metadata

Standard key metadata stores the key material needed to decrypt a single encrypted file. It is serialized as:

```
VersionByte Payload
```

where:

* `VersionByte` is a single byte indicating the key metadata schema version. Currently, the only valid version is `0x01`.
* `Payload` is an Avro binary-encoded record (not a container file — only the raw binary encoding of the fields) using the schema for the given version. Optional fields are encoded as a union of `null` and the field type, with `null` as the first branch.

The Avro schema for version 1 is a record with the following fields, in order:

| Field name | Avro type | Required | Description |
|---|---|---|---|
| **`encryption_key`** | `bytes` | _required_ | The data encryption key (DEK) for this file. Must be 16, 24, or 32 bytes (corresponding to AES-128, AES-192, or AES-256). |
| **`aad_prefix`** | `bytes` | _optional_ | Random AAD prefix used for encryption integrity protection. |
| **`file_length`** | `long` | _optional_ | The encrypted file length in bytes, used to detect truncation attacks. |

The usage of these fields depends on how the file is encrypted:

* **[AES GCM Stream](gcm-stream-spec.md)** (used for Avro and Puffin files): The `encryption_key` is used directly as the AES-GCM key. The `aad_prefix` is combined with a 4-byte little-endian block index to form the AAD for each cipher block, as described in the [AES GCM Stream AAD section](gcm-stream-spec.md#additional-authenticated-data). The `file_length` is required and is checked as described in [AES GCM Stream file length](gcm-stream-spec.md#file-length).
* **[Parquet modular encryption](https://parquet.apache.org/docs/file-format/data-pages/encryption/)**: The `encryption_key` and `aad_prefix` are passed to Parquet readers and writers. The `aad_prefix` is passed as the AAD prefix parameter, which is combined with a module AAD suffix to form the full AAD for each Parquet module. The `file_length` is not set.
